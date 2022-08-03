from numpy import isin

import isodata
from .base import ISOBase, FuelMix, GridStatus
import pandas as pd
from typing import Any


class CAISO(ISOBase):
    BASE = "https://www.caiso.com/outlook/SP"
    HISTORY_BASE = "https://www.caiso.com/outlook/SP/History"

    name = "California ISO"
    iso_id = "caiso"
    default_timezone = "US/Pacific"

    def _current_day(self):
        # get current date from stats api
        return self.get_latest_status().time.date()

    def get_stats(self):
        stats_url = self.BASE + "/stats.txt"
        r = self._get_json(stats_url)
        return r

    def get_latest_status(self) -> str:
        """Get Current Status of the Grid

        Known possible values: Normal
        """

        # todo is it possible for this to return more than one element?
        r = self.get_stats()

        time = pd.to_datetime(r["slotDate"]).tz_localize('US/Pacific')
        status = r["gridstatus"][0]
        reserves = r["Current_reserve"]

        return GridStatus(time=time, status=status, reserves=reserves, iso=self.name)

    def get_latest_fuel_mix(self):
        """
            Returns most recent data point for fuelmix in MW

            Updates every 5 minutes
        """
        url = self.BASE + "/fuelsource.csv"
        df = pd.read_csv(url)

        mix = df.iloc[-1].to_dict()
        time = _make_timestamp(mix.pop("Time"), self._current_day())

        return FuelMix(time=time, mix=mix, iso=self.name)

    def get_fuel_mix_today(self):
        "Get fuel_mix for today in 5 minute intervals"
        # todo should this use the latest endpoint?
        return self._today_from_historical(self.get_historical_fuel_mix)

    def get_fuel_mix_yesterday(self):
        "Get fuel_mix for yesterdat in 5 minute intervals"
        return self._yesterday_from_historical(self.get_historical_fuel_mix)

    def get_historical_fuel_mix(self, date):
        """
        Get historical fuel mix in 5 minute intervals for a provided day 

        Arguments:
            date(datetime, pd.Timestamp, or str): day to return. if string, format should be YYYYMMDD e.g 20200623

        Returns:
            dataframe

        """
        date = isodata.utils._handle_date(date)
        url = self.HISTORY_BASE + "/%s/fuelsource.csv"
        df = _get_historical(url, date)
        return df

    def get_latest_demand(self):
        """Returns most recent data point for demand in MW

        Updates every 5 minutes
        """
        demand_url = self.BASE + "/demand.csv"
        df = pd.read_csv(demand_url)

        # get last non null row
        data = df[~df["Current demand"].isnull()].iloc[-1]

        return {
            "time": _make_timestamp(data["Time"], self._current_day()),
            "demand": data["Current demand"]
        }

    def get_demand_today(self):
        "Get demand for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_demand)

    def get_demand_yesterday(self):
        "Get demand for yesterdat in 5 minute intervals"
        return self._yesterday_from_historical(self.get_historical_demand)

    def get_historical_demand(self, date):
        """Return demand at a previous date in 5 minute intervals"""
        date = isodata.utils._handle_date(date)
        url = self.HISTORY_BASE + "/%s/demand.csv"
        df = _get_historical(url, date)[["Time", "Current demand"]]
        df = df.rename(columns={"Current demand": "Demand"})
        return df

    def get_latest_supply(self):
        """Returns most recent data point for supply in MW

        Updates every 5 minutes
        """
        mix = self.get_latest_fuel_mix()

        return {
            "time": mix.time,
            "supply": mix.total_production
        }

    def get_supply_today(self):
        "Get supply for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_supply)

    def get_supply_yesterday(self):
        "Get supply for yesterdat in 5 minute intervals"
        return self._yesterday_from_historical(self.get_historical_supply)

    def get_historical_supply(self, date):
        """Returns supply at a previous date in 5 minute intervals"""
        return self._supply_from_fuel_mix(date)

    def get_pnodes(self):
        url = "http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ATL_PNODE_MAP&version=1&startdatetime=20220801T07:00-0000&enddatetime=20220802T07:00-0000&pnode_id=ALL"
        df = pd.read_csv(url, compression='zip', usecols=["APNODE_ID", "PNODE_ID"]).rename(columns={
            "APNODE_ID": "Aggregate PNode ID",
            "PNODE_ID": "PNode ID",
        })
        return df

    def get_day_ahead_prices(self, start_date, num_days=1, nodes=None):
        """Get day ahead LMP pricing starting at supplied date for a list of nodes.

        Arguments:
            start_date (str or pd.Timestamp): starting date to return data. Supports starting date up to 39 months ago


            num_days: get data for num_days after starting date. Must be less than 31

            nodes (list): list of nodes to get data from. If no nodes are provided, defaults to NP15, SP15, and ZP26, which are the trading hub nodes. For a list of nodes, call CAISO.get_pnodes()

        Returns
            dataframe of pricing data
        """

        if num_days > 31:
            raise RuntimeError("num_days must be below 31")

        if nodes is None:
            nodes = ["TH_NP15_GEN-APND",
                     "TH_SP15_GEN-APND", "TH_ZP26_GEN-APND"]

        if isinstance(start_date, str):
            start_date = pd.to_datetime(
                start_date).tz_localize(self.default_timezone)

        nodes_str = ",".join(nodes)

        start = start_date.tz_convert("UTC")
        end = start + pd.DateOffset(num_days)

        start = start.strftime("%Y%m%dT%H:%M-0000")
        end = end.strftime("%Y%m%dT%H:%M-0000")
        url = f"http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=12&startdatetime={start}&enddatetime={end}&market_run_id=DAM&node={nodes_str}"
        # todo catch too many requests
        df = pd.read_csv(url,
                         compression='zip',
                         usecols=["INTERVALSTARTTIME_GMT", "NODE", "LMP_TYPE", "MW"])
        df = df.pivot_table(index=['INTERVALSTARTTIME_GMT', 'NODE'],
                            columns='LMP_TYPE', values='MW', aggfunc='first')
        df = df.reset_index().rename(columns={
            "NODE": "pnode", "OPR_HR": "hour", "LMP": "lmp", "MCE": "energy", "MCC": "congestion", "MCL": "losses"})
        df["interval start"] = pd.to_datetime(
            df['INTERVALSTARTTIME_GMT']).dt.tz_convert(self.default_timezone)
        df = df.set_index("interval start").drop(
            columns=["INTERVALSTARTTIME_GMT"])

        return df


def _make_timestamp(time_str, today, timezone='US/Pacific'):
    hour, minute = map(int, time_str.split(":"))
    return pd.Timestamp(year=today.year, month=today.month, day=today.day, hour=hour, minute=minute,  tz=timezone)


def _get_historical(url, date):
    date_str = date.strftime('%Y%m%d')
    date_obj = date
    url = url % date_str
    df = pd.read_csv(url)

    df["Time"] = df["Time"].apply(
        _make_timestamp, today=date_obj, timezone='US/Pacific')

    return df
