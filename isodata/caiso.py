import io
import time
from typing import Any
from zipfile import ZipFile

import pandas as pd
import requests
from numpy import isin

import isodata
from isodata import utils
from isodata.base import FuelMix, GridStatus, ISOBase, Markets


class CAISO(ISOBase):
    BASE = "https://www.caiso.com/outlook/SP"
    HISTORY_BASE = "https://www.caiso.com/outlook/SP/History"

    name = "California ISO"
    iso_id = "caiso"
    default_timezone = "US/Pacific"

    # Markets PRC_RTPD_LMP, PRC_HASP_LMP, PRC_LMP
    markets = [
        Markets.REAL_TIME_15_MIN,
        Markets.REAL_TIME_HOURLY,
        Markets.DAY_AHEAD_HOURLY,
    ]

    trading_hub_locations = [
        "TH_NP15_GEN-APND",
        "TH_SP15_GEN-APND",
        "TH_ZP26_GEN-APND",
    ]

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

        time = pd.to_datetime(r["slotDate"]).tz_localize("US/Pacific")
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
            "demand": data["Current demand"],
        }

    def get_demand_today(self):
        "Get demand for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_demand)

    def get_historical_demand(self, date):
        """Return demand at a previous date in 5 minute intervals"""
        date = isodata.utils._handle_date(date)
        url = self.HISTORY_BASE + "/%s/demand.csv"
        df = _get_historical(url, date)[["Time", "Current demand"]]
        df = df.rename(columns={"Current demand": "Demand"})
        df = df.dropna(subset=["Demand"])
        return df

    def get_latest_supply(self):
        """Returns most recent data point for supply in MW

        Updates every 5 minutes
        """
        return self._latest_supply_from_fuel_mix()

    def get_supply_today(self):
        "Get supply for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_supply)

    def get_historical_supply(self, date):
        """Returns supply at a previous date in 5 minute intervals"""
        return self._supply_from_fuel_mix(date)

    def get_forecast_today(self):
        """Get load forecast for today in 1 hour intervals"""
        d = self._today_from_historical(self.get_historical_forecast)
        return d

    def get_historical_forecast(self, date, sleep=5):
        """Returns load forecast for a previous date in 1 hour intervals

        Arguments:
            date(datetime, pd.Timestamp, or str): day to return. if string, format should be YYYYMMDD e.g 20200623
            sleep (int): number of seconds to sleep before returning to avoid hitting rate limit in regular usage. Defaults to 5 seconds."""
        start = isodata.utils._handle_date(date, tz=self.default_timezone)

        start = start.tz_convert("UTC")
        end = start + pd.DateOffset(1)

        start = start.strftime("%Y%m%dT%H:%M-0000")
        end = end.strftime("%Y%m%dT%H:%M-0000")
        url = (
            "http://oasis.caiso.com/oasisapi/SingleZip?"
            + "resultformat=6&queryname=SLD_FCST&version=1&market_run_id=DAM"
            + f"&startdatetime={start}&enddatetime={end}"
        )

        df = pd.read_csv(
            url,
            compression="zip",
            usecols=["INTERVALSTARTTIME_GMT", "MW", "TAC_AREA_NAME"],
        ).rename(columns={"INTERVALSTARTTIME_GMT": "Time", "MW": "Load Forecast"})
        # returns many areas, we only want one overall iso
        df = df[df["TAC_AREA_NAME"] == "CA ISO-TAC"]

        df["Time"] = pd.to_datetime(
            df["Time"],
        ).dt.tz_convert(self.default_timezone)
        df = df.sort_values("Time")

        df["Forecast Time"] = df["Time"].iloc[0]

        df = df[["Forecast Time", "Time", "Load Forecast"]]
        time.sleep(sleep)
        return df

    def get_pnodes(self):
        url = "http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ATL_PNODE_MAP&version=1&startdatetime=20220801T07:00-0000&enddatetime=20220802T07:00-0000&pnode_id=ALL"
        df = pd.read_csv(
            url,
            compression="zip",
            usecols=["APNODE_ID", "PNODE_ID"],
        ).rename(
            columns={
                "APNODE_ID": "Aggregate PNode ID",
                "PNODE_ID": "PNode ID",
            },
        )
        return df

    def get_latest_lmp(self, market: str, locations: list = None):
        return self._latest_lmp_from_today(market, locations)

    def get_lmp_today(self, market: str, locations: list = None):
        "Get lmp for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_lmp, market, locations)

    def get_historical_lmp(
        self,
        date,
        market: str,
        locations: list = None,
        sleep: int = 5,
    ):
        """Get day ahead LMP pricing starting at supplied date for a list of locations.

        Arguments:
            date: date to return data

            market: market to return from. supports:

            locations (list): list of locations to get data from. If no locations are provided, defaults to NP15, SP15, and ZP26, which are the trading hub locations. For a list of locations, call CAISO.get_pnodes()

            sleep (int): number of seconds to sleep before returning to avoid hitting rate limit in regular usage. Defaults to 5 seconds.

        Returns
            dataframe of pricing data
        """

        if locations is None:
            locations = self.trading_hub_locations

        # todo make sure defaults to local timezone
        start = isodata.utils._handle_date(date, tz=self.default_timezone)

        start = start.tz_convert("UTC")
        end = start + pd.DateOffset(1)

        start = start.strftime("%Y%m%dT%H:%M-0000")
        end = end.strftime("%Y%m%dT%H:%M-0000")

        market = Markets(market)
        if market == Markets.DAY_AHEAD_HOURLY:
            query_name = "PRC_LMP"
            market_run_id = "DAM"
            version = 12
            PRICE_COL = "MW"
        elif market == Markets.REAL_TIME_15_MIN:
            query_name = "PRC_RTPD_LMP"
            market_run_id = "RTPD"
            version = 3
            PRICE_COL = "PRC"
        elif market == Markets.REAL_TIME_HOURLY:
            query_name = "PRC_HASP_LMP"
            market_run_id = "HASP"
            version = 3
            PRICE_COL = "MW"
        else:
            raise RuntimeError("LMP Market is not supported")

        nodes_str = ",".join(locations)
        url = f"http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname={query_name}&version={version}&startdatetime={start}&enddatetime={end}&market_run_id={market_run_id}&node={nodes_str}"

        df = _get_oasis(
            url,
            usecols=[
                "INTERVALSTARTTIME_GMT",
                "NODE",
                "LMP_TYPE",
                PRICE_COL,
            ],
        )

        df = df.pivot_table(
            index=["INTERVALSTARTTIME_GMT", "NODE"],
            columns="LMP_TYPE",
            values=PRICE_COL,
            aggfunc="first",
        )

        df = df.reset_index().rename(
            columns={
                "INTERVALSTARTTIME_GMT": "Time",
                "NODE": "Location",
                "LMP": "LMP",
                "MCE": "Energy",
                "MCC": "Congestion",
                "MCL": "Loss",
            },
        )

        df["Time"] = pd.to_datetime(
            df["Time"],
        ).dt.tz_convert(self.default_timezone)

        df["Market"] = market.value
        df["Location Type"] = None

        df.loc[
            df["Location"].isin(self.trading_hub_locations),
            "Location Type",
        ] = "Trading Hub"

        df = df[
            [
                "Time",
                "Market",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
        ]

        data = utils.filter_lmp_locations(df, locations)

        time.sleep(sleep)

        return df

    def get_battery_today(self):
        """Return battery charging or discharging for today in 5 minute intervals

        Negative means charging, positive means discharging

        Arguments:
            date: date to return data
        """
        return self._today_from_historical(self.get_historical_battery)

    def get_historical_battery(self, date):
        """Return battery charging or discharging at a previous date in 5 minute intervals

        Negative means charging, positive means discharging

        Arguments:
            date: date to return data
        """
        date = isodata.utils._handle_date(date)
        url = self.HISTORY_BASE + "/%s/storage.csv"
        df = _get_historical(url, date)
        df = df.rename(columns={"Batteries": "Battery Supply"})
        return df


def _make_timestamp(time_str, today, timezone="US/Pacific"):
    hour, minute = map(int, time_str.split(":"))
    return pd.Timestamp(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=hour,
        minute=minute,
        tz=timezone,
    )


def _get_historical(url, date):
    date_str = date.strftime("%Y%m%d")
    date_obj = date
    url = url % date_str
    df = pd.read_csv(url)

    df["Time"] = df["Time"].apply(
        _make_timestamp,
        today=date_obj,
        timezone="US/Pacific",
    )

    return df


def _get_oasis(url, usecols=None):
    retry_num = 0
    while retry_num < 3:
        r = requests.get(url)

        if r.status_code == 200:
            break

        retry_num += 1
        print(f"Failed to get data from CAISO. Error: {r.status_code}")
        print(f"Retrying {retry_num}...")
        time.sleep(5)

    z = ZipFile(io.BytesIO(r.content))

    df = pd.read_csv(
        z.open(z.namelist()[0]),
        usecols=usecols,
    )

    return df
