from numpy import isin
from .base import ISOBase, FuelMix
import pandas as pd
from typing import Any


class CAISO(ISOBase):
    BASE = "https://www.caiso.com/outlook/SP"
    HISTORY_BASE = "https://www.caiso.com/outlook/SP/History"

    name = "California ISO"
    iso_id = "caiso"

    def _current_day(self):
        # get current date from stats api
        return pd.to_datetime(self.get_stats()["slotDate"]).date()

    def get_stats(self):
        stats_url = self.BASE + "/stats.txt"
        r = self.get_json(stats_url)
        return r

    def get_current_status(self) -> str:
        """Get Current Status of the Grid

        Known possible values: Normal
        """

        # todo is it possible for this to return more than one element?
        r = self.get_stats()
        return r["gridstatus"][0]

    def get_fuel_mix(self):
        """
            Returns most recent data point for fuelmix in MW

            Updates every 5 minutes
        """
        url = self.BASE + "/fuelsource.csv"
        df = pd.read_csv(url)

        mix = df.iloc[-1].to_dict()
        time = _make_timestamp(mix.pop("Time"), self._current_day())

        return FuelMix(time=time, mix=mix, iso=self.name)

    def get_historical_fuel_mix(self, date):
        """
        Get historical fuel mix in 5 minute intervals for a provided day 

        Arguments:
            date(datetime, pd.Timestamp, or str): day to return. if string, format should be YYYYMMDD e.g 20200623

        Returns:
            dataframe

        """
        # todo test date handling date
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

    def get_historical_demand(self, date):
        url = self.HISTORY_BASE + "/%s/demand.csv"
        df = _get_historical(url, date)[["Time", "Current demand"]]
        df = df.rename(columns={"Current demand": "Demand"})
        return df

    def get_latest_supply(self):
        """Returns most recent data point for supply in MW

        Updates every 5 minutes
        """
        mix = self.get_fuel_mix()

        return {
            "time": mix.time,
            "supply": mix.total_production
        }

    def get_historical_supply(self, date):
        df = self.get_historical_fuel_mix(date)
        supply_df = df.pop("Time").to_frame()
        supply_df["Supply"] = df.sum(axis=1)  # sum all the remaining columns
        return supply_df


def _make_timestamp(time_str, today, timezone='US/Pacific'):
    hour, minute = map(int, time_str.split(":"))
    return pd.Timestamp(year=today.year, month=today.month, day=today.day, hour=hour, minute=minute,  tz=timezone)


def _get_historical(url, date):
    if not isinstance(date, str):
        date_str = date.strftime('%Y%m%d')
        date_obj = date
    else:
        date_str = date
        date_obj = pd.to_datetime(date)

    url = url % date_str
    df = pd.read_csv(url)

    df["Time"] = df["Time"].apply(
        _make_timestamp, today=date_obj, timezone='US/Pacific')

    return df
