from bdb import set_trace
from .base import ISOBase, FuelMix, GridStatus
import pandas as pd


class Ercot(ISOBase):
    name = "Electric Reliability Council of Texas"
    iso_id = "ercot"
    default_timezone = "US/Central"

    BASE = "https://www.ercot.com/api/1/services/read/dashboards"

    def get_latest_status(self):
        r = self._get_json(self.BASE + "/daily-prc.json")

        time = pd.to_datetime(r["current_condition"]["datetime"], unit="s").tz_localize(
            "UTC").tz_convert(self.default_timezone)
        status = r["current_condition"]["state"]
        reserves = float(r["current_condition"]["prc_value"].replace(",", ""))
        return GridStatus(time=time, status=status, reserves=reserves, iso=self.name)

    def get_latest_fuel_mix(self):
        df = self.get_fuel_mix_today()
        currentHour = df.iloc[-1]

        mix_dict = {
            "Wind": currentHour["Wind"],
            "Solar": currentHour["Solar"]
        }

        return FuelMix(time=currentHour["Time"], mix=mix_dict, iso=self.name)

    def get_fuel_mix_today(self):
        """Get historical fuel mix

        Only supports current day
        """
        url = self.BASE + "/combine-wind-solar.json"
        r = self._get_json(url)

        # rows with nulls are forecasts
        df = pd.DataFrame(r['currentDay']["data"])
        df = df.dropna(subset=["actualSolar"])

        df = self._handle_data(
            df, {"actualSolar": "Solar", "actualWind": "Wind"})
        return df

    def get_latest_demand(self):
        d = self._get_demand('currentDay').iloc[-1]

        return {
            "time": d["Time"],
            "demand": d["Demand"]
        }

    def _get_demand(self, when):
        """Returns demand for currentDay or previousDay"""

        url = self.BASE + "/loadForecastVsActual.json"
        r = self._get_json(url)
        df = pd.DataFrame(r[when]["data"])
        df = df.dropna(subset=["systemLoad"])
        df = self._handle_data(df, {"systemLoad": "Demand"})
        return df

    def get_demand_today(self):
        """Returns demand for today"""
        return self._get_demand("currentDay")

    def get_demand_yesterday(self):
        """Returns demand for yesterday"""
        return self._get_demand("previousDay")

    def get_prices(self):
        pass
    # https://www.ercot.com/api/1/services/read/dashboards/systemWidePrices.json

    def _handle_data(self, df, columns):
        df["Time"] = pd.to_datetime(df["epoch"], unit="ms").dt.tz_localize(
            "UTC").dt.tz_convert(self.default_timezone)

        cols_to_keep = ["Time"] + list(columns.keys())
        return df[cols_to_keep].rename(columns=columns)
