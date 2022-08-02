from bdb import set_trace
from .base import ISOBase, FuelMix, GridStatus
import pandas as pd


class Ercot(ISOBase):
    name = "Electric Reliability Council of Texas"
    iso_id = "ercot"

    def get_current_status(self):
        r = self.get_json(
            "https://www.ercot.com/api/1/services/read/dashboards/daily-prc.json")

        time = pd.to_datetime(r["current_condition"]["datetime"], unit="s").tz_localize(
            "UTC").tz_convert("US/Central")
        status = r["current_condition"]["state"]
        reserves = float(r["current_condition"]["prc_value"].replace(",", ""))
        return GridStatus(time=time, status=status, reserves=reserves, iso=self.name)

    def get_fuel_mix(self):
        df = self.get_historical_fuel_mix(None)
        currentHour = df.iloc[-1]

        mix_dict = {
            "Wind": currentHour["Wind"],
            "Solar": currentHour["Solar"]
        }

        return FuelMix(time=currentHour["Time"], mix=mix_dict, iso=self.name)

    def get_historical_fuel_mix(self, date):
        """Get historical fuel mix

        Only supports current day
        """
        url = "https://www.ercot.com/api/1/services/read/dashboards/combine-wind-solar.json"
        r = self.get_json(url)

        # rows with nulls are forecasts
        df = pd.DataFrame(r['currentDay']["data"])
        df = df.dropna(subset=["actualSolar"])

        df["Time"] = pd.to_datetime(df["epoch"], unit="ms").dt.tz_localize(
            "UTC").dt.tz_convert("US/Central")

        df = df[["Time", "actualSolar", "actualWind"]].rename(
            columns={"actualSolar": "Solar", "actualWind": "Wind"})

        return df

    def get_historical_demand(self, date):
        pass
        # https://www.ercot.com/api/1/services/read/dashboards/loadForecastVsActual.json

    def get_prices(self):
        pass
    # https://www.ercot.com/api/1/services/read/dashboards/systemWidePrices.json
