from .base import ISOBase, FuelMix
import pandas as pd


class Ercot(ISOBase):
    name = "Electric Reliability Council of Texas"
    iso_id = "ercot"

    def get_fuel_mix(self):
        url = "https://www.ercot.com/api/1/services/read/dashboards/combine-wind-solar.json"
        r = self.get_json(url)

        # rows with nulls are forecasts
        df = pd.DataFrame(r['currentDay']["data"])
        df = df.dropna(subset=["actualSolar"])

        time = pd.Timestamp(df["epoch"].max(), unit="ms", tz="US/Central")
        currentHour = df.iloc[-1]

        mix_dict = {
            "wind": currentHour["actualWind"],
            "solar": currentHour["actualSolar"]
        }

        return FuelMix(time=time, mix=mix_dict, iso=self.name)

    def get_historical_fuel_mix(self):
        # url above can do it for current day
        pass
