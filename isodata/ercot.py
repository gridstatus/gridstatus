from .base import ISOBase, FuelMix
import pandas as pd


class Ercot(ISOBase):

    def get_fuel_mix(self):
        url = "https://www.ercot.com/api/1/services/read/dashboards/combine-wind-solar.json"
        r = self.get_json(url)

        # rows with nulls are forecasts
        df = pd.DataFrame(r['currentDay']["data"])
        df = df.dropna(subset=["actualSolar"])

        day = r['currentDay']["date"]
        hour = df["hourEnding"].max()  # latest hour in dataset
        time = day + "%d:00:00" % (hour)

        currentHour = df.iloc[-1]

        mix_dict = {
            "wind": currentHour["actualWind"],
            "solar": currentHour["actualSolar"]
        }

        return FuelMix(time=time, mix=mix_dict)
