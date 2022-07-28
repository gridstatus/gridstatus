from .base import ISOBase, FuelMix
import pandas as pd


class CAISO(ISOBase):
    BASE = "https://www.caiso.com/outlook/SP"

    def get_current_status(self) -> str:
        """Get Current Status of the Grid

        Known possible values: Normal
        """
        stats_url = self.BASE + "/stats.txt"
        r = self.get_json(stats_url)
        # todo is it possible for this to return more than one element?
        return r["gridstatus"][0]

    def get_fuel_mix(self):
        url = self.BASE + "/fuelsource.csv"
        df = pd.read_csv(url)

        mix = df.iloc[-1].to_dict()
        time = mix.pop("Time")  # handle time and time zone

        return FuelMix(time=time, mix=mix)
