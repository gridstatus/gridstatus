from .base import ISOBase, FuelMix
import pandas as pd


class SPP(ISOBase):

    def get_fuel_mix(self):
        url = "https://marketplace.spp.org/chart-api/gen-mix/asChart"
        r = self.get_json(url)["response"]

        data = {
            "Timestamp":  r["labels"]
        }
        data.update((d["label"], d["data"]) for d in r["datasets"])

        historical_mix = pd.DataFrame(data)

        current_mix = historical_mix.iloc[0].to_dict()

        time = current_mix.pop("Timestamp")

        return FuelMix(time=time, mix=current_mix)
