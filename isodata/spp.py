from .base import ISOBase, FuelMix
import pandas as pd


class SPP(ISOBase):
    name = "Southwest Power Pool"
    iso_id = "spp"

    def get_latest_fuel_mix(self):
        url = "https://marketplace.spp.org/chart-api/gen-mix/asChart"
        r = self._get_json(url)["response"]

        data = {
            "Timestamp":  r["labels"]
        }
        data.update((d["label"], d["data"]) for d in r["datasets"])

        historical_mix = pd.DataFrame(data)

        current_mix = historical_mix.iloc[0].to_dict()

        time = pd.Timestamp(current_mix.pop("Timestamp"))

        return FuelMix(time=time, mix=current_mix, iso=self.name)
