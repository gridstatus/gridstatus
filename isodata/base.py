import pandas as pd
import requests
from tabulate import tabulate
# TODO: this is needed to make SPP request work. restrict only to SPP
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'


class ISOBase:

    def get_json(self, *args, **kwargs):
        r = requests.get(*args, **kwargs)
        r = r.json()
        return r

    def get_fuel_mix(self):
        raise NotImplementedError()


class FuelMix:
    def __init__(self, time, mix, unit="MW") -> None:
        self.time = time
        self.unit = unit

        mix_df = pd.Series(mix, name=self.unit).sort_values(
            ascending=False).to_frame()
        mix_df["Percent"] = mix_df[self.unit] / mix_df[self.unit].sum() * 100
        mix_df.index.name = "Fuel"
        self._mix_df = mix_df

    def __repr__(self) -> str:
        # TODO sort by magnitude
        s = "Total Production: %d %s \n" % (self.total_production, self.unit)
        s += "Time: %s \n" % self.time

        mix = self.mix
        mix["Percent"] = mix["Percent"].round(1)
        s += tabulate(mix, headers='keys', tablefmt='psql')

        return s

    @property
    def total_production(self):
        return self.mix[self.unit].sum()

    @property
    def mix(self):
        return self._mix_df.copy()


"""
Todos

- fuel mix
    - how standardize should the mix be? 
    - mark renewables
    - historical data
    - is the unit mh or mhw?
- units for return values
- documentation
    - include where the data is from
    - api reference
"""
