import pandas as pd
import requests
from tabulate import tabulate
# TODO: this is needed to make SPP request work. restrict only to SPP
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'


class ISOBase:

    def _get_json(self, *args, **kwargs):
        r = requests.get(*args, **kwargs)
        r = r.json()
        return r

    def get_latest_status(self):
        raise NotImplementedError()

    def get_latest_fuel_mix(self):
        raise NotImplementedError()

    def get_fuel_mix_today(self):
        raise NotImplementedError()

    def get_fuel_mix_yesterday(self):
        raise NotImplementedError()

    def get_historical_fuel_mix(self, date):
        raise NotImplementedError()

    def get_latest_demand(self):
        raise NotImplementedError()

    def get_demand_today(self):
        raise NotImplementedError()

    def get_demand_yesterday(self):
        raise NotImplementedError()

    def get_historical_demand(self, date):
        raise NotImplementedError()

    def get_latest_supply(self):
        raise NotImplementedError()

    def get_supply_today(self):
        raise NotImplementedError()

    def get_supply_yesterday(self):
        raise NotImplementedError()

    def get_historical_supply(self, date):
        raise NotImplementedError()

    def _today_from_historical(self, method):
        today = pd.Timestamp.now(self.default_timezone).date()
        return method(today)

    def _yesterday_from_historical(self, method):
        yesterday = (pd.Timestamp.now(self.default_timezone) -
                     pd.DateOffset(1)).date()
        return method(yesterday)

    def _supply_from_fuel_mix(self, date):
        df = self.get_historical_fuel_mix(date)
        supply_df = df.pop("Time").to_frame()
        supply_df["Supply"] = df.sum(axis=1)  # sum all the remaining columns
        return supply_df


class GridStatus():
    def __init__(self, time, status, reserves, iso, unit="MW") -> None:
        self.iso = iso
        self.time = time
        self.status = status
        self.reserves = reserves
        self.unit = unit

    def __repr__(self) -> str:
        s = self.iso + "\n"

        s += "Time: %s \n" % str(self.time)
        s += "Status: %s \n" % self.status
        s += "Reserves: %.0f %s" % (self.reserves, self.unit)

        return s


class FuelMix:
    def __init__(self, time, mix, iso=None, unit="MW") -> None:
        self.iso = iso
        self.time = time
        self.unit = unit

        mix_df = pd.Series(mix, name=self.unit).sort_values(
            ascending=False).to_frame()
        mix_df["Percent"] = mix_df[self.unit] / mix_df[self.unit].sum() * 100
        mix_df.index.name = "Fuel"
        self._mix_df = mix_df

    def __repr__(self) -> str:
        # TODO sort by magnitude
        s = ''
        if self.iso:
            s += "ISO: " + self.iso + "\n"
        s += "Total Production: %d %s \n" % (self.total_production, self.unit)
        s += "Time: %s \n" % self.time

        mix = self.mix
        mix["Percent"] = mix["Percent"].round(1)
        s += tabulate(mix, headers='keys', tablefmt='psql')

        return s

    @ property
    def total_production(self):
        return self.mix[self.unit].sum()

    @ property
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
    - time step differences
    - what is the interval
    - api reference
    -

- get_historical_fuel_mix vs get_fuel_mix_trend
"""
