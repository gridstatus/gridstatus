from enum import Enum

import pandas as pd
import requests
from tabulate import tabulate

# TODO: this is needed to make SPP request work. restrict only to SPP
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = "ALL:@SECLEVEL=1"


class Markets(Enum):
    REAL_TIME_5_MIN = "REAL_TIME_5_MIN"
    REAL_TIME_15_MIN = "REAL_TIME_15_MIN"
    REAL_TIME_HOURLY = "REAL_TIME_HOURLY"
    DAY_AHEAD_5_MIN = "DAY_AHEAD_5_MIN"
    DAY_AHEAD_HOURLY = "DAY_AHEAD_HOURLY"

    def __contains__(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True


class ISOBase:
    markets = []

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

    def get_historical_fuel_mix(self, date):
        raise NotImplementedError()

    def get_latest_demand(self):
        raise NotImplementedError()

    def get_demand_today(self):
        raise NotImplementedError()

    def get_historical_demand(self, date):
        raise NotImplementedError()

    def get_forecast_today(self):
        raise NotImplementedError()

    def get_historical_forecast(self, date):
        raise NotImplementedError()

    def get_latest_supply(self):
        raise NotImplementedError()

    def get_supply_today(self):
        raise NotImplementedError()

    def get_historical_supply(self, date):
        raise NotImplementedError()

    def get_battery_today(self):
        raise NotImplementedError()

    def get_historical_battery(self, date):
        raise NotImplementedError()

    def _latest_lmp_from_today(self, market, locations):
        lmp_df = self.get_lmp_today(market, locations)
        # Assume sorted in ascending order
        latest_df = lmp_df.groupby("Location").last().reset_index()
        return latest_df

    def _latest_from_today(self, method, *args, **kwargs):
        data = method(*args, **kwargs)
        latest = data.iloc[-1]

        latest.index = latest.index.str.lower()

        return latest.to_dict()

    def _today_from_historical(self, method, *args, **kwargs):
        today = pd.Timestamp.now(self.default_timezone).date()
        return method(today, *args, **kwargs)

    def _supply_from_fuel_mix(self, date):
        df = self.get_historical_fuel_mix(date)
        supply_df = df.pop("Time").to_frame()
        supply_df["Supply"] = df.sum(axis=1)  # sum all the remaining columns
        return supply_df

    def _latest_supply_from_fuel_mix(self):
        mix = self.get_latest_fuel_mix()

        return {"time": mix.time, "supply": mix.total_production}


class GridStatus:
    def __init__(self, time, status, reserves, iso, notes=None, unit="MW") -> None:
        self.iso = iso
        self.time = time
        self.status = status
        self.reserves = reserves
        self.unit = unit
        self.notes = notes

    def __repr__(self) -> str:
        s = self.iso + "\n"

        s += "Time: %s \n" % str(self.time)
        s += "Status: %s \n" % self.status

        if self.reserves is not None:
            s += "Reserves: %.0f %s" % (self.reserves, self.unit)

        if self.notes and len(self.notes):
            s += "Notes:\n"
            for n in self.notes:
                s += "-  %s\n" % n

        return s


class FuelMix:
    def __init__(self, time, mix, iso=None, unit="MW") -> None:
        self.iso = iso
        self.time = time
        self.unit = unit

        mix_df = (
            pd.Series(mix, name=self.unit)
            .sort_values(
                ascending=False,
            )
            .to_frame()
        )
        mix_df["Percent"] = mix_df[self.unit] / mix_df[self.unit].sum() * 100
        mix_df.index.name = "Fuel"
        self._mix_df = mix_df

    def __repr__(self) -> str:
        # TODO sort by magnitude
        s = ""
        if self.iso:
            s += "ISO: " + self.iso + "\n"
        s += "Total Production: %d %s \n" % (self.total_production, self.unit)
        s += "Time: %s \n" % self.time

        mix = self.mix
        mix["Percent"] = mix["Percent"].round(1)
        s += tabulate(mix, headers="keys", tablefmt="psql")

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
    - time step differences
    - what is the interval
    - api reference
    -

- get_historical_fuel_mix vs get_fuel_mix_trend
"""
