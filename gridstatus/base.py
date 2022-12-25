from enum import Enum

import pandas as pd
import requests
from tabulate import tabulate

# TODO: this is needed to make SPP request work. restrict only to SPP
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = "ALL:@SECLEVEL=1"

# not supported exception


class NotSupported(Exception):
    pass


class Markets(Enum):
    """Names of LMP Markets"""

    REAL_TIME_5_MIN = "REAL_TIME_5_MIN"
    REAL_TIME_15_MIN = "REAL_TIME_15_MIN"
    REAL_TIME_HOURLY = "REAL_TIME_HOURLY"
    DAY_AHEAD_HOURLY = "DAY_AHEAD_HOURLY"

    def __contains__(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True


class InterconnectionQueueStatus(Enum):
    """Interconnection queue types"""

    ACTIVE = "Active"
    WITHDRAWN = "Withdrawn"
    COMPLETED = "Completed"


_interconnection_columns = [
    "Queue ID",
    "Project Name",
    "Interconnecting Entity",
    "County",
    "State",
    "Interconnection Location",
    "Transmission Owner",
    "Generation Type",
    "Capacity (MW)",
    "Summer Capacity (MW)",
    "Winter Capacity (MW)",
    "Queue Date",
    "Status",
    "Proposed Completion Date",
    "Withdrawn Date",
    "Withdrawal Comment",
    "Actual Completion Date",
]


class ISOBase:
    markets = []
    status_homepage = None

    def _get_json(self, *args, **kwargs):
        if "verbose" in kwargs:
            verbose = kwargs.pop("verbose")
            if verbose:
                print("Requesting", args[0], "with", kwargs)

        r = requests.get(*args, **kwargs)
        r = r.json()

        return r

    def get_status(self, date, end=None, verbose=False):
        raise NotImplementedError()

    def get_fuel_mix(self, date, end=None, verbose=False):
        """Get fuel mix in 5 minute intervals for a provided day

        Arguments:
            date (datetime or str): "latest", "today", or an object that can be parsed as a datetime for the day to return data.

            start (datetime or str): start of date range to return. alias for `date` parameter. Only specify one of `date` or `start`.

            end (datetime or str): "today" or an object that can be parsed as a datetime for the day to return data. Only used if requesting a range of dates.

            verbose (bool): print verbose output. Defaults to False.


        Returns:
            pd.Dataframe: dataframe with columns: Time and columns for each fuel type
        """
        raise NotImplementedError()

    def get_load(self, date, end=None, verbose=False):
        raise NotImplementedError()

    def get_load_forecast(self, date, end=None, verbose=False):
        raise NotImplementedError()

    def get_storage(self, date, end=None, verbose=False):
        raise NotImplementedError()

    def get_interconnection_queue(self):
        raise NotImplementedError()

    def _latest_lmp_from_today(self, market, locations, **kwargs):
        lmp_df = self.get_lmp(
            date="today", market=market, locations=locations, **kwargs
        )
        # Assume sorted in ascending order
        latest_df = lmp_df.groupby("Location").last().reset_index()
        return latest_df

    def _latest_from_today(self, method, *args, **kwargs):
        data = method(date="today", *args, **kwargs)
        latest = data.iloc[-1]

        latest.index = latest.index.str.lower()

        return latest.to_dict()

    def _supply_from_fuel_mix(self, date):
        df = self.get_fuel_mix(date)
        supply_df = df.pop("Time").to_frame()
        supply_df["Supply"] = df.sum(axis=1)  # sum all the remaining columns
        return supply_df

    def _latest_supply_from_fuel_mix(self):
        mix = self.get_fuel_mix(date="latest")

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
        s = self.iso.name + "\n"

        s += "Time: %s \n" % str(self.time)
        s += "Status: %s \n" % self.status

        if self.iso.status_homepage:
            s += "Status Homepage: %s \n" % self.iso.status_homepage

        if self.reserves is not None:
            s += "Reserves: %.0f %s" % (self.reserves, self.unit)

        if self.notes and len(self.notes):
            s += "Notes:\n"
            for n in self.notes:
                s += "-  %s\n" % n

        return s

    def to_dict(self):
        return {
            "time": self.time,
            "status": self.status,
            "notes": self.notes,
        }


class FuelMix:
    def __init__(self, time, mix, iso=None, unit="MW") -> None:
        self.iso = iso
        self.time = time
        self.unit = unit

        mix_df = pd.DataFrame(mix, index=[0])
        mix_df.insert(0, "Time", time)

        self._mix_df = mix_df

    def __repr__(self) -> str:
        # TODO sort by magnitude
        s = ""
        if self.iso:
            s += "ISO: " + self.iso + "\n"
        s += "Total Production: %d %s \n" % (self.total_production, self.unit)
        s += "Time: %s \n" % self.time

        mix = self.mix.drop("Time", axis=1).T
        mix.columns = ["MW"]
        mix["Percent"] = (mix["MW"] / self.total_production * 100).round(1)
        s += tabulate(mix, headers="keys", tablefmt="psql")

        return s

    @property
    def total_production(self):
        return self.mix.drop("Time", axis=1).sum().sum()

    @property
    def mix(self):
        return self._mix_df.copy()

    @property
    def mix_dict(self):
        return self.mix.iloc[0].to_dict()


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
