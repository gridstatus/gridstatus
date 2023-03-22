from enum import Enum

import requests

from gridstatus.logging import log

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
    interconnection_homepage = None

    def _get_json(self, *args, **kwargs):
        if "verbose" in kwargs:
            verbose = kwargs.pop("verbose")
            msg = f"Requesting {args[0]} with {kwargs}"
            log(msg, verbose)

        r = requests.get(*args, **kwargs)
        r = r.json()

        return r

    def get_status(self, date, end=None, verbose=False):
        raise NotImplementedError()

    def get_fuel_mix(self, date, end=None, verbose=False):
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
        col_order = lmp_df.columns.tolist()
        # Assume sorted in ascending order
        latest_df = lmp_df.groupby("Location").last().reset_index()
        latest_df = latest_df[col_order]
        return latest_df

    def _latest_from_today(self, method, *args, **kwargs):
        data = method(date="today", *args, **kwargs)
        latest = data.iloc[-1]

        latest.index = latest.index.str.lower()

        return latest.to_dict()


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
