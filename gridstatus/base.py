import time
from enum import Enum
from typing import BinaryIO

import pandas as pd
import requests

from gridstatus.gs_logging import logger

# TODO: this is needed to make SPP request work. restrict only to SPP
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = "ALL:@SECLEVEL=1"

# not supported exception


class NotSupported(Exception):
    pass


# Custom exception to raise when no data is found
class NoDataFoundException(Exception):
    pass


class RetiredDataException(Exception):
    pass


class Markets(Enum):
    """Names of LMP Markets"""

    REAL_TIME_5_MIN = "REAL_TIME_5_MIN"

    REAL_TIME_5_MIN_EX_ANTE = "REAL_TIME_5_MIN_EX_ANTE"

    REAL_TIME_5_MIN_EX_POST_PRELIM = "REAL_TIME_5_MIN_EX_POST_PRELIM"
    REAL_TIME_5_MIN_EX_POST_FINAL = "REAL_TIME_5_MIN_EX_POST_FINAL"
    REAL_TIME_5_MIN_FINAL = "REAL_TIME_5_MIN_FINAL"

    REAL_TIME_15_MIN = "REAL_TIME_15_MIN"
    REAL_TIME_HOURLY = "REAL_TIME_HOURLY"

    REAL_TIME_HOURLY_EX_POST_PRELIM = "REAL_TIME_HOURLY_EX_POST_PRELIM"
    REAL_TIME_HOURLY_EX_POST_FINAL = "REAL_TIME_HOURLY_EX_POST_FINAL"

    DAY_AHEAD_HOURLY = "DAY_AHEAD_HOURLY"

    # "ex-ante" means before the fact, "ex-post" means after the fact
    DAY_AHEAD_HOURLY_EX_ANTE = "DAY_AHEAD_HOURLY_EX_ANTE"
    DAY_AHEAD_HOURLY_EX_POST = "DAY_AHEAD_HOURLY_EX_POST"

    REAL_TIME_HOURLY_FINAL = "REAL_TIME_HOURLY_FINAL"  # for MISO LMPs
    REAL_TIME_HOURLY_PRELIM = "REAL_TIME_HOURLY_PRELIM"  # for MISO LMPs

    # for ercot LMPs
    REAL_TIME_SCED = "REAL_TIME_SCED"

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

    default_timezone = None

    def local_now(self):
        return pd.Timestamp.now(tz=self.default_timezone)

    def _get_json(
        self,
        url: str,
        verbose: bool = False,
        retries: int | None = None,
        **kwargs,
    ):
        """
        Makes a get request to the given url and returns the json response. Optionally
        retries the request if it fails.

        Args:
            url (str): The URL to request
            verbose (bool): Whether to print log messages
            retries (int): The number of retries to attempt if the request fails. The
                total tries will be 1 + retries
            **kwargs: Additional keyword arguments to pass to requests.get

        Returns:
            dict: The JSON response from the request if successful. Otherwise, raises
                a requests.RequestException
        """
        max_attempts = 1 if retries is None else retries + 1
        attempt = 0
        while attempt < max_attempts:
            try:
                logger.info(f"Requesting {url} with {kwargs}")
                r = requests.get(url, **kwargs)
                r.raise_for_status()  # Raise an error for HTTP error codes
                return r.json()
            except requests.RequestException as e:
                attempt += 1
                if attempt >= max_attempts:
                    raise
                wait_time = 2 ** (attempt - 1)
                logger.warning(
                    f"Request failed with {e}. Retrying in {wait_time} seconds...",
                )
                time.sleep(wait_time)

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

    def get_raw_interconnection_queue(self, verbose: bool = False) -> BinaryIO:
        raise NotImplementedError()

    def get_interconnection_queue(self, verbose: bool = False):
        raise NotImplementedError()

    def _latest_lmp_from_today(self, market, locations, **kwargs):
        lmp_df = self.get_lmp(
            date="today", market=market, locations=locations, **kwargs
        )
        col_order = lmp_df.columns.tolist()

        # Special case to handle PJM 5 min LMPs
        grouper_column_name = "Location" if "Location" in col_order else "Location Id"

        # Assume sorted in ascending order
        latest_df = lmp_df.groupby(grouper_column_name).last().reset_index()
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
            s += "Reserves: %.0f %s \n" % (self.reserves, self.unit)

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
