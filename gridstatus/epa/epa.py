import os
from datetime import datetime
from functools import lru_cache
from typing import Any, cast
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tzfpy import get_tz
from urllib3.util.retry import Retry

from gridstatus.base import NoDataFoundException, NotSupported
from gridstatus.decorators import support_date_range
from gridstatus.epa.epa_constants import (
    ATTRIBUTE_COLUMN_RENAMES,
    ATTRIBUTE_COLUMNS,
    HOURLY_COLUMN_RENAMES,
    POWER_PLANT_EMISSIONS_GENERATION_COLUMNS,
)
from gridstatus.gs_logging import setup_gs_logger

logger = setup_gs_logger()
BASE_URL = "https://api.epa.gov/easey"


@lru_cache(maxsize=10000)
def _standard_offset(
    latitude: float,
    longitude: float,
    local: datetime,
) -> pd.Timedelta:
    # CAMPD reports local standard time year-round, including during DST.
    if (
        pd.isna(latitude)
        or pd.isna(longitude)
        or not (-90 <= latitude <= 90 and -180 <= longitude <= 180)
    ):
        raise ValueError("EPA facility coordinates are missing or invalid")
    timezone = get_tz(longitude, latitude)
    if not timezone:
        raise ValueError(f"No time zone for EPA coordinates {latitude}, {longitude}")
    # Use the reporting hour: some counties changed standard time zones mid-year.
    local = local.replace(tzinfo=ZoneInfo(timezone))
    offset, dst = local.utcoffset(), local.dst()
    assert offset is not None and dst is not None
    return pd.Timedelta(offset - dst)


class EPA:
    """EPA data client."""

    default_timezone = "UTC"
    REQUEST_TIMEOUT = (10, 180)
    FACILITIES_ATTRIBUTES_PAGE_SIZE = 500

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("EPA_API_KEY")
        if not self.api_key:
            raise ValueError("Provide an API key or set EPA_API_KEY")
        self.session = requests.Session()
        self.session.mount(
            f"{BASE_URL}/",
            HTTPAdapter(
                max_retries=Retry(
                    total=3,
                    connect=0,
                    read=0,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["GET"],
                    backoff_factor=1,
                    respect_retry_after_header=True,
                    raise_on_status=False,
                ),
            ),
        )
        self._facility_attributes_cache: dict[
            tuple[int, tuple[str, ...]],
            pd.DataFrame,
        ] = {}

    def _request_json(
        self,
        path: str,
        params: dict[str, str | int],
        verbose: bool,
    ) -> Any:
        if verbose:
            logger.info(f"Fetching {path} params={params}")
        response = self.session.get(
            f"{BASE_URL}/{path}",
            params=params,
            headers={"X-Api-Key": self.api_key},
            timeout=self.REQUEST_TIMEOUT,
        )
        if response.status_code == 400:
            # EPA's validation includes its current reporting-quarter cutoff.
            raise ValueError(f"EPA rejected the request: {response.text}")
        response.raise_for_status()
        return response.json()

    def _fetch_facility_attributes(
        self,
        year: int,
        state_codes: tuple[str, ...],
        verbose: bool,
    ) -> pd.DataFrame:
        cache_key = (year, state_codes)
        if cache_key not in self._facility_attributes_cache:
            params: dict[str, str | int] = {
                "year": year,
                "perPage": self.FACILITIES_ATTRIBUTES_PAGE_SIZE,
            }
            if state_codes:
                params["stateCode"] = "|".join(state_codes)
            rows = []
            page = 1
            while True:
                items = self._request_json(
                    "facilities-mgmt/facilities/attributes",
                    {**params, "page": page},
                    verbose,
                )["items"]
                rows.extend(items)
                # The API does not consistently return a total-count header.
                if len(items) < self.FACILITIES_ATTRIBUTES_PAGE_SIZE:
                    break
                page += 1
            attrs = pd.DataFrame(rows).reindex(columns=ATTRIBUTE_COLUMNS)
            attrs["facilityId"] = attrs["facilityId"].astype("Int64")
            attrs["unitId"] = attrs["unitId"].astype("string")
            self._facility_attributes_cache[cache_key] = attrs.drop_duplicates()
        return self._facility_attributes_cache[cache_key]

    @support_date_range(frequency=None)
    def get_power_plant_emissions_generation(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | None = None,
        state_codes: list[str] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Hourly Clean Air Markets Program Data (CAMPD).

        Includes emissions, heat input, gross load, and annual unit attributes.

        Dates define a UTC interval, with an exclusive end (one day by default).
        EPA's local standard reporting hours are converted using facility coordinates;
        missing coordinates raise rather than silently guessing a time zone. Gross Load
        is MW, not MWh. Local reporting dates begin on 1995-01-01. EPA validates
        the latest supported reporting quarter; its validation errors are preserved.
        Ranges are not clamped to EPA's latest supported date. Pass error="raise"
        to propagate validation errors for date ranges.
        Reports are due within 30 days of quarter-end and may be revised later;
        an accepted date does not guarantee complete submissions.
        Use an explicit date; "latest" is not supported.
        Chunk large national date ranges at the caller to bound response size.
        """
        if date == "latest":
            raise NotSupported(
                "EPA CAMPD requires an explicit date; latest is not supported",
            )
        # The decorator normalizes dates to UTC without splitting the range.
        start = cast(pd.Timestamp, date)
        end = cast(pd.Timestamp, end or start + pd.Timedelta(days=1))
        first_available = pd.Timestamp("1995-01-01", tz="UTC")
        if start < first_available:
            raise ValueError("EPA CAMPD hourly data begins on 1995-01-01")
        states = tuple(sorted(set(state_codes or [])))
        # US local dates can precede their UTC date. Include the preceding day,
        # then trim after conversion so successive UTC chunks neither overlap nor gap.
        first_day = max(start - pd.Timedelta(days=1), first_available).date()
        last_day = (end - pd.Timedelta(nanoseconds=1)).date()
        params: dict[str, str | int] = {
            "beginDate": str(first_day),
            "endDate": str(last_day),
        }
        if states:
            params["stateCode"] = "|".join(states)
        hourly = pd.DataFrame(
            self._request_json(
                "streaming-services/emissions/apportioned/hourly",
                params,
                verbose,
            ),
        )
        if hourly.empty:
            raise NoDataFoundException(f"No EPA CAMPD hourly data for {start} to {end}")
        hourly["facilityId"] = hourly["facilityId"].astype("Int64")
        hourly["unitId"] = hourly["unitId"].astype("string")
        hourly["year"] = pd.to_datetime(hourly["date"]).dt.year
        attrs = pd.concat(
            [
                self._fetch_facility_attributes(year, states, verbose).assign(year=year)
                for year in hourly["year"].unique()
            ],
            ignore_index=True,
        )
        hourly = hourly.merge(
            attrs,
            on=["facilityId", "unitId", "year"],
            how="left",
            validate="many_to_one",
        )
        local = pd.to_datetime(hourly["date"]) + pd.to_timedelta(
            hourly["hour"],
            unit="h",
        )
        offsets = pd.to_timedelta(
            [
                _standard_offset(lat, lon, hour)
                for lat, lon, hour in zip(
                    hourly["latitude"],
                    hourly["longitude"],
                    local,
                )
            ],
        )
        hourly["Interval Start"] = (local - offsets).dt.tz_localize("UTC")
        hourly["Interval End"] = hourly["Interval Start"] + pd.Timedelta(hours=1)
        hourly = hourly.loc[
            hourly["Interval Start"].between(start, end, inclusive="left")
        ]
        if hourly.empty:
            raise NoDataFoundException(f"No EPA CAMPD hourly data for {start} to {end}")
        hourly = hourly.rename(
            columns={**HOURLY_COLUMN_RENAMES, **ATTRIBUTE_COLUMN_RENAMES},
        )
        hourly["Commercial Operation Date"] = pd.to_datetime(
            hourly["Commercial Operation Date"],
            errors="raise",
        ).dt.date
        return hourly.reindex(
            columns=POWER_PLANT_EMISSIONS_GENERATION_COLUMNS,
        ).sort_values(
            ["Interval Start", "Facility ID", "Unit ID"],
            ignore_index=True,
        )
