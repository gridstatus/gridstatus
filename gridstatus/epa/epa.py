from __future__ import annotations

import math
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from gridstatus import utils
from gridstatus.base import NoDataFoundException
from gridstatus.decorators import support_date_range
from gridstatus.epa.epa_constants import (
    ATTRIBUTE_COLUMN_RENAMES,
    ATTRIBUTE_COLUMNS,
    HOURLY_COLUMN_RENAMES,
    POWER_PLANT_EMISSIONS_GENERATION_COLUMNS,
    STATE_TIMEZONES,
)
from gridstatus.gs_logging import setup_gs_logger

logger = setup_gs_logger()

STREAMING_HOURLY_URL = (
    "https://api.epa.gov/easey/streaming-services/emissions/apportioned/hourly"
)
FACILITIES_ATTRIBUTES_URL = (
    "https://api.epa.gov/easey/facilities-mgmt/facilities/attributes"
)


class EPA:
    """EPA Clean Air Markets Program Data (CAMPD) API client."""

    default_timezone = "UTC"
    REQUEST_TIMEOUT = (10, 180)
    FACILITIES_ATTRIBUTES_PAGE_SIZE = 500

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("EPA_API_KEY") or os.getenv("EIA_API_KEY")
        if not self.api_key:
            raise ValueError(
                "API key is required. Provide it directly or set EPA_API_KEY "
                "(or EIA_API_KEY) environment variable.",
            )
        self.session = requests.Session()
        self._facility_attributes_cache: dict[
            tuple[int, tuple[str, ...] | None],
            pd.DataFrame,
        ] = {}
        self._standard_offsets: dict[str, timedelta] = {}

    def _get_standard_offset(self, state_code: str) -> timedelta:
        if state_code not in self._standard_offsets:
            tz_name = STATE_TIMEZONES.get(state_code, "UTC")
            self._standard_offsets[state_code] = datetime(
                2020,
                1,
                1,
                tzinfo=ZoneInfo(tz_name),
            ).utcoffset() or timedelta(0)
        return self._standard_offsets[state_code]

    def _request_json(
        self,
        url: str,
        params: dict[str, str | int],
        verbose: bool = False,
    ) -> tuple[object, requests.Response]:
        request_params = {"api_key": self.api_key, **params}
        if verbose:
            logger.info(f"Fetching {url} params={params}")
        response = self.session.get(
            url,
            params=request_params,
            timeout=self.REQUEST_TIMEOUT,
        )
        if response.status_code >= 400:
            try:
                message = response.json()["error"]["message"]
            except Exception:
                message = response.text
            raise requests.HTTPError(
                f"EPA API error {response.status_code}: {message}",
                response=response,
            )
        return response.json(), response

    def _fetch_hourly_emissions(
        self,
        begin_date: str,
        end_date: str,
        state_codes: list[str] | None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if not state_codes:
            payload, _ = self._request_json(
                STREAMING_HOURLY_URL,
                {"beginDate": begin_date, "endDate": end_date},
                verbose=verbose,
            )
            return pd.DataFrame(payload)

        frames: list[pd.DataFrame] = []
        for state_code in state_codes:
            payload, _ = self._request_json(
                STREAMING_HOURLY_URL,
                {
                    "beginDate": begin_date,
                    "endDate": end_date,
                    "stateCode": state_code,
                },
                verbose=verbose,
            )
            if payload:
                frames.append(pd.DataFrame(payload))
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def _fetch_facility_attributes(
        self,
        year: int,
        state_codes: list[str] | None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        cache_key = (
            year,
            tuple(state_codes) if state_codes is not None else None,
        )
        if cache_key in self._facility_attributes_cache:
            return self._facility_attributes_cache[cache_key]

        states: list[str | None] = list(state_codes) if state_codes else [None]
        frames: list[pd.DataFrame] = []
        for state_code in states:
            page = 1
            total_count: int | None = None
            while True:
                params: dict[str, str | int] = {
                    "year": year,
                    "page": page,
                    "perPage": self.FACILITIES_ATTRIBUTES_PAGE_SIZE,
                }
                if state_code is not None:
                    params["stateCode"] = state_code
                payload, response = self._request_json(
                    FACILITIES_ATTRIBUTES_URL,
                    params,
                    verbose=verbose,
                )
                items = payload.get("items", []) if isinstance(payload, dict) else []
                if items:
                    frames.append(pd.DataFrame(items))
                if total_count is None:
                    total_count = int(response.headers.get("X-Total-Count", len(items)))
                total_pages = max(
                    1,
                    math.ceil(total_count / self.FACILITIES_ATTRIBUTES_PAGE_SIZE),
                )
                if page >= total_pages:
                    break
                page += 1

        if not frames:
            attrs = pd.DataFrame(columns=ATTRIBUTE_COLUMNS)
        else:
            attrs = pd.concat(frames, ignore_index=True)
            keep = [c for c in ATTRIBUTE_COLUMNS if c in attrs.columns]
            attrs = attrs[keep].drop_duplicates(
                subset=["facilityId", "unitId"],
                keep="first",
            )

        self._facility_attributes_cache[cache_key] = attrs
        return attrs

    def _local_hours_to_utc(
        self,
        dates: pd.Series,
        hours: pd.Series,
        state_codes: pd.Series,
    ) -> pd.Series:
        naive = pd.to_datetime(dates) + pd.to_timedelta(hours.astype(int), unit="h")
        offsets = state_codes.map(self._get_standard_offset)
        return (naive - offsets).dt.tz_localize("UTC")

    @support_date_range(frequency="DAY_START")
    def get_power_plant_emissions_generation(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | None = None,
        state_codes: list[str] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Hourly apportioned emissions, heat input, and gross load for EPA CAMPD
        units, joined with facility/unit attributes.
        """
        handled_date = utils._handle_date(date, self.default_timezone)
        if handled_date is None:
            raise ValueError("date is required")
        day = handled_date.strftime("%Y-%m-%d")
        hourly = self._fetch_hourly_emissions(
            begin_date=day,
            end_date=day,
            state_codes=state_codes,
            verbose=verbose,
        )
        if hourly.empty:
            raise NoDataFoundException(
                f"No EPA CAMPD hourly emissions data found for {day}",
            )

        year = pd.Timestamp(day).year
        attrs = self._fetch_facility_attributes(
            year=year,
            state_codes=state_codes,
            verbose=verbose,
        )

        hourly = hourly.copy()
        hourly["facilityId"] = hourly["facilityId"].astype("Int64")
        hourly["unitId"] = hourly["unitId"].astype(str)
        if not attrs.empty:
            attrs = attrs.copy()
            attrs["facilityId"] = attrs["facilityId"].astype("Int64")
            attrs["unitId"] = attrs["unitId"].astype(str)
            hourly = hourly.merge(
                attrs,
                on=["facilityId", "unitId"],
                how="left",
                suffixes=("", "_attr"),
            )

        interval_start = self._local_hours_to_utc(
            hourly["date"],
            hourly["hour"],
            hourly["stateCode"],
        )
        hourly["Interval Start"] = interval_start
        hourly["Interval End"] = interval_start + pd.Timedelta(hours=1)

        hourly = hourly.rename(
            columns={**HOURLY_COLUMN_RENAMES, **ATTRIBUTE_COLUMN_RENAMES},
        )

        if "Commercial Operation Date" in hourly.columns:
            hourly["Commercial Operation Date"] = pd.to_datetime(
                hourly["Commercial Operation Date"],
                errors="coerce",
            ).dt.date

        for column in POWER_PLANT_EMISSIONS_GENERATION_COLUMNS:
            if column not in hourly.columns:
                hourly[column] = pd.NA

        return hourly[POWER_PLANT_EMISSIONS_GENERATION_COLUMNS].reset_index(drop=True)
