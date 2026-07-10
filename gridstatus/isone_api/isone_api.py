import os
import time
from datetime import datetime
from typing import Literal

import pandas as pd
import polars as pl
import pytz
import requests

from gridstatus import utils
from gridstatus.base import NoDataFoundException
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import logger as log
from gridstatus.isone_api.isone_api_constants import (
    ISONE_CAPACITY_FORECAST_7_DAY_COLUMNS,
    ISONE_CONSTRAINT_DAY_AHEAD_COLUMNS,
    ISONE_CONSTRAINT_FIFTEEN_MIN_COLUMNS,
    ISONE_CONSTRAINT_FIVE_MIN_COLUMNS,
    ISONE_FCM_RECONFIGURATION_COLUMNS,
    ISONE_FIVE_MIN_ESTIMATED_ZONAL_LOAD_COLUMNS,
    ISONE_FIVE_MIN_ZONAL_LOAD_FORECAST_COLUMNS,
    ISONE_MORNING_REPORT_CITY_FIELDS,
    ISONE_MORNING_REPORT_COLUMNS,
    ISONE_MORNING_REPORT_INTERCHANGE_FIELDS,
    ISONE_MORNING_REPORT_SCALAR_MAP,
    ISONE_MORNING_REPORT_TIE_ALIASES,
    ISONE_MORNING_REPORT_TIE_DELIVERY_FIELD,
    ISONE_MORNING_REPORT_TIE_NAMES,
    ISONE_RESERVE_ZONE_ALL_COLUMNS,
    ISONE_RESERVE_ZONE_COLUMN_MAP,
    ISONE_RESERVE_ZONE_FLOAT_COLUMNS,
    ISONE_TOTAL_DEMAND_COLUMNS,
)

# Default page size for API requests
DEFAULT_PAGE_SIZE = 1000

ISO_OFFSET_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S%.f%z"


# See more info here: https://www.iso-ne.com/participate/support/web-services-data#loadzone
ZONE_LOCATIONID_MAP = {
    "NEPOOL AREA": 32,
    ".H.INTERNALHUB": 4000,
    ".Z.MAINE": 4001,
    ".Z.NEWHAMPSHIRE": 4002,
    ".Z.VERMONT": 4003,
    ".Z.CONNECTICUT": 4004,
    ".Z.RHODEISLAND": 4005,
    ".Z.SEMASS": 4006,
    ".Z.WCMASS": 4007,
    ".Z.NEMASSBOST": 4008,
}

EXCLUDE_FROM_REALTIME_HOURLY_DEMAND = [
    "NEPOOL AREA",
    ".H.INTERNALHUB",
]


class ISONEAPI:
    """
    Class to authenticate with and make requests to the ISO New England API.

    To authenticate, you need a username and password.

    To register, create an account here: https://www.iso-ne.com/participate/applications-status-changes/access-software-systems#ws-api
    """

    default_timezone = "US/Eastern"

    def __init__(
        self,
        sleep_seconds: float = 5,
        max_retries: int = 3,
    ):
        self.username = os.getenv("ISONE_API_USERNAME")
        self.password = os.getenv("ISONE_API_PASSWORD")

        if not all([self.username, self.password]):
            raise ValueError(
                "Username and password must be provided or set as environment variables",
            )

        self.base_url = "https://webservices.iso-ne.com/api/v1.1"
        self.sleep_seconds = sleep_seconds
        self.initial_delay = min(sleep_seconds, 60.0)
        self.max_retries = min(max(0, max_retries), 10)

    def parse_problematic_datetime(self, date_string: str | pd.Timestamp) -> datetime:
        if isinstance(date_string, pd.Timestamp):
            date_string = date_string.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z")
        return dt.astimezone(pytz.timezone(self.default_timezone))

    def _build_url(
        self,
        dataset: str,
        date: pd.Timestamp | Literal["latest"],
    ) -> str:
        """
        Build URL for API requests following the pattern:
        - {base_url}/{dataset}/current for "latest"
        - {base_url}/{dataset}/day/{YYYYMMDD} for specific dates

        Args:
            dataset: The dataset path (e.g., "fiveminutercp", "daasreservedata")
            date: Either "latest" for current data or a Timestamp for historical data

        Returns:
            str: The formatted URL
        """
        if date == "latest":
            return f"{self.base_url}/{dataset}/current"
        else:
            return f"{self.base_url}/{dataset}/day/{date.strftime('%Y%m%d')}"

    @staticmethod
    def _safe_get(d: dict, *keys):
        """
        Safely get nested dictionary values, returning empty dict if any key is missing.
        Returns the value at the final key, which may be a dict, list, or other type.

        Args:
            d: Dictionary to traverse
            *keys: Keys to access in nested order

        Returns:
            The value at the nested key path, or empty dict if not found
        """
        for i, k in enumerate(keys):
            if not isinstance(d, dict):
                return {}
            d = d.get(k, {})
            # If this is not the last key and the value is not a dict, return empty dict
            if i < len(keys) - 1 and not isinstance(d, dict):
                return {}
        return d

    @staticmethod
    def _prepare_records(records: dict | list[dict]) -> list[dict]:
        if not records:
            return []
        if isinstance(records, dict):
            return [records]
        return records

    def _records_to_polars(self, records: dict | list[dict]) -> pl.DataFrame:
        prepared = self._prepare_records(records)
        if not prepared:
            return pl.DataFrame()
        return pl.DataFrame(prepared)

    def _parse_offset_datetime_expr(self, col: str) -> pl.Expr:
        return (
            pl.col(col)
            .str.to_datetime(format=ISO_OFFSET_DATETIME_FORMAT)
            .dt.convert_time_zone(self.default_timezone)
        )

    def _parse_begin_date_interval_start(self, df: pl.DataFrame) -> pl.DataFrame:
        try:
            return df.with_columns(
                self._parse_offset_datetime_expr("BeginDate").alias("Interval Start"),
            )
        except Exception:
            log.warning("Standard datetime conversion failed. Using custom parsing.")
            return df.with_columns(
                pl.col("BeginDate")
                .map_elements(
                    self.parse_problematic_datetime,
                    return_dtype=pl.Datetime("us", self.default_timezone),
                )
                .alias("Interval Start"),
            )

    def _extract_location_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        if isinstance(df.schema["Location"], pl.Struct):
            return df.with_columns(
                pl.col("Location").struct.field("$").alias("Location"),
                pl.col("Location").struct.field("@LocId").alias("Location Id"),
            )
        location_df = pl.from_pandas(
            pd.json_normalize(df.get_column("Location").to_list()),
        )
        return df.with_columns(
            location_df["$"].alias("Location"),
            location_df["@LocId"].alias("Location Id"),
        )

    def make_api_call(
        self,
        url: str,
        api_params: dict = None,
        parse_json: bool = True,
        verbose: bool = False,
    ):
        if verbose:
            log.debug(f"Requesting url: {url} with params: {api_params}")
        retries = 0
        delay = self.initial_delay
        headers = {"Accept": "application/json"}
        while retries <= self.max_retries:
            response = requests.get(
                url,
                params=api_params,
                auth=(self.username, self.password),
                headers=headers,
            )

            retries += 1
            if response.status_code == 200:
                break
            elif response.status_code == 429 and retries <= self.max_retries:
                log.warn(
                    f"Warn: Rate-limited: waiting {delay} seconds before retry {retries}/{self.max_retries} "
                    f"requesting url: {url} with params: {api_params}",
                    verbose,
                )
                time.sleep(delay)
                delay *= 2
            else:
                if retries > self.max_retries:
                    error_message = (
                        f"Error: Rate-limited still after {self.max_retries} retries. "
                        f"Failed to get data from {url} with params: {api_params}"
                    )
                else:
                    error_message = (
                        f"Error: Failed to get data from {url} with params:"
                        f" {api_params}"
                    )
                log.error(error_message)
                response.raise_for_status()

        if parse_json:
            return response.json()
        else:
            return response.content

    def get_locations(self) -> pl.DataFrame:
        """
        Get a list of core hub and zone locations.

        Returns:
            polars.DataFrame: A DataFrame containing location information.
        """
        url = f"{self.base_url}/locations"
        response = self.make_api_call(url)
        if "Locations" not in response or "Location" not in response["Locations"]:
            raise NoDataFoundException("No location data found.")

        locations = response["Locations"]["Location"]
        return pl.DataFrame(locations)

    def get_location_by_id(self, location_id: int) -> pl.DataFrame:
        """
        Get information for a specific location by its ID.

        Args:
            location_id (int): The ID of the location to retrieve.

        Returns:
            polars.DataFrame: A DataFrame containing the location information.
        """
        url = f"{self.base_url}/locations/{location_id}"
        response = self.make_api_call(url)

        if "Location" not in response:
            raise NoDataFoundException(f"No data found for location ID: {location_id}")

        location = response["Location"]
        return pl.DataFrame([location])

    def get_locations_all(self) -> pl.DataFrame:
        """
        Get detailed information for all locations.

        Returns:
            polars.DataFrame: A DataFrame containing detailed information for all locations.
        """
        url = f"{self.base_url}/locations/all"
        response = self.make_api_call(url)

        if "Locations" not in response or "Location" not in response["Locations"]:
            raise NoDataFoundException("No location data found.")

        locations = response["Locations"]["Location"]
        return pl.DataFrame(locations)

    @support_date_range("DAY_START")
    def get_fuel_mix(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Return fuel mix data for the specified date range

        Args:
            date (str | pd.Timestamp): The start date for the data request. Use "latest" for most recent data.
            end (str | pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: DataFrame containing fuel mix data with timestamps and generation by fuel type
        """
        if date == "latest":
            url = f"{self.base_url}/genfuelmix/current"
        else:
            url = f"{self.base_url}/genfuelmix/day/{date.strftime('%Y%m%d')}"

        response = self.make_api_call(url)
        df = pl.DataFrame(response["GenFuelMixes"]["GenFuelMix"])

        mix_df = df.pivot(
            on="FuelCategory",
            index="BeginDate",
            values="GenMw",
            aggregate_function="first",
        ).rename({"BeginDate": "Time"})

        mix_df = mix_df.with_columns(
            self._parse_offset_datetime_expr("Time"),
        )
        fuel_cols = sorted(c for c in mix_df.columns if c != "Time")
        mix_df = mix_df.with_columns(
            [pl.col(col).fill_null(0).cast(pl.Float64) for col in fuel_cols],
        )

        return utils.move_cols_to_front(mix_df, ["Time"])

    @support_date_range("DAY_START")
    def get_marginal_fuel_type(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Return marginal-fuel flags per timestamp, one boolean column per fuel type.

        Args:
            date (str | pd.Timestamp): The start date for the data request. Use "latest" for most recent data.
            end (str | pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: One row per timestamp. Column "Time" plus one boolean
                column per fuel category (e.g. "Natural Gas", "Solar"). True
                means the fuel was marginal at that timestamp; missing
                (timestamp, fuel) pairs and timestamps with no marginal fuel
                are False.
        """
        if date == "latest":
            url = f"{self.base_url}/genfuelmix/current"
        else:
            url = f"{self.base_url}/genfuelmix/day/{date.strftime('%Y%m%d')}"

        response = self.make_api_call(url)
        df = pl.DataFrame(response["GenFuelMixes"]["GenFuelMix"])
        df = df.with_columns((pl.col("MarginalFlag") == "Y").alias("IsMarginal"))

        pivoted = df.pivot(
            on="FuelCategory",
            index="BeginDate",
            values="IsMarginal",
            aggregate_function="first",
        ).rename({"BeginDate": "Time"})

        pivoted = pivoted.with_columns(
            self._parse_offset_datetime_expr("Time"),
        )

        fuel_cols = sorted(c for c in pivoted.columns if c != "Time")
        pivoted = pivoted.with_columns(
            [pl.col(col).fill_null(False).cast(pl.Boolean) for col in fuel_cols],
        )

        return utils.move_cols_to_front(pivoted, ["Time"] + fuel_cols)

    @support_date_range("DAY_START")
    def get_morning_report(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get ISO-NE's daily morning report for the operating day."""
        url = f"{self.base_url}/morningreport/day/{date.strftime('%Y%m%d')}/all"
        response = self.make_api_call(url, verbose=verbose)
        reports = self._prepare_records(
            self._safe_get(response, "MorningReports", "MorningReport"),
        )
        if not reports:
            raise NoDataFoundException(f"No morning report data found for {date}")

        report = next(
            (
                item
                for item in reports
                if str(item.get("ReportType", "")).upper()
                in {"MR", "MORNING REPORT", "MORNINGREPORT"}
            ),
            reports[0],
        )
        df = pl.DataFrame([self._parse_morning_report(report)])
        return df.select(ISONE_MORNING_REPORT_COLUMNS)

    def _parse_morning_report(self, report: dict) -> dict[str, object]:
        """Flatten one MorningReport JSON object into the daily wide-row output schema."""
        tz = self.default_timezone

        begin_date = pd.to_datetime(report["BeginDate"], utc=True).tz_convert(tz)
        report_date = begin_date.normalize().date()

        prior_peak_time = report.get("PeakLoadYesterdayHour")
        if prior_peak_time is None or (
            isinstance(prior_peak_time, float) and pd.isna(prior_peak_time)
        ):
            prior_day = None
            prior_day_peak_hour = None
        else:
            prior_peak_time = pd.to_datetime(prior_peak_time, utc=True).tz_convert(tz)
            prior_day = prior_peak_time.normalize().date()
            prior_day_peak_hour = prior_peak_time.hour + 1

        row: dict[str, object] = {
            "Report Date": report_date,
            "Prior Day": prior_day,
            "Prior Day Peak Hour": prior_day_peak_hour,
        }
        row.update(
            {
                output: report.get(source)
                for source, output in ISONE_MORNING_REPORT_SCALAR_MAP.items()
            },
        )
        row.update(
            self._morning_report_tie_columns(
                report.get("TieDelivery"),
                ISONE_MORNING_REPORT_TIE_DELIVERY_FIELD,
            ),
        )
        row.update(
            self._morning_report_tie_columns(
                report.get("InterchangeDetail"),
                ISONE_MORNING_REPORT_INTERCHANGE_FIELDS,
            ),
        )

        cities = pl.from_pandas(
            pd.json_normalize(
                self._prepare_records(report.get("CityForecastDetail") or []),
            ),
        )
        for city_key, prefix in [("boston", "Boston"), ("hartford", "Hartford")]:
            for api_field, suffix in ISONE_MORNING_REPORT_CITY_FIELDS.items():
                row[f"{prefix} {suffix}"] = None
            if cities.is_empty() or "CityName" not in cities.columns:
                continue
            match = cities.filter(
                pl.col("CityName").str.to_lowercase() == city_key,
            )
            if match.is_empty():
                continue
            city_row = match.row(0, named=True)
            for api_field, suffix in ISONE_MORNING_REPORT_CITY_FIELDS.items():
                row[f"{prefix} {suffix}"] = city_row.get(api_field)

        return row

    def _morning_report_tie_columns(
        self,
        records: dict | list[dict] | None,
        api_field_suffix_pairs: dict[str, str],
    ) -> dict[str, object]:
        """Pivot TieDelivery or InterchangeDetail arrays into wide tie columns."""
        columns = {
            f"{tie} {suffix}": None
            for tie in ISONE_MORNING_REPORT_TIE_NAMES
            for suffix in api_field_suffix_pairs.values()
        }
        if not records:
            return columns

        ties = pl.from_pandas(
            pd.json_normalize(self._prepare_records(records)),
        )
        ties = ties.with_columns(
            pl.col("TieName")
            .map_elements(
                lambda name: ISONE_MORNING_REPORT_TIE_ALIASES.get(
                    str(name).strip().casefold(),
                    str(name).strip(),
                ),
                return_dtype=pl.Utf8,
            )
            .alias("TieName"),
        )
        for api_field, suffix in api_field_suffix_pairs.items():
            if api_field not in ties.columns:
                continue
            tie_values = {
                row["TieName"]: row[api_field]
                for row in ties.select(["TieName", api_field]).iter_rows(named=True)
            }
            for tie in ISONE_MORNING_REPORT_TIE_NAMES:
                column = f"{tie} {suffix}"
                if tie in tie_values:
                    columns[column] = tie_values[tie]
        return columns

    @support_date_range("DAY_START")
    def get_realtime_hourly_demand(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        locations: list[str] = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get the real-time hourly demand data for specified locations and date range.

        Args:
            date (str): The start date for the data request. Use "latest" for most recent data.
            end_date (str | None): The end date for the data request. Only used if date is not "latest".
            locations (list[str], optional): List of specific location names to request data for.
                                             If None, data for all locations will be retrieved.

        Returns:
            polars.DataFrame: A DataFrame containing the real-time hourly demand data for all requested locations.
        """
        all_data = []

        match (date, locations):
            case ("latest", None):
                url = f"{self.base_url}/realtimehourlydemand/current"
                response = self.make_api_call(url)
                df = pl.DataFrame(response["HourlyRtDemands"]["HourlyRtDemand"])
                df = df.with_columns(
                    pl.col("Location").struct.field("@LocId").alias("LocId"),
                    pl.col("Location").struct.field("$").alias("Location"),
                )
                return self._handle_demand(df, interval_minutes=60)

            case ("latest", _):
                for location in locations:
                    location_id = ZONE_LOCATIONID_MAP.get(location)
                    if not location_id:
                        raise ValueError(
                            f"{location}: Not a known ISO NE Hub or Zone for this data",
                        )
                    url = f"{self.base_url}/realtimehourlydemand/current/location/{location_id}"
                    response = self.make_api_call(url)
                    data = response["HourlyRtDemand"]
                    if not data:
                        raise NoDataFoundException(
                            f"No data found for location: {location}",
                        )
                    data["Location"] = location
                    data["LocId"] = location_id
                    all_data.append(data)

            case _:
                if not locations:
                    locations = [
                        loc
                        for loc in ZONE_LOCATIONID_MAP.keys()
                        if loc not in EXCLUDE_FROM_REALTIME_HOURLY_DEMAND
                    ]

                for location in locations:
                    location_id = ZONE_LOCATIONID_MAP.get(location)
                    if (
                        not location_id
                        or location in EXCLUDE_FROM_REALTIME_HOURLY_DEMAND
                    ):
                        raise ValueError(
                            f"{location}: Not a known ISO NE Hub or Zone for this data",
                        )

                    url = f"{self.base_url}/realtimehourlydemand/day/{date.strftime('%Y%m%d')}/location/{location_id}"
                    response = self.make_api_call(url)
                    data = response["HourlyRtDemands"]["HourlyRtDemand"]
                    if not data:
                        raise NoDataFoundException(
                            f"No data found for location: {location}. In favor of not returning incomplete data based on the request, no data has been returned. Please try again.",
                        )
                    for item in data:
                        item["Location"] = location
                        item["LocId"] = location_id
                    all_data.extend(data)

        if not all_data:
            raise NoDataFoundException(
                "No real-time hourly demand data found for the specified parameters.",
            )

        df = pl.DataFrame(all_data)
        return self._handle_demand(df, interval_minutes=60)

    @support_date_range("DAY_START")
    def get_load_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Return hourly load data for a given date or date range

        Args:
            date (str | pd.Timestamp): The start date for the data request. Use "latest" for most recent data.
            end (str | pd.Timestamp, optional): The end date for the data request. Only used if date is not "latest".
            locations (list[str], optional): List of specific location names to request data for.
                                             If None, data for all locations will be retrieved.
            verbose (bool, optional): Whether to print verbose logging information.


        Returns:
            pl.DataFrame: DataFrame containing load data with timestamps and load
        """

        if date == "latest":
            url = f"{self.base_url}/hourlysysload/current"
            response = self.make_api_call(url)
            raw_data = [response["HourlySystemLoads"]["HourlySystemLoad"]]
        else:
            url = f"{self.base_url}/hourlysysload/day/{date.strftime('%Y%m%d')}"
            response = self.make_api_call(url)
            raw_data = response["HourlySystemLoads"]["HourlySystemLoad"]

        df = pl.from_pandas(
            pd.json_normalize(
                raw_data,
                meta=["BeginDate", "Load", "NativeLoad", "ArdDemand"],
            ),
        )
        df = df.with_columns(
            pl.col("Location.$").alias("Location"),
            pl.col("Location.@LocId").alias("LocId"),
        ).drop(["Location.$", "Location.@LocId"])
        df = df.rename(
            {
                "NativeLoad": "Native Load",
                "ArdDemand": "ARD Demand",
            },
        )

        return self._handle_demand(
            df,
            interval_minutes=60,
            columns=[
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Load",
                "Native Load",
                "ARD Demand",
            ],
        )

    @support_date_range("DAY_START")
    def get_dayahead_hourly_demand(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        locations: list[str] = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get the day-ahead hourly demand data for specified locations and date range.

        Args:
            date (str): The start date for the data request. Use "latest" for most recent data.
            end_date (str | None): The end date for the data request. Only used if date is not "latest".
            locations (list[str], optional): List of specific location names to request data for.
                                             If None, data for all locations will be retrieved.

        Returns:
            polars.DataFrame: A DataFrame containing the day-ahead hourly demand data for all requested locations.
        """
        all_data = []

        match (date, locations):
            case ("latest", None):
                url = f"{self.base_url}/dayaheadhourlydemand/current"
                response = self.make_api_call(url)
                df = pl.DataFrame(response["HourlyDaDemands"]["HourlyDaDemand"])
                df = df.with_columns(
                    pl.col("Location").struct.field("@LocId").alias("LocId"),
                    pl.col("Location").struct.field("$").alias("Location"),
                )
                return self._handle_demand(df, interval_minutes=60)

            case ("latest", _):
                for location in locations:
                    location_id = ZONE_LOCATIONID_MAP.get(location)
                    if not location_id:
                        raise ValueError(
                            f"{location}: Not a known ISO NE Hub or Zone for this data",
                        )
                    url = f"{self.base_url}/dayaheadhourlydemand/current/location/{location_id}"
                    response = self.make_api_call(url)
                    data = response["HourlyDaDemand"]
                    if not data:
                        raise NoDataFoundException(
                            f"No data found for location: {location}. In favor of not returning incomplete data based on the request, no data has been returned. Please try again.",
                        )
                    data["Location"] = location
                    data["LocId"] = location_id
                    all_data.append(data)

            case _:
                if not locations:
                    locations = list(ZONE_LOCATIONID_MAP.keys())

                for location in locations:
                    location_id = ZONE_LOCATIONID_MAP.get(location)
                    if not location_id:
                        raise ValueError(
                            f"{location}: Not a known ISO NE Hub or Zone for this data",
                        )

                    url = f"{self.base_url}/dayaheadhourlydemand/day/{date.strftime('%Y%m%d')}/location/{location_id}"
                    response = self.make_api_call(url)
                    data = response["HourlyDaDemands"]["HourlyDaDemand"]
                    for item in data:
                        item["Location"] = location
                        item["LocId"] = location_id
                    all_data.extend(data)

        if not all_data:
            raise NoDataFoundException(
                "No day-ahead hourly demand data found for the specified parameters.",
            )

        df = pl.DataFrame(all_data)
        # NOTE(kladar): 2017-07-01 to 2018-06-01 causes an issue
        # as there are duplicates of the .H.INTERNALHUB location. Deduping them here
        df = df.unique(
            subset=["BeginDate", "Location"],
            keep="first",
            maintain_order=True,
        )

        return self._handle_demand(df, interval_minutes=60)

    def _handle_demand(
        self,
        df: pl.DataFrame,
        interval_minutes: int = 60,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Process demand DataFrame: convert types, rename columns, and add Interval End.

        Args:
            df (pl.DataFrame): Input DataFrame with demand data.
            interval_minutes (int): Duration of each interval in minutes. Default is 60.

        Returns:
            pl.DataFrame: Processed DataFrame.
        """
        if columns is None:
            columns = [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Load",
            ]

        df = self._parse_begin_date_interval_start(df)
        df = df.sort(["Interval Start", "Location"])
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(minutes=interval_minutes)).alias(
                "Interval End",
            ),
            pl.col("Load").cast(pl.Float64, strict=False),
            pl.col("LocId").cast(pl.Float64, strict=False).alias("Location Id"),
        )

        log.info(
            f"Processed demand data: {df.height} entries from {df['Interval Start'].min()} to {df['Interval Start'].max()}",
        )
        return df.select(columns)

    @support_date_range("DAY_START")
    def get_load_forecast_hourly(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        vintage: Literal["latest", "all"] = "all",
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get the hourly load forecast data for specified locations and date range.

        NB: ISO NE publishes load forecasts roughly every 30 minutes for the next 48-72 future intervals.
        Getting all forecasts (all "vintages") can be a lot of data, potentially thousands of rows for a single day.
        Sometimes you may want this, and that's why ISO NE provides the option to get all vintages, but you may be most interested
        in the most recent forecast for a given historical interval, essentially the shortest vintages, most
        accurate forecast, which they also provide. All vintages is typically 5x to 20x more data than latest,
        so it's something to consider when making a request.

        Giving the option for just the "latest" forecast (aka shortest horizon, aka most recent publish time/vintage)
        for a given historical interval avoids this large data pull and collation since ISO NE API
        has done that work for you already.

        Args:
            date (str): The start date for the data request. Use "latest" for most recent data.
            end_date (str | None): The end date for the data request. Only used if date is not "latest".
            vintage (Literal["latest", "all"]): The vintage for the data request. Options are "latest" or "all", defaults to "all".

        Returns:
            polars.DataFrame: A DataFrame containing the hourly load forecast data for the system.
        """

        if date == "latest":
            url = f"{self.base_url}/hourlyloadforecast/current"
            response = self.make_api_call(url)
            df = pl.DataFrame(response["HourlyLoadForecast"])
            return self._handle_load_forecast(df, interval_minutes=60)

        elif vintage == "all":
            url = (
                f"{self.base_url}/hourlyloadforecast/all/day/{date.strftime('%Y%m%d')}"
            )
        else:
            url = f"{self.base_url}/hourlyloadforecast/day/{date.strftime('%Y%m%d')}"

        response = self.make_api_call(url)
        df = pl.DataFrame(response["HourlyLoadForecasts"]["HourlyLoadForecast"])
        return self._handle_load_forecast(df, interval_minutes=60)

    @support_date_range("DAY_START")
    def get_reliability_region_load_forecast(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        vintage: Literal["latest", "all"] = "all",
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get the regional load forecast data for specified date range and vintages.

        Args:
            date (str): The start date for the data request. Use "latest" for most recent data.
            end (str | None): The end date for the data request. Only used if date is not "latest".
            vintages (Literal["latest", "all"]): The vintage for the data request. Options are "latest" or "all".

        Returns:
            polars.DataFrame: A DataFrame containing the regional load forecast data for all requested locations.
        """

        if date == "latest":
            url = f"{self.base_url}/reliabilityregionloadforecast/current"
        elif vintage == "all":
            url = f"{self.base_url}/reliabilityregionloadforecast/day/{date.strftime('%Y%m%d')}/all"
        else:
            url = f"{self.base_url}/reliabilityregionloadforecast/day/{date.strftime('%Y%m%d')}"

        response = self.make_api_call(url)
        df = pl.DataFrame(
            response["ReliabilityRegionLoadForecasts"]["ReliabilityRegionLoadForecast"],
        )
        return self._handle_load_forecast(df, interval_minutes=60)

    def _handle_load_forecast(
        self,
        df: pl.DataFrame,
        interval_minutes: int = 60,
    ) -> pl.DataFrame:
        """
        Process load forecast DataFrame: convert types, rename columns, and add Interval End.

        Args:
            df (pl.DataFrame): Input DataFrame with load forecast data.
            interval_minutes (int): Duration of each interval in minutes. Default is 60.

        Returns:
            pl.DataFrame: Processed DataFrame.
        """
        try:
            df = df.with_columns(
                self._parse_offset_datetime_expr("BeginDate"),
                self._parse_offset_datetime_expr("CreationDate"),
            )
        except Exception:
            # NOTE(kladar) consistently the above logic fails for DST conversion days.
            # This catches those and fixes it.
            # Data coming out looks good, no missed intervals and no duplicates.
            log.warning("Standard datetime conversion failed. Using custom parsing.")
            df = df.with_columns(
                pl.col("BeginDate")
                .map_elements(
                    self.parse_problematic_datetime,
                    return_dtype=pl.Datetime("us", self.default_timezone),
                )
                .alias("BeginDate"),
                pl.col("CreationDate")
                .map_elements(
                    self.parse_problematic_datetime,
                    return_dtype=pl.Datetime("us", self.default_timezone),
                )
                .alias("CreationDate"),
            )

        df = df.with_columns(
            (pl.col("BeginDate") + pl.duration(minutes=interval_minutes)).alias(
                "Interval End",
            ),
        )
        regional_cols = {
            "BeginDate": "Interval Start",
            "Interval End": "Interval End",
            "CreationDate": "Publish Time",
            "ReliabilityRegion": "Location",
            "LoadMw": "Load Forecast",
            "ReliabilityRegionLoadPercentage": "Regional Percentage",
        }
        system_cols = {
            "BeginDate": "Interval Start",
            "Interval End": "Interval End",
            "CreationDate": "Publish Time",
            "LoadMw": "Load Forecast",
            "NetLoadMw": "Net Load Forecast",
        }
        if "ReliabilityRegion" in df.columns:
            df = df.rename(regional_cols)
            df = df.with_columns(
                pl.col("Regional Percentage").cast(pl.Float64, strict=False),
            )
            output_cols = list(regional_cols.values())
        else:
            df = df.rename(system_cols)
            df = df.with_columns(
                pl.col("Net Load Forecast").cast(pl.Float64, strict=False),
            )
            output_cols = list(system_cols.values())

        df = df.sort(["Interval Start", "Publish Time"])
        df = df.with_columns(pl.col("Load Forecast").cast(pl.Float64, strict=False))

        log.info(
            f"Processed load forecast data: {df.height} entries from {df['Interval Start'].min()} to {df['Interval Start'].max()}",
        )
        return df.select(output_cols)

    @support_date_range("DAY_START")
    def get_interchange_hourly(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get the hourly interchange data for specified date range. Hourly data includes
        multiple locations.

        Args:
            date (str): The start date for the data request. Use "latest" for most
            recent data.
            end_date (str | None): The end date for the data request. Only used if date
            is not "latest".

        Returns:
            polars.DataFrame: A DataFrame containing the interchange fifteen minute
            data for all requested locations.
        """
        if date == "latest":
            url = f"{self.base_url}/actualinterchange/current"
        else:
            url = f"{self.base_url}/actualinterchange/day/{date.strftime('%Y%m%d')}"

        log.info(f"Requesting interchange data for date: {date}")

        response = self.make_api_call(url)

        if data := response.get("ActualInterchanges"):
            df = pl.DataFrame(
                data["ActualInterchange"],
            )
        else:
            raise NoDataFoundException(f"No hourly interchange data found for {date}")

        return self._handle_interchange_dataframe(df, interval_minutes=60)

    @support_date_range("DAY_START")
    def get_interchange_15_min(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get the fifteen minute interchange data for specified date range.

        Args:
            date (str): The start date for the data request. Use "latest" for most
            recent data.
            end_date (str | None): The end date for the data request. Only used if date
            is not "latest".

        Returns:
            polars.DataFrame: A DataFrame containing the interchange fifteen minute
            data for all requested locations.
        """
        if date == "latest":
            url = f"{self.base_url}/fifteenminuteinterchange/current"
        else:
            url = f"{self.base_url}/fifteenminuteinterchange/day/{date.strftime('%Y%m%d')}"  # noqa: E501

        log.info(f"Requesting interchange data for date: {date}")

        response = self.make_api_call(url)

        if data := response.get("ActualFifteenMinInterchanges"):
            df = pl.DataFrame(data["ActualFifteenMinInterchange"])
        else:
            raise NoDataFoundException(
                f"No fifteen minute interchange data found for {date}",
            )

        return self._handle_interchange_dataframe(df, interval_minutes=15)

    def _handle_interchange_dataframe(
        self,
        df: pl.DataFrame,
        interval_minutes: int,
    ) -> pl.DataFrame:
        df = df.with_columns(
            self._parse_offset_datetime_expr("BeginDate").alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(minutes=interval_minutes)).alias(
                "Interval End",
            ),
        )
        df = self._extract_location_columns(df)
        df = df.rename({"ActInterchange": "Actual Interchange"})

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Actual Interchange",
                "Purchase",
                "Sale",
            ],
        ).sort(["Interval Start", "Location"])

    @support_date_range("DAY_START")
    def get_external_flows_5_min(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get the five minute external flow data for specified date range.

        Args:
            date (str): The start date for the data request. Use "latest" for most
            recent data.
            end_date (str | None): The end date for the data request. Only used if date
            is not "latest".

        Returns:
            polars.DataFrame: A DataFrame containing the external flow five minute
            data for all requested locations.
        """
        if date == "latest":
            url = f"{self.base_url}/fiveminuteexternalflow/current"
        else:
            url = (
                f"{self.base_url}/fiveminuteexternalflow/day/{date.strftime('%Y%m%d')}"
            )

        log.info(f"Requesting external flow data for date: {date}")

        response = self.make_api_call(url)

        if data := response.get("ExternalFlows"):
            df = pl.DataFrame(data["ExternalFlow"])
        else:
            raise NoDataFoundException(
                f"No five minute external flow data found for {date}",
            )

        return self._handle_external_flows_dataframe(df, interval_minutes=5)

    def _handle_external_flows_dataframe(
        self,
        df: pl.DataFrame,
        interval_minutes: int,
    ) -> pl.DataFrame:
        df = df.with_columns(
            self._parse_offset_datetime_expr("BeginDate").alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(minutes=interval_minutes)).alias(
                "Interval End",
            ),
        )

        # Split location column from {'$': '.I.ROSETON 345 1', '@LocId': '4011'} to
        # Location and Location Id
        df = self._extract_location_columns(df)

        df = df.rename(
            {
                "ActualFlow": "Actual Flow",
                "ImportLimit": "Import Limit",
                "ExportLimit": "Export Limit",
                "CurrentSchedule": "Current Schedule",
                "TotalExports": "Total Exports",
                "TotalImports": "Total Imports",
            },
        )

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Actual Flow",
                "Import Limit",
                "Export Limit",
                "Current Schedule",
                "Purchase",
                "Sale",
                "Total Exports",
                "Total Imports",
            ],
        ).sort(["Interval Start", "Location"])

    @support_date_range("DAY_START")
    def get_zonal_load_estimated_5_min(
        self,
        date: str | pd.Timestamp | Literal["latest"],
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get five-minute estimated zonal load data for all load zones.

        Args:
            date (pd.Timestamp | Literal["latest"]): The start date for the data
                request. Use "latest" for most recent data.
            end (pd.Timestamp | None): The end date for the data request. Only used
                if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: A DataFrame containing five-minute estimated zonal load data.
        """
        url = self._build_url("fiveminuteestimatedzonalload", date)
        response = self.make_api_call(url, verbose=verbose)

        records = self._prepare_records(
            self._safe_get(
                response,
                "isone_web_services",
                "five_min_estimated_zonal_loads",
                "five_min_estimated_zonal_load",
            ),
        )

        if not records:
            raise NoDataFoundException(
                f"No five-minute estimated zonal load data found for {date}",
            )

        df = pl.DataFrame(records)

        df = df.with_columns(
            pl.col("interval_begin_date")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(minutes=5)).alias("Interval End"),
        )

        df = df.rename(
            {
                "load_zone_id": "Load Zone ID",
                "load_zone_name": "Load Zone Name",
                "estimated_load_mw": "Estimated Load",
                "estimated_btm_pv_mw": "Estimated BTM Solar",
            },
        )

        df = df.with_columns(
            pl.col("Load Zone ID").cast(pl.Float64, strict=False),
            pl.col("Estimated Load").cast(pl.Float64, strict=False),
            pl.col("Estimated BTM Solar").cast(pl.Float64, strict=False),
        )

        return df.select(ISONE_FIVE_MIN_ESTIMATED_ZONAL_LOAD_COLUMNS).sort(
            ["Interval Start", "Load Zone ID"],
        )

    def get_load_forecast_by_zone_5_min(
        self,
        date: str | pd.Timestamp | Literal["latest"] = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get five-minute zonal load forecast data for all load zones.

        Args:
            date (pd.Timestamp | Literal["latest"]): Unused. Kept for API
                compatibility. This endpoint always returns the current forecast.
            end (pd.Timestamp | None): Unused. Date ranges are not supported.
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: A DataFrame containing five-minute zonal load forecast data.
                Publish Time comes from the /info endpoint CreationDate field.
        """
        if end is not None:
            raise ValueError(
                "Date ranges are not supported for five-minute zonal load forecast. "
                "Use date='latest'.",
            )
        data_url = self._build_url("fiveminutezonalloadforecast", "latest")

        info_response = self.make_api_call(
            f"{self.base_url}/fiveminutezonalloadforecast/info",
            verbose=verbose,
        )
        response = self.make_api_call(data_url, verbose=verbose)
        publish_time = pd.to_datetime(
            info_response["ServiceInfo"]["CreationDate"],
            utc=True,
        ).tz_convert(self.default_timezone)

        records = self._prepare_records(
            self._safe_get(
                response,
                "isone_web_services",
                "five_min_zonal_forecast_data",
                "five_min_zonal_forecast",
            ),
        )

        if not records:
            raise NoDataFoundException(
                "No five-minute zonal load forecast data found",
            )

        df = pl.DataFrame(records)

        df = df.with_columns(
            pl.col("interval_begin_date")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(minutes=5)).alias("Interval End"),
        )

        df = df.rename(
            {
                "load_zone_id": "Load Zone ID",
                "load_zone_name": "Load Zone Name",
                "load_mw": "Load Forecast",
                "btm_pv_mw": "BTM Solar Forecast",
            },
        )

        df = df.with_columns(
            pl.lit(publish_time).alias("Publish Time"),
            pl.col("Load Zone ID").cast(pl.Float64, strict=False),
            pl.col("Load Forecast").cast(pl.Float64, strict=False),
            pl.col("BTM Solar Forecast").cast(pl.Float64, strict=False),
        )

        return df.select(ISONE_FIVE_MIN_ZONAL_LOAD_FORECAST_COLUMNS).sort(
            ["Interval Start", "Load Zone Name"],
        )

    @support_date_range("DAY_START")
    def get_total_demand(
        self,
        date: str | pd.Timestamp | Literal["latest"],
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get five-minute system load ("total demand") data.

        Includes Total Load, Native Load, Storage Load, and the
        behind-the-meter (estimated solar) variants, as reported by the
        ISO-NE /fiveminutesystemload web service endpoint.

        Args:
            date (pd.Timestamp | Literal["latest"]): The start date for the
                data request. Use "latest" for most recent data.
            end (pd.Timestamp | None): The end date for the data request. Only
                used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: A DataFrame containing five-minute total demand data.
        """
        url = self._build_url("fiveminutesystemload", date)
        response = self.make_api_call(url, verbose=verbose)

        # /current returns {"FiveMinSystemLoad": [...]}; /day nests it under
        # "FiveMinSystemLoads". Collapse both shapes to the inner container.
        container = response.get("FiveMinSystemLoads", response)
        records = self._prepare_records(container.get("FiveMinSystemLoad"))

        if not records:
            raise NoDataFoundException(
                f"No five-minute system load data found for {date}",
            )

        df = pl.DataFrame(records)

        df = df.with_columns(
            self._parse_offset_datetime_expr("BeginDate").alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(minutes=5)).alias("Interval End"),
        )

        df = df.rename(
            {
                "LoadMw": "Total Load",
                "NativeLoad": "Native Load",
                "ArdDemand": "Storage Load",
                "SystemLoadBtmPv": "Total Load With Estimated Solar",
                "NativeLoadBtmPv": "Native Load With Estimated Solar",
            },
        )

        df = df.with_columns(
            [
                pl.col(col).cast(pl.Float64, strict=False)
                for col in ISONE_TOTAL_DEMAND_COLUMNS[2:]
            ],
        )

        return df.select(ISONE_TOTAL_DEMAND_COLUMNS).sort("Interval Start")

    @support_date_range("HOUR_START")
    def get_lmp_real_time_5_min_prelim(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get the real-time 5 minute LMP preliminary data for specified date range.

        Args:
            date (str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp]): The start date for the data request. Use "latest" for most recent data.
            end (str | pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: A DataFrame containing the real-time 5 minute LMP preliminary data.
        """

        if date == "latest":
            url = f"{self.base_url}/fiveminutelmp/prelim/current/all"
        else:
            url = f"{self.base_url}/fiveminutelmp/prelim/day/{date.strftime('%Y%m%d')}/starthour/{date.hour:02d}"

        return self._handle_lmp_real_time(url, verbose, interval_minutes=5)

    @support_date_range("HOUR_START")
    def get_lmp_real_time_5_min_final(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get the real-time 5 minute LMP final data for specified date range.

        Args:
            date (str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp]): The start date for the data request. Use "latest" for most recent data.
            end (str | pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: A DataFrame containing the real-time 5 minute LMP final data.
        """

        if date == "latest":
            # NB: We don't quite know when this is published each day,
            # and don't have a /current/all option for final data, so grab the full day on "latest"
            try:
                return self.get_lmp_real_time_5_min_final("today")
            except Exception:
                try:
                    return self.get_lmp_real_time_5_min_final(
                        pd.Timestamp.now(self.default_timezone) - pd.DateOffset(days=1),
                    )
                except Exception:
                    return self.get_lmp_real_time_5_min_final(
                        pd.Timestamp.now(self.default_timezone) - pd.DateOffset(days=2),
                    )

        url = f"{self.base_url}/fiveminutelmp/final/day/{date.strftime('%Y%m%d')}/starthour/{date.hour:02d}"
        return self._handle_lmp_real_time(url, verbose, interval_minutes=5)

    @support_date_range("DAY_START")
    def get_lmp_real_time_hourly_prelim(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get the real-time hourly LMP data for specified date range.

        Args:
            date (str | pd.Timestamp): The start date for the data request. Use "latest" for most
            recent data.
            end_date (str | pd.Timestamp | None): The end date for the data request. Only used if date
            is not "latest".

        Returns:
            pandas.DataFrame: A DataFrame containing the real-time hourly LMP data.
        """
        if date == "latest":
            return self.get_lmp_real_time_hourly_prelim("today", verbose)
        else:
            url = f"{self.base_url}/hourlylmp/rt/prelim/day/{date.strftime('%Y%m%d')}"

        return self._handle_lmp_real_time(url, verbose)

    @support_date_range("DAY_START")
    def get_lmp_real_time_hourly_final(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get the real-time hourly LMP data for specified date range.

        Args:
            date (str | pd.Timestamp): The start date for the data request. Use "latest" for most
            recent data.
            end_date (str | pd.Timestamp | None): The end date for the data request. Only used if date
            is not "latest".

        Returns:
            pandas.DataFrame: A DataFrame containing the real-time hourly LMP data.
        """
        if date == "latest":
            return self.get_lmp_real_time_hourly_prelim("today", verbose)

        url = f"{self.base_url}/hourlylmp/rt/final/day/{date.strftime('%Y%m%d')}"
        return self._handle_lmp_real_time(url, verbose)

    def _handle_lmp_real_time(
        self,
        url: str,
        verbose: bool = False,
        interval_minutes: int = 60,
    ) -> pl.DataFrame:
        response = self.make_api_call(url)

        if interval_minutes == 60:
            df = pl.DataFrame(response["HourlyLmps"]["HourlyLmp"])
        else:
            df = pl.DataFrame(response["FiveMinLmps"]["FiveMinLmp"])

        df = df.with_columns(
            self._parse_offset_datetime_expr("BeginDate").alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(minutes=interval_minutes)).alias(
                "Interval End",
            ),
            pl.col("Location").struct.field("@LocType").alias("Location Type"),
            pl.col("Location").struct.field("$").alias("Location"),
        )
        df = df.rename(
            {
                "LmpTotal": "LMP",
                "EnergyComponent": "Energy",
                "CongestionComponent": "Congestion",
                "LossComponent": "Loss",
            },
        )

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ],
        ).sort(["Interval Start", "Location"])

    @support_date_range("DAY_START")
    def get_capacity_forecast_7_day(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get the capacity forecast for the next 7 days.
        """
        if date == "latest":
            url = f"{self.base_url}/sevendayforecast/current"
        else:
            url = f"{self.base_url}/sevendayforecast/day/{date.strftime('%Y%m%d')}/all"

        return self._handle_capacity_forecast(url, verbose)

    def _handle_capacity_forecast(
        self,
        url: str,
        verbose: bool = False,
    ) -> pl.DataFrame:
        response = self.make_api_call(url)
        df = pl.from_pandas(
            pd.json_normalize(
                response["SevenDayForecasts"]["SevenDayForecast"],
                record_path=["MarketDay"],
                meta=["CreationDate"],
            ),
        )

        df = df.with_columns(
            pl.col("CreationDate")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Publish Time"),
            pl.col("MarketDate")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(days=1)).alias("Interval End"),
        )

        df = df.rename(
            {
                "TotAvailGenMw": "Total Generation Available",
                "CsoMw": "Total Capacity Supply Obligation",
                "ColdWeatherOutagesMw": "Anticipated Cold Weather Outages",
                "OtherGenOutagesMw": "Other Generation Outages",
                "DelistMw": "Anticipated Delist MW Offered",
                "PeakImportMw": "Import at Time of Peak",
                "TotAvailGenImportMw": "Total Available Generation and Imports",
                "PeakLoadMw": "Projected Peak Load",
                "ReplReserveReqMw": "Replacement Reserve Requirement",
                "ReqdReserveMw": "Required Reserve",
                "ReqdReserveInclReplMw": "Required Reserve Including Replacement",
                "TotLoadPlusReqdReserveMw": "Total Load Plus Required Reserve",
                "SurplusDeficiencyMw": "Projected Surplus or Deficiency",
                "DrrMw": "Available Demand Response Resources",
                "PowerWatch": "Power Watch",
                "PowerWarn": "Power Warning",
                "ColdWeatherWatch": "Cold Weather Watch",
                "ColdWeatherWarn": "Cold Weather Warning",
                "ColdWeatherEvent": "Cold Weather Event",
            },
        )

        # NB: These are often missing, but they can be present, so we set them to None here
        for col in [
            "Available Realtime Emergency Generation",
            "Load Relief Actions Anticipated",
            "Generating Capacity Position",
        ]:
            df = df.with_columns(pl.lit(None).alias(col))

        def _city_weather_value(
            cities: list[dict] | None,
            city_name: str,
            field: str,
        ) -> object:
            if not cities:
                return None
            for city in cities:
                if city.get("CityName") == city_name:
                    return city.get(field)
            return None

        df = df.with_columns(
            pl.col("Weather.CityWeather")
            .map_elements(
                lambda cities: _city_weather_value(cities, "Boston", "HighTempF"),
                return_dtype=pl.Float64,
            )
            .alias("High Temperature Boston"),
            pl.col("Weather.CityWeather")
            .map_elements(
                lambda cities: _city_weather_value(cities, "Hartford", "HighTempF"),
                return_dtype=pl.Float64,
            )
            .alias("High Temperature Hartford"),
            pl.col("Weather.CityWeather")
            .map_elements(
                lambda cities: _city_weather_value(cities, "Boston", "DewPointF"),
                return_dtype=pl.Float64,
            )
            .alias("Dew Point Boston"),
            pl.col("Weather.CityWeather")
            .map_elements(
                lambda cities: _city_weather_value(cities, "Hartford", "DewPointF"),
                return_dtype=pl.Float64,
            )
            .alias("Dew Point Hartford"),
        )

        return df.select(ISONE_CAPACITY_FORECAST_7_DAY_COLUMNS)

    @support_date_range("DAY_START")
    def get_regulation_clearing_prices_real_time_5_min(
        self,
        date: str | pd.Timestamp | Literal["latest"],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get five-minute clearing prices for both regulation capacity and service in real-time.

        Args:
            date (pd.Timestamp | Literal["latest"]): The start date for the data request. Use "latest" for most recent data.
            end (pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: A DataFrame containing five-minute regulation clearing prices.
        """
        url = self._build_url("fiveminutercp", date)
        response = self.make_api_call(url, verbose=verbose)
        df = pl.DataFrame(self._safe_get(response, "FiveMinRcps", "FiveMinRcp"))

        if df.is_empty():
            raise NoDataFoundException(
                f"No five-minute regulation clearing price data found for {date}",
            )

        # Timestamps already have an offset, so we parse as UTC then convert to local
        # Floor to 5-minute intervals to handle API data inconsistencies (sometimes returns :01, :02, etc instead of :00). Round in UTC to avoid DST issues
        df = df.with_columns(
            pl.col("BeginDate")
            .str.to_datetime(time_zone="UTC")
            .dt.truncate("5m")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(minutes=5)).alias("Interval End"),
        )

        df = df.rename(
            {
                "RegServiceClearingPrice": "Reg Service Clearing Price",
                "RegCapacityClearingPrice": "Reg Capacity Clearing Price",
            },
        )

        df = df.with_columns(
            pl.col("Reg Service Clearing Price").cast(pl.Float64),
            pl.col("Reg Capacity Clearing Price").cast(pl.Float64),
        )

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Reg Service Clearing Price",
                "Reg Capacity Clearing Price",
            ],
        ).sort("Interval Start")

    @support_date_range("DAY_START")
    def get_reserve_requirements_prices_forecast_day_ahead(
        self,
        date: str | pd.Timestamp | Literal["latest"],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get day-ahead reserve prices, requirements, and forecast for reserve zone 7000 (system-wide).

        Args:
            date (pd.Timestamp | Literal["latest"]): The start date for the data request. Use "latest" for most recent data.
            end (pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: A DataFrame containing day-ahead reserve requirements, prices, and forecast.
        """
        url = self._build_url("daasreservedata", date)
        response = self.make_api_call(url, verbose=verbose)

        df = pl.from_pandas(
            pd.json_normalize(
                self._safe_get(
                    response,
                    "isone_web_services",
                    "day_ahead_reserves",
                    "day_ahead_reserve",
                ),
            ),
        )

        if df.is_empty():
            raise NoDataFoundException(f"No day-ahead reserve data found for {date}")

        # Parse market hour information - API returns timezone-aware datetimes
        df = df.with_columns(
            pl.col("market_hour.local_day")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(hours=1)).alias("Interval End"),
        )

        df = df.rename(
            {
                "eir_designation_mw": "EIR Designation MW",
                "fer_clearing_price": "FER Clearing Price",
                "forecasted_energy_req_mw": "Forecasted Energy Req MW",
                "ten_min_spin_req_mw": "Ten Min Spin Req MW",
                "tmnsr_clearing_price": "TMNSR Clearing Price",
                "tmnsr_designation_mw": "TMNSR Designation MW",
                "tmor_clearing_price": "TMOR Clearing Price",
                "tmor_designation_mw": "TMOR Designation MW",
                "tmsr_clearing_price": "TMSR Clearing Price",
                "tmsr_designation_mw": "TMSR Designation MW",
                "total_ten_min_req_mw": "Total Ten Min Req MW",
                "total_thirty_min_req_mw": "Total Thirty Min Req MW",
            },
        )

        numeric_columns = [
            "EIR Designation MW",
            "FER Clearing Price",
            "Forecasted Energy Req MW",
            "Ten Min Spin Req MW",
            "TMNSR Clearing Price",
            "TMNSR Designation MW",
            "TMOR Clearing Price",
            "TMOR Designation MW",
            "TMSR Clearing Price",
            "TMSR Designation MW",
            "Total Ten Min Req MW",
            "Total Thirty Min Req MW",
        ]

        df = df.with_columns(
            [pl.col(col).cast(pl.Float64, strict=False) for col in numeric_columns],
        )

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "EIR Designation MW",
                "FER Clearing Price",
                "Forecasted Energy Req MW",
                "Ten Min Spin Req MW",
                "TMNSR Clearing Price",
                "TMNSR Designation MW",
                "TMOR Clearing Price",
                "TMOR Designation MW",
                "TMSR Clearing Price",
                "TMSR Designation MW",
                "Total Ten Min Req MW",
                "Total Thirty Min Req MW",
            ],
        ).sort("Interval Start")

    def _handle_reserve_zone_dataframe(
        self,
        df: pl.DataFrame,
        interval_minutes: int,
        floor_to_five_minutes: bool = False,
    ) -> pl.DataFrame:
        if floor_to_five_minutes:
            interval_start = (
                pl.col("BeginDate")
                .str.to_datetime(time_zone="UTC")
                .dt.truncate("5m")
                .dt.convert_time_zone(self.default_timezone)
            )
        else:
            interval_start = self._parse_offset_datetime_expr("BeginDate")

        df = df.with_columns(interval_start.alias("Interval Start"))
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(minutes=interval_minutes)).alias(
                "Interval End",
            ),
        )
        df = df.rename(ISONE_RESERVE_ZONE_COLUMN_MAP)
        df = df.with_columns(
            [pl.col(col).cast(pl.Float64) for col in ISONE_RESERVE_ZONE_FLOAT_COLUMNS],
        )
        return df.select(ISONE_RESERVE_ZONE_ALL_COLUMNS).sort(
            ["Interval Start", "Reserve Zone Id"],
        )

    @support_date_range("DAY_START")
    def get_reserve_zone_prices_designations_real_time_5_min(
        self,
        date: str | pd.Timestamp | Literal["latest"],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get five-minute real-time reserve prices, requirements, and designations by zone.

        Args:
            date (pd.Timestamp | Literal["latest"]): The start date for the data request. Use "latest" for most recent data.
            end (pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: A DataFrame containing five-minute reserve zone prices and designations.
        """
        url = self._build_url("fiveminutereserveprice", date)
        response = self.make_api_call(url, verbose=verbose)

        df = pl.DataFrame(
            self._safe_get(response, "FiveMinReservePrices", "FiveMinReservePrice"),
        )

        if df.is_empty():
            raise NoDataFoundException(
                f"No five-minute reserve zone price data found for {date}",
            )

        # Floor to 5-minute intervals to handle API data inconsistencies (sometimes returns :01, :02, etc instead of :00). Do rounding in UTC.
        return self._handle_reserve_zone_dataframe(
            df,
            interval_minutes=5,
            floor_to_five_minutes=True,
        )

    @support_date_range("DAY_START")
    def get_reserve_zone_prices_designations_real_time_hourly_prelim(
        self,
        date: str | pd.Timestamp | Literal["latest"],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get preliminary hourly reserve prices, requirements, and designations by zone.

        Args:
            date (pd.Timestamp | Literal["latest"]): The start date for the data request. Use "latest" for most recent data.
            end (pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: A DataFrame containing preliminary hourly reserve zone prices and designations.
        """
        url = self._build_url("hourlyprelimreserveprice", date)
        response = self.make_api_call(url, verbose=verbose)

        df = pl.DataFrame(
            self._safe_get(
                response,
                "PrelimHourlyReservePrices",
                "PrelimHourlyReservePrice",
            ),
        )

        if df.is_empty():
            raise NoDataFoundException(
                f"No preliminary hourly reserve zone price data found for {date}",
            )

        return self._handle_reserve_zone_dataframe(df, interval_minutes=60)

    @support_date_range("DAY_START")
    def get_reserve_zone_prices_designations_real_time_hourly_final(
        self,
        date: str | pd.Timestamp | Literal["latest"],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get final hourly reserve prices, requirements, and designations by zone.

        Args:
            date (pd.Timestamp | Literal["latest"]): The start date for the data request. Use "latest" for most recent data.
            end (pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: A DataFrame containing final hourly reserve zone prices and designations.
        """
        url = self._build_url("hourlyfinalreserveprice", date)
        response = self.make_api_call(url, verbose=verbose)

        df = pl.DataFrame(
            self._safe_get(
                response,
                "FinalHourlyReservePrices",
                "FinalHourlyReservePrice",
            ),
        )

        if df.is_empty():
            raise NoDataFoundException(
                f"No final hourly reserve zone price data found for {date}",
            )

        return self._handle_reserve_zone_dataframe(df, interval_minutes=60)

    @support_date_range("DAY_START")
    def get_ancillary_services_strike_prices_day_ahead(
        self,
        date: str | pd.Timestamp | Literal["latest"],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get day-ahead strike prices and close-out components for ISO-NE.

        Args:
            date (pd.Timestamp | Literal["latest"]): The start date for the data request. Use "latest" for most recent data.
            end (pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: A DataFrame containing day-ahead strike prices and related data.
        """
        url = self._build_url("daasstrikeprices", date)
        response = self.make_api_call(url, verbose=verbose)

        df = pl.from_pandas(
            pd.json_normalize(
                self._safe_get(
                    response,
                    "isone_web_services",
                    "day_ahead_strike_prices",
                    "day_ahead_strike_price",
                ),
            ),
        )

        if df.is_empty():
            raise NoDataFoundException(
                f"No day-ahead strike price data found for {date}",
            )

        # Parse market hour information - API returns timezone-aware datetimes
        df = df.with_columns(
            pl.col("market_hour.local_day")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Interval Start"),
            pl.col("strike_price_timestamp")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Publish Time"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(hours=1)).alias("Interval End"),
        )

        df = df.rename(
            {
                "expected_closeout_charge": "Expected Closeout Charge",
                "expected_closeout_charge_override": "Expected Closeout Charge Override",
                "expected_rt_hub_lmp": "Expected RT Hub LMP",
                "percentile_10_rt_hub_lmp": "Percentile 10 RT Hub LMP",
                "percentile_25_rt_hub_lmp": "Percentile 25 RT Hub LMP",
                "percentile_75_rt_hub_lmp": "Percentile 75 RT Hub LMP",
                "percentile_90_rt_hub_lmp": "Percentile 90 RT Hub LMP",
                "spc_load_forecast_mw": "SPC Load Forecast MW",
                "strike_price": "Strike Price",
            },
        )

        numeric_columns = [
            "Expected Closeout Charge",
            "Expected Closeout Charge Override",
            "Expected RT Hub LMP",
            "Percentile 10 RT Hub LMP",
            "Percentile 25 RT Hub LMP",
            "Percentile 75 RT Hub LMP",
            "Percentile 90 RT Hub LMP",
            "SPC Load Forecast MW",
            "Strike Price",
        ]

        df = df.with_columns(
            [pl.col(col).cast(pl.Float64, strict=False) for col in numeric_columns],
        )

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Expected Closeout Charge",
                "Expected Closeout Charge Override",
                "Expected RT Hub LMP",
                "Percentile 10 RT Hub LMP",
                "Percentile 25 RT Hub LMP",
                "Percentile 75 RT Hub LMP",
                "Percentile 90 RT Hub LMP",
                "SPC Load Forecast MW",
                "Strike Price",
            ],
        ).sort(["Interval Start", "Publish Time"])

    @support_date_range("DAY_START")
    def get_binding_constraints_day_ahead_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        url = self._build_url("dayaheadconstraints", date)
        response = self.make_api_call(url, verbose=verbose)
        records = self._prepare_records(
            self._safe_get(response, "DayAheadConstraints", "DayAheadConstraint"),
        )

        if not records:
            raise NoDataFoundException(
                f"No day-ahead constraint data found for {date}.",
            )

        df = pl.DataFrame(records)

        return self._parse_constraint_dataframe(
            df,
            interval_field="BeginDate",
            interval_minutes=60,
            rename_map={
                "ConstraintName": "Constraint Name",
                "ContingencyName": "Contingency Name",
                "InterfaceFlag": "Interface Flag",
                "MarginalValue": "Marginal Value",
            },
            columns=ISONE_CONSTRAINT_DAY_AHEAD_COLUMNS,
        )

    @support_date_range("DAY_START")
    def get_binding_constraints_preliminary_real_time_15_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        return self._get_constraints_fifteen_min(
            date,
            constraint_type="prelim",
            verbose=verbose,
        )

    @support_date_range("DAY_START")
    def get_binding_constraints_final_real_time_15_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        return self._get_constraints_fifteen_min(
            date,
            constraint_type="final",
            verbose=verbose,
        )

    def _get_constraints_fifteen_min(
        self,
        date: str | pd.Timestamp,
        constraint_type: Literal["prelim", "final"],
        verbose: bool,
    ) -> pl.DataFrame:
        dataset = f"fifteenminuteconstraints/{constraint_type}"
        url = self._build_url(dataset, date)
        response = self.make_api_call(url, verbose=verbose)
        records = self._prepare_records(
            self._safe_get(response, "FifteenMinBcs", "FifteenMinBc"),
        )

        if not records:
            raise NoDataFoundException(
                f"No fifteen-minute {constraint_type} constraint data found for {date}.",
            )

        df = pl.DataFrame(records)

        return self._parse_constraint_dataframe(
            df,
            interval_field="BeginDate",
            interval_minutes=15,
            rename_map={
                "ConstraintName": "Constraint Name",
                "MarginalValue": "Marginal Value",
                "HourEnd": "Hour Ending",
            },
            columns=ISONE_CONSTRAINT_FIFTEEN_MIN_COLUMNS,
        )

    @support_date_range("DAY_START")
    def get_binding_constraints_preliminary_real_time_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        return self._get_constraints_five_min(
            date,
            constraint_type="prelim",
            verbose=verbose,
        )

    @support_date_range("DAY_START")
    def get_binding_constraints_final_real_time_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        return self._get_constraints_five_min(
            date,
            constraint_type="final",
            verbose=verbose,
        )

    def _get_constraints_five_min(
        self,
        date: str | pd.Timestamp,
        constraint_type: Literal["prelim", "final"],
        verbose: bool,
    ) -> pl.DataFrame:
        dataset = f"fiveminuteconstraints/{constraint_type}"
        url = self._build_url(dataset, date)
        response = self.make_api_call(url, verbose=verbose)
        records = self._prepare_records(
            self._safe_get(response, "RealTimeConstraints", "RealTimeConstraint"),
        )

        if not records:
            raise NoDataFoundException(
                f"No five-minute {constraint_type} constraint data found for {date}.",
            )

        df = pl.DataFrame(records)

        rename_map = {
            "ConstraintName": "Constraint Name",
            "MarginalValue": "Marginal Value",
        }

        columns = ISONE_CONSTRAINT_FIVE_MIN_COLUMNS

        return self._parse_constraint_dataframe(
            df,
            interval_field="BeginDate",
            interval_minutes=5,
            rename_map=rename_map,
            columns=columns,
        )

    def _parse_constraint_dataframe(
        self,
        df: pl.DataFrame,
        interval_field: str,
        interval_minutes: int,
        rename_map: dict[str, str],
        columns: list[str],
    ) -> pl.DataFrame:
        if df.is_empty():
            raise NoDataFoundException(
                "No constraint data found for the requested parameters.",
            )

        df = df.with_columns(
            pl.col(interval_field)
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(minutes=interval_minutes)).alias(
                "Interval End",
            ),
        )
        df = df.rename(rename_map)
        df = df.with_columns(
            pl.col("Marginal Value").cast(pl.Float64, strict=False),
        )

        if "Contingency Name" in df.columns:
            df = df.with_columns(
                pl.when(
                    pl.col("Contingency Name").is_null()
                    | (pl.col("Contingency Name").cast(pl.Utf8).str.strip_chars() == "")
                    | (
                        pl.col("Contingency Name")
                        .cast(pl.Utf8)
                        .str.strip_chars()
                        .str.to_uppercase()
                        == "NULL"
                    ),
                )
                .then(None)
                .otherwise(
                    pl.col("Contingency Name").cast(pl.Utf8).str.strip_chars(),
                )
                .alias("Contingency Name"),
            )

        for col in columns:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))

        return df.select(columns).sort("Interval Start")

    @support_date_range(frequency="MONTH_START")
    def get_fcm_reconfiguration_monthly(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get FCM Monthly Reconfiguration Auction data.

        Args:
            date (str | pd.Timestamp): The start date for the data request. Use "latest" for most recent data.
            end (str | pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: A DataFrame containing monthly reconfiguration auction data.
        """
        if date == "latest":
            endpoint = f"{self.base_url}/fcmmra/current"
        else:
            start = date[0] if isinstance(date, tuple) else pd.Timestamp(date)
            cp_label = self._get_fcm_commitment_period_label(start)
            endpoint = (
                f"{self.base_url}/fcmmra/cp/{cp_label}/month/{date.strftime('%Y%m')}"
            )

        response = self.make_api_call(endpoint, verbose=verbose)
        auctions = self._prepare_records(
            self._safe_get(response, "FCMRAResults", "FCMRAResult"),
        )

        annotated_auctions: list[tuple[dict, pd.Timestamp, pd.Timestamp]] = []
        for wrapper in auctions:
            auction = wrapper.get("Auction", wrapper)
            description = auction.get("Description")
            interval_start = (
                pd.Timestamp(description)
                if description
                else pd.Timestamp(auction.get("ApprovalDate"))
            )
            if interval_start.tz is None:
                interval_start = interval_start.tz_localize(self.default_timezone)
            interval_end = interval_start + pd.DateOffset(months=1)
            annotated_auctions.append((auction, interval_start, interval_end))

        return self._parse_fcm_reconfiguration_dataframe(
            annotated_auctions,
            auction_type="monthly",
        )

    @support_date_range(frequency="YEAR_START")
    def get_fcm_reconfiguration_annual(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Get FCM Annual Reconfiguration Auction data for all three auctions (ARA1, ARA2, ARA3).

        Queries the API for all three annual reconfiguration auctions using the endpoint
        /fcmara/cp/{cp}/ara/{ARA} where {ARA} can be ARA1, ARA2, or ARA3.

        Args:
            date (str | pd.Timestamp): The start date for the data request. Use "latest" for most recent data.
            end (str | pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pl.DataFrame: A DataFrame containing annual reconfiguration auction data with an "ARA"
                          column with values 1, 2, or 3 distinguishing between ARA1, ARA2, and ARA3.
                          Note that not all three auctions may exist for all commitment periods
                          (e.g., ARA3 may not exist yet for recent periods).
        """
        if date == "latest":
            endpoint = f"{self.base_url}/fcmara/current"
            response = self.make_api_call(endpoint, verbose=verbose)
            auctions = self._prepare_records(
                self._safe_get(response, "FCMRAResults", "FCMRAResult"),
            )
            target_cp = None
            if auctions:
                auction = auctions[0].get("Auction", auctions[0])
                period = auction.get("CommitmentPeriod", {})
                if isinstance(period, dict) and period.get("Description"):
                    target_cp = period["Description"]
        else:
            start = date[0] if isinstance(date, tuple) else pd.Timestamp(date)
            target_cp = self._get_fcm_commitment_period_label(start)

        annotated_auctions: list[tuple[dict, pd.Timestamp, pd.Timestamp]] = []

        if target_cp:
            cp_start_year = int(target_cp.split("-")[0])
            use_historical_endpoint = cp_start_year <= 2020

            if use_historical_endpoint:
                endpoint = f"{self.base_url}/fcmara/cp/{target_cp}"
                response = self.make_api_call(endpoint, verbose=verbose)
                auctions = self._prepare_records(
                    self._safe_get(response, "FCMRAResults", "FCMRAResult"),
                )

                for wrapper in auctions:
                    auction = wrapper.get("Auction", wrapper)
                    period = auction.get("CommitmentPeriod", {})
                    begin = period.get("BeginDate")
                    end_date = period.get("EndDate")

                    if begin and end_date:
                        interval_start = pd.Timestamp(begin)
                        interval_end = pd.Timestamp(end_date)
                    else:
                        description = auction.get("Description")
                        interval_start = (
                            pd.Timestamp(description)
                            if description
                            else pd.Timestamp(auction.get("ApprovalDate"))
                        )
                        interval_end = interval_start + pd.DateOffset(years=1)

                    annotated_auctions.append(
                        (auction, interval_start, interval_end),
                    )
            else:
                for ara_type in ["ARA1", "ARA2", "ARA3"]:
                    endpoint = f"{self.base_url}/fcmara/cp/{target_cp}/ara/{ara_type}"

                    try:
                        response = self.make_api_call(endpoint, verbose=verbose)
                        auctions = self._prepare_records(
                            self._safe_get(response, "FCMRAResults", "FCMRAResult"),
                        )

                        for wrapper in auctions:
                            auction = wrapper.get("Auction", wrapper)
                            period = auction.get("CommitmentPeriod", {})
                            begin = period.get("BeginDate")
                            end_date = period.get("EndDate")

                            if begin and end_date:
                                interval_start = pd.Timestamp(begin)
                                interval_end = pd.Timestamp(end_date)
                            else:
                                description = auction.get("Description")
                                interval_start = (
                                    pd.Timestamp(description)
                                    if description
                                    else pd.Timestamp(auction.get("ApprovalDate"))
                                )
                                interval_end = interval_start + pd.DateOffset(years=1)

                            annotated_auctions.append(
                                (auction, interval_start, interval_end),
                            )
                    except Exception as e:
                        log.debug(
                            f"Could not fetch {ara_type} for CP {target_cp}: {e}",
                        )

        else:
            for wrapper in auctions:
                auction = wrapper.get("Auction", wrapper)
                period = auction.get("CommitmentPeriod", {})
                begin = period.get("BeginDate")
                end_date = period.get("EndDate")

                if begin and end_date:
                    interval_start = pd.Timestamp(begin)
                    interval_end = pd.Timestamp(end_date)
                else:
                    description = auction.get("Description")
                    interval_start = (
                        pd.Timestamp(description)
                        if description
                        else pd.Timestamp(auction.get("ApprovalDate"))
                    )
                    interval_end = interval_start + pd.DateOffset(years=1)

                annotated_auctions.append((auction, interval_start, interval_end))

        return self._parse_fcm_reconfiguration_dataframe(
            annotated_auctions,
            auction_type="annual",
        )

    def _get_fcm_commitment_period_label(self, timestamp: pd.Timestamp) -> str:
        start_year = timestamp.year if timestamp.month >= 6 else timestamp.year - 1
        end_year_suffix = (start_year + 1) % 100
        return f"{start_year}-{end_year_suffix:02d}"

    def _parse_fcm_reconfiguration_dataframe(
        self,
        auction_records: list[tuple[dict, pd.Timestamp, pd.Timestamp]],
        auction_type: str = "monthly",
    ) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []

        zone_column_map = {
            "CapacityZoneID": "Location ID",
            "CapacityZoneName": "Location Name",
            "CapacityZoneType": "Capacity Zone Type",
            "SupplySubmitted": "Total Supply Offers Submitted",
            "DemandSubmitted": "Total Demand Bids Submitted",
            "SupplyCleared": "Total Supply Offers Cleared",
            "DemandCleared": "Total Demand Bids Cleared",
            "NetCapacityCleared": "Net Capacity Cleared",
            "ClearingPrice": "Clearing Price",
        }

        interface_column_map = {
            "ExternalInterfaceId": "Location ID",
            "ExternalInterfaceName": "Location Name",
            "SupplySubmitted": "Total Supply Offers Submitted",
            "DemandSubmitted": "Total Demand Bids Submitted",
            "SupplyCleared": "Total Supply Offers Cleared",
            "DemandCleared": "Total Demand Bids Cleared",
            "NetCapacityCleared": "Net Capacity Cleared",
            "ClearingPrice": "Clearing Price",
        }

        for auction, interval_start, interval_end in auction_records:
            zone_frame = pl.from_pandas(
                pd.json_normalize(
                    auction,
                    record_path=["ClearedCapacityZones", "ClearedCapacityZone"],
                    errors="ignore",
                ),
            )

            zone_frame = zone_frame.rename(zone_column_map)
            zone_frame = zone_frame.with_columns(
                pl.lit(interval_start).alias("Interval Start"),
                pl.lit(interval_end).alias("Interval End"),
                pl.lit("Capacity Zone").alias("Location Type"),
            )
            if auction_type == "annual":
                auction_type_str = auction.get("Type", "")
                ara_value = None
                if auction_type_str.startswith("ARA"):
                    try:
                        ara_value = int(auction_type_str.replace("ARA", ""))
                    except ValueError:
                        pass
                zone_frame = zone_frame.with_columns(pl.lit(ara_value).alias("ARA"))
            frames.append(zone_frame)

            interface_frame = pl.from_pandas(
                pd.json_normalize(
                    auction,
                    record_path=[
                        "ClearedCapacityZones",
                        "ClearedCapacityZone",
                        "ClearedExternalInterfaces",
                        "ClearedExternalInterface",
                    ],
                    errors="ignore",
                ),
            )

            interface_frame = interface_frame.rename(interface_column_map)
            interface_frame = interface_frame.with_columns(
                pl.lit(interval_start).alias("Interval Start"),
                pl.lit(interval_end).alias("Interval End"),
                pl.lit("External Interface").alias("Location Type"),
                pl.lit(None).alias("Capacity Zone Type"),
            )
            if auction_type == "annual":
                auction_type_str = auction.get("Type", "")
                ara_value = None
                if auction_type_str.startswith("ARA"):
                    try:
                        ara_value = int(auction_type_str.replace("ARA", ""))
                    except ValueError:
                        pass
                interface_frame = interface_frame.with_columns(
                    pl.lit(ara_value).alias("ARA"),
                )
            frames.append(interface_frame)

        df = pl.concat(frames, how="diagonal")

        numeric_columns = [
            "Total Supply Offers Submitted",
            "Total Demand Bids Submitted",
            "Total Supply Offers Cleared",
            "Total Demand Bids Cleared",
            "Net Capacity Cleared",
            "Clearing Price",
        ]

        cast_exprs = [
            pl.col(column).cast(pl.Float64, strict=False)
            for column in numeric_columns
            if column in df.columns
        ]
        if cast_exprs:
            df = df.with_columns(cast_exprs)

        if auction_type == "annual":
            df = df.with_columns(pl.col("ARA").cast(pl.Int64))

        columns_to_use = (
            ISONE_FCM_RECONFIGURATION_COLUMNS
            if auction_type == "annual"
            else [col for col in ISONE_FCM_RECONFIGURATION_COLUMNS if col != "ARA"]
        )
        for col in columns_to_use:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        df = df.select(columns_to_use)

        sort_columns = ["Interval Start", "Location ID"]
        if auction_type == "annual":
            sort_columns.insert(1, "ARA")
        df = df.sort(sort_columns)

        log.debug(
            f"Processed FCM reconfiguration auction data. "
            f"{df.height} entries from {df['Interval Start'].min()} to {df['Interval Start'].max()}",
        )

        return df
