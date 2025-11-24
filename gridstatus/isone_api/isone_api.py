import os
import time
from datetime import datetime
from typing import Literal

import pandas as pd
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
    ISONE_CONSTRAINT_FIVE_MIN_FINAL_COLUMNS,
    ISONE_CONSTRAINT_FIVE_MIN_PRELIM_COLUMNS,
    ISONE_FCM_RECONFIGURATION_COLUMNS,
    ISONE_RESERVE_ZONE_ALL_COLUMNS,
    ISONE_RESERVE_ZONE_COLUMN_MAP,
    ISONE_RESERVE_ZONE_FLOAT_COLUMNS,
)

# Default page size for API requests
DEFAULT_PAGE_SIZE = 1000


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

    def get_locations(self) -> pd.DataFrame:
        """
        Get a list of core hub and zone locations.

        Returns:
            pandas.DataFrame: A DataFrame containing location information.
        """
        url = f"{self.base_url}/locations"
        response = self.make_api_call(url)
        if "Locations" not in response or "Location" not in response["Locations"]:
            raise NoDataFoundException("No location data found.")

        locations = response["Locations"]["Location"]
        df = pd.DataFrame(locations)

        return df

    def get_location_by_id(self, location_id: int) -> pd.DataFrame:
        """
        Get information for a specific location by its ID.

        Args:
            location_id (int): The ID of the location to retrieve.

        Returns:
            pandas.DataFrame: A DataFrame containing the location information.
        """
        url = f"{self.base_url}/locations/{location_id}"
        response = self.make_api_call(url)

        if "Location" not in response:
            raise NoDataFoundException(f"No data found for location ID: {location_id}")

        location = response["Location"]
        df = pd.DataFrame([location])
        return df

    def get_locations_all(self) -> pd.DataFrame:
        """
        Get detailed information for all locations.

        Returns:
            pandas.DataFrame: A DataFrame containing detailed information for all locations.
        """
        url = f"{self.base_url}/locations/all"
        response = self.make_api_call(url)

        if "Locations" not in response or "Location" not in response["Locations"]:
            raise NoDataFoundException("No location data found.")

        locations = response["Locations"]["Location"]
        df = pd.DataFrame(locations)
        return df

    @support_date_range("DAY_START")
    def get_fuel_mix(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Return fuel mix data for the specified date range

        Args:
            date (str | pd.Timestamp): The start date for the data request. Use "latest" for most recent data.
            end (str | pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pd.DataFrame: DataFrame containing fuel mix data with timestamps and generation by fuel type
        """
        if date == "latest":
            url = f"{self.base_url}/genfuelmix/current"
        else:
            url = f"{self.base_url}/genfuelmix/day/{date.strftime('%Y%m%d')}"

        response = self.make_api_call(url)
        df = pd.DataFrame(response["GenFuelMixes"]["GenFuelMix"])

        mix_df = df.pivot_table(
            index="BeginDate",
            columns="FuelCategory",
            values="GenMw",
            aggfunc="first",
        ).reset_index()
        mix_df.columns.name = None

        mix_df = mix_df.rename(columns={"BeginDate": "Time"})
        mix_df["Time"] = mix_df["Time"].apply(self.parse_problematic_datetime)
        mix_df = mix_df.fillna(0)
        mix_df = utils.move_cols_to_front(mix_df, ["Time"])

        return mix_df

    @support_date_range("DAY_START")
    def get_realtime_hourly_demand(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        locations: list[str] = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get the real-time hourly demand data for specified locations and date range.

        Args:
            date (str): The start date for the data request. Use "latest" for most recent data.
            end_date (str | None): The end date for the data request. Only used if date is not "latest".
            locations (list[str], optional): List of specific location names to request data for.
                                             If None, data for all locations will be retrieved.

        Returns:
            pandas.DataFrame: A DataFrame containing the real-time hourly demand data for all requested locations.
        """
        all_data = []

        match (date, locations):
            case ("latest", None):
                url = f"{self.base_url}/realtimehourlydemand/current"
                response = self.make_api_call(url)
                df = pd.DataFrame(response["HourlyRtDemands"]["HourlyRtDemand"])
                df["LocId"] = df["Location"].apply(lambda x: x["@LocId"])
                df["Location"] = df["Location"].apply(lambda x: x["$"])
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

        df = pd.DataFrame(all_data)
        return self._handle_demand(df, interval_minutes=60)

    @support_date_range("DAY_START")
    def get_load_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Return hourly load data for a given date or date range

        Args:
            date (str | pd.Timestamp): The start date for the data request. Use "latest" for most recent data.
            end (str | pd.Timestamp, optional): The end date for the data request. Only used if date is not "latest".
            locations (list[str], optional): List of specific location names to request data for.
                                             If None, data for all locations will be retrieved.
            verbose (bool, optional): Whether to print verbose logging information.


        Returns:
            pd.DataFrame: DataFrame containing load data with timestamps and load
        """

        if date == "latest":
            url = f"{self.base_url}/hourlysysload/current"
            response = self.make_api_call(url)
            raw_data = [response["HourlySystemLoads"]["HourlySystemLoad"]]
        else:
            url = f"{self.base_url}/hourlysysload/day/{date.strftime('%Y%m%d')}"
            response = self.make_api_call(url)
            raw_data = response["HourlySystemLoads"]["HourlySystemLoad"]

        df = pd.json_normalize(
            raw_data,
            meta=["BeginDate", "Load", "NativeLoad", "ArdDemand"],
        )
        df["Location"] = df["Location.$"]
        df["LocId"] = df["Location.@LocId"]
        df = df.drop(columns=["Location.$", "Location.@LocId"])
        df.rename(
            columns={"NativeLoad": "Native Load", "ArdDemand": "ARD Demand"},
            inplace=True,
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
    ) -> pd.DataFrame:
        """
        Get the day-ahead hourly demand data for specified locations and date range.

        Args:
            date (str): The start date for the data request. Use "latest" for most recent data.
            end_date (str | None): The end date for the data request. Only used if date is not "latest".
            locations (list[str], optional): List of specific location names to request data for.
                                             If None, data for all locations will be retrieved.

        Returns:
            pandas.DataFrame: A DataFrame containing the day-ahead hourly demand data for all requested locations.
        """
        all_data = []

        match (date, locations):
            case ("latest", None):
                url = f"{self.base_url}/dayaheadhourlydemand/current"
                response = self.make_api_call(url)
                df = pd.DataFrame(response["HourlyDaDemands"]["HourlyDaDemand"])
                df["LocId"] = df["Location"].apply(lambda x: x["@LocId"])
                df["Location"] = df["Location"].apply(lambda x: x["$"])
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

        df = pd.DataFrame(all_data)
        # NOTE(kladar): 2017-07-01 to 2018-06-01 causes an issue
        # as there are duplicates of the .H.INTERNALHUB location. Deduping them here
        df = df.drop_duplicates(subset=["BeginDate", "Location"], keep="first")

        return self._handle_demand(df, interval_minutes=60)

    def _handle_demand(
        self,
        df: pd.DataFrame,
        interval_minutes: int = 60,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Process demand DataFrame: convert types, rename columns, and add Interval End.

        Args:
            df (pd.DataFrame): Input DataFrame with demand data.
            interval_minutes (int): Duration of each interval in minutes. Default is 60.

        Returns:
            pd.DataFrame: Processed DataFrame.
        """
        if columns is None:
            columns = [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Load",
            ]

        try:
            # Try the standard pandas datetime conversion first
            df["Interval Start"] = pd.to_datetime(df["BeginDate"])
            df["Interval Start"] = df["Interval Start"].dt.tz_convert(
                self.default_timezone,
            )
        except AttributeError:
            # NOTE(kladar) consistently the above logic fails for DST conversion days.
            # This catches those and fixes it.
            # Data coming out looks good, no missed intervals and no duplicates.
            log.warning("Standard datetime conversion failed. Using custom parsing.")
            df["Interval Start"] = df["BeginDate"].apply(
                self.parse_problematic_datetime,
            )

        df = df.sort_values(["Interval Start", "Location"])
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(
            minutes=interval_minutes,
        )

        df["Load"] = pd.to_numeric(df["Load"], errors="coerce")
        df["Location Id"] = pd.to_numeric(df["LocId"], errors="coerce")

        log.info(
            f"Processed demand data: {len(df)} entries from {df['Interval Start'].min()} to {df['Interval Start'].max()}",
        )
        return df[columns]

    @support_date_range("DAY_START")
    def get_load_forecast_hourly(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        vintage: Literal["latest", "all"] = "all",
        verbose: bool = False,
    ) -> pd.DataFrame:
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
            pandas.DataFrame: A DataFrame containing the hourly load forecast data for the system.
        """

        if date == "latest":
            url = f"{self.base_url}/hourlyloadforecast/current"
            response = self.make_api_call(url)
            df = pd.DataFrame(response["HourlyLoadForecast"])
            return self._handle_load_forecast(df, interval_minutes=60)

        elif vintage == "all":
            url = (
                f"{self.base_url}/hourlyloadforecast/all/day/{date.strftime('%Y%m%d')}"
            )
        else:
            url = f"{self.base_url}/hourlyloadforecast/day/{date.strftime('%Y%m%d')}"

        response = self.make_api_call(url)
        df = pd.DataFrame(response["HourlyLoadForecasts"]["HourlyLoadForecast"])
        return self._handle_load_forecast(df, interval_minutes=60)

    @support_date_range("DAY_START")
    def get_reliability_region_load_forecast(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        vintage: Literal["latest", "all"] = "all",
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get the regional load forecast data for specified date range and vintages.

        Args:
            date (str): The start date for the data request. Use "latest" for most recent data.
            end (str | None): The end date for the data request. Only used if date is not "latest".
            vintages (Literal["latest", "all"]): The vintage for the data request. Options are "latest" or "all".

        Returns:
            pandas.DataFrame: A DataFrame containing the regional load forecast data for all requested locations.
        """

        if date == "latest":
            url = f"{self.base_url}/reliabilityregionloadforecast/current"
        elif vintage == "all":
            url = f"{self.base_url}/reliabilityregionloadforecast/day/{date.strftime('%Y%m%d')}/all"
        else:
            url = f"{self.base_url}/reliabilityregionloadforecast/day/{date.strftime('%Y%m%d')}"

        response = self.make_api_call(url)
        df = pd.DataFrame(
            response["ReliabilityRegionLoadForecasts"]["ReliabilityRegionLoadForecast"],
        )
        return self._handle_load_forecast(df, interval_minutes=60)

    def _handle_load_forecast(
        self,
        df: pd.DataFrame,
        interval_minutes: int = 60,
    ) -> pd.DataFrame:
        """
        Process load forecast DataFrame: convert types, rename columns, and add Interval End.

        Args:
            df (pd.DataFrame): Input DataFrame with load forecast data.
            interval_minutes (int): Duration of each interval in minutes. Default is 60.

        Returns:
            pd.DataFrame: Processed DataFrame.
        """
        try:
            # Try the standard pandas datetime conversion first

            date_columns = ["BeginDate", "CreationDate"]
            df[date_columns] = df[date_columns].apply(pd.to_datetime)
            df[date_columns] = df[date_columns].apply(
                lambda x: x.dt.tz_convert(self.default_timezone),
            )

        except AttributeError:
            # NOTE(kladar) consistently the above logic fails for DST conversion days.
            # This catches those and fixes it.
            # Data coming out looks good, no missed intervals and no duplicates.
            log.warning("Standard datetime conversion failed. Using custom parsing.")
            df["BeginDate"] = df["BeginDate"].apply(self.parse_problematic_datetime)
            df["CreationDate"] = df["CreationDate"].apply(
                self.parse_problematic_datetime,
            )

        df["Interval End"] = df["BeginDate"] + pd.Timedelta(
            minutes=interval_minutes,
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
            df = df.rename(columns=regional_cols)
            df["Regional Percentage"] = pd.to_numeric(
                df["Regional Percentage"],
                errors="coerce",
            )
        else:
            df = df.rename(columns=system_cols)
            df["Net Load Forecast"] = pd.to_numeric(
                df["Net Load Forecast"],
                errors="coerce",
            )

        df = df.sort_values(["Interval Start", "Publish Time"])
        df["Load Forecast"] = pd.to_numeric(df["Load Forecast"], errors="coerce")

        log.info(
            f"Processed load forecast data: {len(df)} entries from {df['Interval Start'].min()} to {df['Interval Start'].max()}",
        )
        return df[
            list(
                (
                    regional_cols.values()
                    if "Location" in df.columns
                    else system_cols.values()
                ),
            )
        ]

    @support_date_range("DAY_START")
    def get_interchange_hourly(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get the hourly interchange data for specified date range. Hourly data includes
        multiple locations.

        Args:
            date (str): The start date for the data request. Use "latest" for most
            recent data.
            end_date (str | None): The end date for the data request. Only used if date
            is not "latest".

        Returns:
            pandas.DataFrame: A DataFrame containing the interchange fifteen minute
            data for all requested locations.
        """
        if date == "latest":
            url = f"{self.base_url}/actualinterchange/current"
        else:
            url = f"{self.base_url}/actualinterchange/day/{date.strftime('%Y%m%d')}"

        log.info(f"Requesting interchange data for date: {date}")

        response = self.make_api_call(url)

        if data := response.get("ActualInterchanges"):
            df = pd.DataFrame(
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
    ) -> pd.DataFrame:
        """
        Get the fifteen minute interchange data for specified date range.

        Args:
            date (str): The start date for the data request. Use "latest" for most
            recent data.
            end_date (str | None): The end date for the data request. Only used if date
            is not "latest".

        Returns:
            pandas.DataFrame: A DataFrame containing the interchange fifteen minute
            data for all requested locations.
        """
        if date == "latest":
            url = f"{self.base_url}/fifteenminuteinterchange/current"
        else:
            url = f"{self.base_url}/fifteenminuteinterchange/day/{date.strftime('%Y%m%d')}"  # noqa: E501

        log.info(f"Requesting interchange data for date: {date}")

        response = self.make_api_call(url)

        if data := response.get("ActualFifteenMinInterchanges"):
            df = pd.DataFrame(data["ActualFifteenMinInterchange"])
        else:
            raise NoDataFoundException(
                f"No fifteen minute interchange data found for {date}",
            )

        return self._handle_interchange_dataframe(df, interval_minutes=15)

    def _handle_interchange_dataframe(self, df: pd.DataFrame, interval_minutes: int):
        df["Interval Start"] = pd.to_datetime(df["BeginDate"], utc=True).dt.tz_convert(
            self.default_timezone,
        )
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(
            minutes=interval_minutes,
        )

        # Split location column from {'$': '.I.ROSETON 345 1', '@LocId': '4011'} to
        # Location and Location Id
        df[["Location", "Location Id"]] = pd.json_normalize(df["Location"])

        df = df.rename(columns={"ActInterchange": "Actual Interchange"})

        return df[
            [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Actual Interchange",
                "Purchase",
                "Sale",
            ]
        ].sort_values(["Interval Start", "Location"])

    @support_date_range("DAY_START")
    def get_external_flows_5_min(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get the five minute external flow data for specified date range.

        Args:
            date (str): The start date for the data request. Use "latest" for most
            recent data.
            end_date (str | None): The end date for the data request. Only used if date
            is not "latest".

        Returns:
            pandas.DataFrame: A DataFrame containing the external flow five minute
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
            df = pd.DataFrame(data["ExternalFlow"])
        else:
            raise NoDataFoundException(
                f"No five minute external flow data found for {date}",
            )

        return self._handle_external_flows_dataframe(df, interval_minutes=5)

    def _handle_external_flows_dataframe(self, df: pd.DataFrame, interval_minutes: int):
        df["Interval Start"] = pd.to_datetime(df["BeginDate"], utc=True).dt.tz_convert(
            self.default_timezone,
        )
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(
            minutes=interval_minutes,
        )

        # Split location column from {'$': '.I.ROSETON 345 1', '@LocId': '4011'} to
        # Location and Location Id
        df[["Location", "Location Id"]] = pd.json_normalize(df["Location"])

        df = df.rename(
            columns={
                "ActualFlow": "Actual Flow",
                "ImportLimit": "Import Limit",
                "ExportLimit": "Export Limit",
                "CurrentSchedule": "Current Schedule",
                "TotalExports": "Total Exports",
                "TotalImports": "Total Imports",
            },
        )

        return df[
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
            ]
        ].sort_values(["Interval Start", "Location"])

    @support_date_range("HOUR_START")
    def get_lmp_real_time_5_min_prelim(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get the real-time 5 minute LMP preliminary data for specified date range.

        Args:
            date (str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp]): The start date for the data request. Use "latest" for most recent data.
            end (str | pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pd.DataFrame: A DataFrame containing the real-time 5 minute LMP preliminary data.
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
    ) -> pd.DataFrame:
        """
        Get the real-time 5 minute LMP final data for specified date range.

        Args:
            date (str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp]): The start date for the data request. Use "latest" for most recent data.
            end (str | pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pd.DataFrame: A DataFrame containing the real-time 5 minute LMP final data.
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
    ) -> pd.DataFrame:
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
    ) -> pd.DataFrame:
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
    ) -> pd.DataFrame:
        response = self.make_api_call(url)

        if interval_minutes == 60:
            df = pd.DataFrame(response["HourlyLmps"]["HourlyLmp"])
        else:
            df = pd.DataFrame(response["FiveMinLmps"]["FiveMinLmp"])

        df["Interval Start"] = pd.to_datetime(df["BeginDate"], utc=True).dt.tz_convert(
            self.default_timezone,
        )
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(
            minutes=interval_minutes,
        )

        df["Location Type"] = df["Location"].apply(lambda x: x["@LocType"])
        df["Location"] = df["Location"].apply(lambda x: x["$"])
        df = df.rename(
            columns={
                "LmpTotal": "LMP",
                "EnergyComponent": "Energy",
                "CongestionComponent": "Congestion",
                "LossComponent": "Loss",
            },
        )

        return df[
            [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
        ].sort_values(["Interval Start", "Location"])

    @support_date_range("DAY_START")
    def get_capacity_forecast_7_day(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
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
    ) -> pd.DataFrame:
        response = self.make_api_call(url)
        df = pd.json_normalize(
            response["SevenDayForecasts"]["SevenDayForecast"],
            record_path=["MarketDay"],
            meta=["CreationDate"],
        )

        df["Publish Time"] = pd.to_datetime(df["CreationDate"], utc=True).dt.tz_convert(
            self.default_timezone,
        )
        df["Interval Start"] = pd.to_datetime(df["MarketDate"], utc=True).dt.tz_convert(
            self.default_timezone,
        )
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(days=1)

        df = df.rename(
            columns={
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
            df[col] = None

        df["High Temperature Boston"] = df["Weather.CityWeather"].apply(
            lambda x: next(
                (city["HighTempF"] for city in x if city["CityName"] == "Boston"),
                None,
            ),
        )
        df["High Temperature Hartford"] = df["Weather.CityWeather"].apply(
            lambda x: next(
                (city["HighTempF"] for city in x if city["CityName"] == "Hartford"),
                None,
            ),
        )
        df["Dew Point Boston"] = df["Weather.CityWeather"].apply(
            lambda x: next(
                (city["DewPointF"] for city in x if city["CityName"] == "Boston"),
                None,
            ),
        )
        df["Dew Point Hartford"] = df["Weather.CityWeather"].apply(
            lambda x: next(
                (city["DewPointF"] for city in x if city["CityName"] == "Hartford"),
                None,
            ),
        )

        return df[ISONE_CAPACITY_FORECAST_7_DAY_COLUMNS]

    @support_date_range("DAY_START")
    def get_regulation_clearing_prices_real_time_5_min(
        self,
        date: str | pd.Timestamp | Literal["latest"],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get five-minute clearing prices for both regulation capacity and service in real-time.

        Args:
            date (pd.Timestamp | Literal["latest"]): The start date for the data request. Use "latest" for most recent data.
            end (pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pd.DataFrame: A DataFrame containing five-minute regulation clearing prices.
        """
        url = self._build_url("fiveminutercp", date)
        response = self.make_api_call(url, verbose=verbose)
        df = pd.DataFrame(self._safe_get(response, "FiveMinRcps", "FiveMinRcp"))

        if df.empty:
            raise NoDataFoundException(
                f"No five-minute regulation clearing price data found for {date}",
            )

        # Timestamps already have an offset, so we parse as UTC then convert to local
        df["Interval Start"] = pd.to_datetime(df["BeginDate"], utc=True)
        # Floor to 5-minute intervals to handle API data inconsistencies (sometimes returns :01, :02, etc instead of :00). Round in UTC to avoid DST issues
        df["Interval Start"] = (
            df["Interval Start"]
            .dt.floor("5min")
            .dt.tz_convert(
                self.default_timezone,
            )
        )
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(minutes=5)

        df = df.rename(
            columns={
                "RegServiceClearingPrice": "Reg Service Clearing Price",
                "RegCapacityClearingPrice": "Reg Capacity Clearing Price",
            },
        )

        df["Reg Service Clearing Price"] = df["Reg Service Clearing Price"].astype(
            float,
        )

        df["Reg Capacity Clearing Price"] = df["Reg Capacity Clearing Price"].astype(
            float,
        )

        return df[
            [
                "Interval Start",
                "Interval End",
                "Reg Service Clearing Price",
                "Reg Capacity Clearing Price",
            ]
        ].sort_values("Interval Start")

    @support_date_range("DAY_START")
    def get_reserve_requirements_prices_forecast_day_ahead(
        self,
        date: str | pd.Timestamp | Literal["latest"],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get day-ahead reserve prices, requirements, and forecast for reserve zone 7000 (system-wide).

        Args:
            date (pd.Timestamp | Literal["latest"]): The start date for the data request. Use "latest" for most recent data.
            end (pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pd.DataFrame: A DataFrame containing day-ahead reserve requirements, prices, and forecast.
        """
        url = self._build_url("daasreservedata", date)
        response = self.make_api_call(url, verbose=verbose)

        df = pd.json_normalize(
            self._safe_get(
                response,
                "isone_web_services",
                "day_ahead_reserves",
                "day_ahead_reserve",
            ),
        )

        if df.empty:
            raise NoDataFoundException(f"No day-ahead reserve data found for {date}")

        # Parse market hour information - API returns timezone-aware datetimes
        df["Interval Start"] = pd.to_datetime(
            df["market_hour.local_day"],
            utc=True,
        ).dt.tz_convert(
            self.default_timezone,
        )
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)

        df = df.rename(
            columns={
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

        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df[
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
            ]
        ].sort_values("Interval Start")

    @support_date_range("DAY_START")
    def get_reserve_zone_prices_designations_real_time_5_min(
        self,
        date: str | pd.Timestamp | Literal["latest"],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get five-minute real-time reserve prices, requirements, and designations by zone.

        Args:
            date (pd.Timestamp | Literal["latest"]): The start date for the data request. Use "latest" for most recent data.
            end (pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pd.DataFrame: A DataFrame containing five-minute reserve zone prices and designations.
        """
        url = self._build_url("fiveminutereserveprice", date)
        response = self.make_api_call(url, verbose=verbose)

        df = pd.DataFrame(
            self._safe_get(response, "FiveMinReservePrices", "FiveMinReservePrice"),
        )

        if df.empty:
            raise NoDataFoundException(
                f"No five-minute reserve zone price data found for {date}",
            )

        # Floor to 5-minute intervals to handle API data inconsistencies (sometimes returns :01, :02, etc instead of :00). Do rounding in UTC.
        df["Interval Start"] = (
            pd.to_datetime(df["BeginDate"], utc=True)
            .dt.floor("5min")
            .dt.tz_convert(
                self.default_timezone,
            )
        )

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(minutes=5)

        df = df.rename(columns=ISONE_RESERVE_ZONE_COLUMN_MAP)

        for col in ISONE_RESERVE_ZONE_FLOAT_COLUMNS:
            df[col] = df[col].astype(float)

        return df[ISONE_RESERVE_ZONE_ALL_COLUMNS].sort_values(
            ["Interval Start", "Reserve Zone Id"],
        )

    @support_date_range("DAY_START")
    def get_reserve_zone_prices_designations_real_time_hourly_prelim(
        self,
        date: str | pd.Timestamp | Literal["latest"],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get preliminary hourly reserve prices, requirements, and designations by zone.

        Args:
            date (pd.Timestamp | Literal["latest"]): The start date for the data request. Use "latest" for most recent data.
            end (pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pd.DataFrame: A DataFrame containing preliminary hourly reserve zone prices and designations.
        """
        url = self._build_url("hourlyprelimreserveprice", date)
        response = self.make_api_call(url, verbose=verbose)

        df = pd.DataFrame(
            self._safe_get(
                response,
                "PrelimHourlyReservePrices",
                "PrelimHourlyReservePrice",
            ),
        )

        if df.empty:
            raise NoDataFoundException(
                f"No preliminary hourly reserve zone price data found for {date}",
            )

        df["Interval Start"] = pd.to_datetime(df["BeginDate"], utc=True).dt.tz_convert(
            self.default_timezone,
        )
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)

        df = df.rename(columns=ISONE_RESERVE_ZONE_COLUMN_MAP)

        for col in ISONE_RESERVE_ZONE_FLOAT_COLUMNS:
            df[col] = df[col].astype(float)

        return df[ISONE_RESERVE_ZONE_ALL_COLUMNS].sort_values(
            ["Interval Start", "Reserve Zone Id"],
        )

    @support_date_range("DAY_START")
    def get_reserve_zone_prices_designations_real_time_hourly_final(
        self,
        date: str | pd.Timestamp | Literal["latest"],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get final hourly reserve prices, requirements, and designations by zone.

        Args:
            date (pd.Timestamp | Literal["latest"]): The start date for the data request. Use "latest" for most recent data.
            end (pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pd.DataFrame: A DataFrame containing final hourly reserve zone prices and designations.
        """
        url = self._build_url("hourlyfinalreserveprice", date)
        response = self.make_api_call(url, verbose=verbose)

        df = pd.DataFrame(
            self._safe_get(
                response,
                "FinalHourlyReservePrices",
                "FinalHourlyReservePrice",
            ),
        )

        if df.empty:
            raise NoDataFoundException(
                f"No final hourly reserve zone price data found for {date}",
            )

        df["Interval Start"] = pd.to_datetime(df["BeginDate"], utc=True).dt.tz_convert(
            self.default_timezone,
        )
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)

        df = df.rename(columns=ISONE_RESERVE_ZONE_COLUMN_MAP)

        for col in ISONE_RESERVE_ZONE_FLOAT_COLUMNS:
            df[col] = df[col].astype(float)

        return df[ISONE_RESERVE_ZONE_ALL_COLUMNS].sort_values(
            ["Interval Start", "Reserve Zone Id"],
        )

    @support_date_range("DAY_START")
    def get_ancillary_services_strike_prices_day_ahead(
        self,
        date: str | pd.Timestamp | Literal["latest"],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get day-ahead strike prices and close-out components for ISO-NE.

        Args:
            date (pd.Timestamp | Literal["latest"]): The start date for the data request. Use "latest" for most recent data.
            end (pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pd.DataFrame: A DataFrame containing day-ahead strike prices and related data.
        """
        url = self._build_url("daasstrikeprices", date)
        response = self.make_api_call(url, verbose=verbose)

        df = pd.json_normalize(
            self._safe_get(
                response,
                "isone_web_services",
                "day_ahead_strike_prices",
                "day_ahead_strike_price",
            ),
        )

        if df.empty:
            raise NoDataFoundException(
                f"No day-ahead strike price data found for {date}",
            )

        # Parse market hour information - API returns timezone-aware datetimes
        df["Interval Start"] = pd.to_datetime(
            df["market_hour.local_day"],
            utc=True,
        ).dt.tz_convert(
            self.default_timezone,
        )
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)

        # Parse publish time
        df["Publish Time"] = pd.to_datetime(
            df["strike_price_timestamp"],
            utc=True,
        ).dt.tz_convert(
            self.default_timezone,
        )

        df = df.rename(
            columns={
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

        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df[
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
            ]
        ].sort_values(["Interval Start", "Publish Time"])

    @support_date_range("DAY_START")
    def get_binding_constraints_day_ahead_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        url = self._build_url("dayaheadconstraints", date)
        response = self.make_api_call(url, verbose=verbose)
        records = self._prepare_records(
            self._safe_get(response, "DayAheadConstraints", "DayAheadConstraint"),
        )

        if not records:
            raise NoDataFoundException(
                f"No day-ahead constraint data found for {date}.",
            )

        df = pd.DataFrame(records)

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
    ) -> pd.DataFrame:
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
    ) -> pd.DataFrame:
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
    ) -> pd.DataFrame:
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

        df = pd.DataFrame(records)

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
    ) -> pd.DataFrame:
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
    ) -> pd.DataFrame:
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
    ) -> pd.DataFrame:
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

        df = pd.DataFrame(records)

        rename_map = {
            "ConstraintName": "Constraint Name",
            "MarginalValue": "Marginal Value",
        }

        if constraint_type == "prelim":
            rename_map["ContingencyName"] = "Contingency Name"
            columns = ISONE_CONSTRAINT_FIVE_MIN_PRELIM_COLUMNS
        else:
            columns = ISONE_CONSTRAINT_FIVE_MIN_FINAL_COLUMNS

        return self._parse_constraint_dataframe(
            df,
            interval_field="BeginDate",
            interval_minutes=5,
            rename_map=rename_map,
            columns=columns,
        )

    def _parse_constraint_dataframe(
        self,
        df: pd.DataFrame,
        interval_field: str,
        interval_minutes: int,
        rename_map: dict[str, str],
        columns: list[str],
    ) -> pd.DataFrame:
        if df.empty:
            raise NoDataFoundException(
                "No constraint data found for the requested parameters.",
            )

        df = df.copy()

        df["Interval Start"] = pd.to_datetime(
            df[interval_field],
            errors="coerce",
            utc=True,
        ).dt.tz_convert(self.default_timezone)
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(
            minutes=interval_minutes,
        )
        df = df.rename(columns=rename_map)
        df["Marginal Value"] = pd.to_numeric(df["Marginal Value"], errors="coerce")

        if "Contingency Name" in df.columns:
            df["Contingency Name"] = (
                df["Contingency Name"]
                .astype("string")
                .str.strip()
                .mask(lambda s: s.isna() | (s == "") | (s.str.upper() == "NULL"))
                .map(lambda value: None if pd.isna(value) else value)
            )

        df = df.reindex(columns=columns)
        df = df.sort_values("Interval Start").reset_index(drop=True)
        return df

    @support_date_range(frequency="MONTH_START")
    def get_fcm_reconfiguration_monthly(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get FCM Monthly Reconfiguration Auction data.

        Args:
            date (str | pd.Timestamp): The start date for the data request. Use "latest" for most recent data.
            end (str | pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pd.DataFrame: A DataFrame containing monthly reconfiguration auction data.
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

        return self._parse_fcm_reconfiguration_dataframe(annotated_auctions)

    @support_date_range(frequency="YEAR_START")
    def get_fcm_reconfiguration_annual(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get FCM Annual Reconfiguration Auction data.

        Args:
            date (str | pd.Timestamp): The start date for the data request. Use "latest" for most recent data.
            end (str | pd.Timestamp | None): The end date for the data request. Only used if date is not "latest".
            verbose (bool): Whether to print verbose logging information.

        Returns:
            pd.DataFrame: A DataFrame containing annual reconfiguration auction data.
        """
        if date == "latest":
            endpoint = f"{self.base_url}/fcmara/current"
        else:
            start = date[0] if isinstance(date, tuple) else pd.Timestamp(date)
            cp_label = self._get_fcm_commitment_period_label(start)
            endpoint = f"{self.base_url}/fcmara/cp/{cp_label}"

        response = self.make_api_call(endpoint, verbose=verbose)

        auctions = self._prepare_records(
            self._safe_get(response, "FCMRAResults", "FCMRAResult"),
        )

        annotated_auctions: list[tuple[dict, pd.Timestamp, pd.Timestamp]] = []
        for wrapper in auctions:
            auction = wrapper.get("Auction", wrapper)
            period = auction.get("CommitmentPeriod", {})
            begin = period.get("BeginDate")
            end = period.get("EndDate")

            if begin and end:
                interval_start = pd.Timestamp(begin)
                interval_end = pd.Timestamp(end)
            else:
                description = auction.get("Description")
                interval_start = (
                    pd.Timestamp(description)
                    if description
                    else pd.Timestamp(auction.get("ApprovalDate"))
                )
                interval_end = interval_start + pd.DateOffset(years=1)

            annotated_auctions.append((auction, interval_start, interval_end))
        return self._parse_fcm_reconfiguration_dataframe(annotated_auctions)

    def _get_fcm_commitment_period_label(self, timestamp: pd.Timestamp) -> str:
        start_year = timestamp.year if timestamp.month >= 6 else timestamp.year - 1
        end_year_suffix = (start_year + 1) % 100
        return f"{start_year}-{end_year_suffix:02d}"

    def _parse_fcm_reconfiguration_dataframe(
        self,
        auction_records: list[tuple[dict, pd.Timestamp, pd.Timestamp]],
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []

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
            zone_frame = pd.json_normalize(
                auction,
                record_path=["ClearedCapacityZones", "ClearedCapacityZone"],
                errors="ignore",
            )

            zone_frame = zone_frame.rename(columns=zone_column_map)
            zone_frame = zone_frame.assign(
                **{
                    "Interval Start": interval_start,
                    "Interval End": interval_end,
                    "Location Type": "Capacity Zone",
                },
            )
            frames.append(zone_frame)

            interface_frame = pd.json_normalize(
                auction,
                record_path=[
                    "ClearedCapacityZones",
                    "ClearedCapacityZone",
                    "ClearedExternalInterfaces",
                    "ClearedExternalInterface",
                ],
                errors="ignore",
            )

            interface_frame = interface_frame.rename(columns=interface_column_map)
            interface_frame = interface_frame.assign(
                **{
                    "Interval Start": interval_start,
                    "Interval End": interval_end,
                    "Location Type": "External Interface",
                    "Capacity Zone Type": None,
                },
            )
            frames.append(interface_frame)

        df = pd.concat(frames, ignore_index=True)

        numeric_columns = [
            "Total Supply Offers Submitted",
            "Total Demand Bids Submitted",
            "Total Supply Offers Cleared",
            "Total Demand Bids Cleared",
            "Net Capacity Cleared",
            "Clearing Price",
        ]

        for column in numeric_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce")

        df = df.reindex(columns=ISONE_FCM_RECONFIGURATION_COLUMNS)
        df = df.sort_values(
            ["Interval Start", "Location ID"],
        ).reset_index(drop=True)

        log.debug(
            f"Processed FCM reconfiguration auction data. "
            f"{len(df)} entries from {df['Interval Start'].min()} to {df['Interval Start'].max()}",
        )

        return df
