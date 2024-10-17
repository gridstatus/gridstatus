import os
import time
from datetime import datetime
from typing import Literal

import pandas as pd
import pytz
import requests

from gridstatus.base import NoDataFoundException
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import logger as log

# API base URL
BASE_URL = "https://webservices.iso-ne.com/api/v1.1"

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

        self.sleep_seconds = sleep_seconds
        self.initial_delay = min(sleep_seconds, 60.0)
        self.max_retries = min(max(0, max_retries), 10)

    # TODO(kladar) abstract this out to a base class since it is shared with ERCOT API logic
    def make_api_call(
        self,
        url: str,
        api_params: dict = None,
        parse_json: bool = True,
        verbose: bool = False,
    ):
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
        url = f"{BASE_URL}/locations"
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
        url = f"{BASE_URL}/locations/{location_id}"
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
        url = f"{BASE_URL}/locations/all"
        response = self.make_api_call(url)

        if "Locations" not in response or "Location" not in response["Locations"]:
            raise NoDataFoundException("No location data found.")

        locations = response["Locations"]["Location"]
        df = pd.DataFrame(locations)
        return df

    def _handle_demand(
        self,
        df: pd.DataFrame,
        interval_minutes: int = 60,
    ) -> pd.DataFrame:
        """
        Process demand DataFrame: convert types, rename columns, and add Interval End.

        Args:
            df (pd.DataFrame): Input DataFrame with demand data.
            interval_minutes (int): Duration of each interval in minutes. Default is 60.

        Returns:
            pd.DataFrame: Processed DataFrame.
        """

        def parse_problematic_datetime(date_string: str) -> pd.Timestamp:
            dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z")
            return dt.astimezone(pytz.timezone(self.default_timezone))

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
            df["Interval Start"] = df["BeginDate"].apply(parse_problematic_datetime)

        df = df.sort_values("Interval Start")
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(
            minutes=interval_minutes,
        )

        df["Load"] = pd.to_numeric(df["Load"], errors="coerce")
        df["Location Id"] = pd.to_numeric(df["LocId"], errors="coerce")

        log.info(
            f"Processed demand data: {len(df)} entries from {df['Interval Start'].min()} to {df['Interval Start'].max()}",
        )
        return df[["Interval Start", "Interval End", "Location", "Location Id", "Load"]]

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
                url = f"{BASE_URL}/realtimehourlydemand/current"
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
                    url = f"{BASE_URL}/realtimehourlydemand/current/location/{location_id}"
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

                    url = f"{BASE_URL}/realtimehourlydemand/day/{date.strftime('%Y%m%d')}/location/{location_id}"
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
                url = f"{BASE_URL}/dayaheadhourlydemand/current"
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
                    url = f"{BASE_URL}/dayaheadhourlydemand/current/location/{location_id}"
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

                    url = f"{BASE_URL}/dayaheadhourlydemand/day/{date.strftime('%Y%m%d')}/location/{location_id}"
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

        def parse_problematic_datetime(date_string: str) -> pd.Timestamp:
            dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z")
            return dt.astimezone(pytz.timezone(self.default_timezone))

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
            df["BeginDate"] = df["BeginDate"].apply(parse_problematic_datetime)
            df["CreationDate"] = df["CreationDate"].apply(parse_problematic_datetime)

        regional_cols = {
            "BeginDate": "Interval Start",
            "CreationDate": "Publish Time",
            "ReliabilityRegion": "Location",
            "LoadMw": "Load",
            "ReliabilityRegionLoadPercentage": "Regional Percentage",
        }
        system_cols = {
            "BeginDate": "Interval Start",
            "CreationDate": "Publish Time",
            "LoadMw": "Load",
            "NetLoadMw": "Net Load",
        }
        if "ReliabilityRegion" in df.columns:
            df = df.rename(columns=regional_cols)
            df["Regional Percentage"] = pd.to_numeric(
                df["Regional Percentage"],
                errors="coerce",
            )
        else:
            df = df.rename(columns=system_cols)
            df["Net Load"] = pd.to_numeric(df["Net Load"], errors="coerce")

        df = df.sort_values(["Interval Start", "Publish Time"])
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(
            minutes=interval_minutes,
        )
        df["Load"] = pd.to_numeric(df["Load"], errors="coerce")

        log.info(
            f"Processed load forecast data: {len(df)} entries from {df['Interval Start'].min()} to {df['Interval Start'].max()}",
        )
        return df[
            list(
                regional_cols.values()
                if "ReliabilityRegion" in df.columns
                else system_cols.values(),
            )
        ]

    @support_date_range("DAY_START")
    def get_hourly_load_forecast(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        horizons: Literal["latest", "all"] = "all",
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get the hourly load forecast data for specified locations and date range.

        Args:
            date (str): The start date for the data request. Use "latest" for most recent data.
            end_date (str | None): The end date for the data request. Only used if date is not "latest".
            locations (list[str], optional): List of specific location names to request data for.
                                             If None, data for all locations will be retrieved.
        """

        if date == "latest":
            url = f"{BASE_URL}/hourlyloadforecast/current"
            response = self.make_api_call(url)
            from pprint import pprint

            pprint(response)
            df = pd.DataFrame(response["HourlyLoadForecast"])
            return self._handle_load_forecast(df, interval_minutes=60)
        elif horizons == "all":
            url = f"{BASE_URL}/hourlyloadforecast/all/day/{date.strftime('%Y%m%d')}"
            response = self.make_api_call(url)
            df = pd.DataFrame(response["HourlyLoadForecasts"]["HourlyLoadForecast"])
            return self._handle_load_forecast(df, interval_minutes=60)
        else:
            url = f"{BASE_URL}/hourlyloadforecast/day/{date.strftime('%Y%m%d')}"
            response = self.make_api_call(url)
            df = pd.DataFrame(response["HourlyLoadForecasts"]["HourlyLoadForecast"])
            return self._handle_load_forecast(df, interval_minutes=60)

    @support_date_range("DAY_START")
    def get_reliability_region_load_forecast(
        self,
        date: str | pd.Timestamp = "latest",
        end: str | pd.Timestamp | None = None,
        horizons: Literal["latest", "all"] = "all",
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get the day-ahead hourly demand data for specified locations and date range.

        Args:
            date (str): The start date for the data request. Use "latest" for most recent data.
            end (str | None): The end date for the data request. Only used if date is not "latest".
            horizons (Literal["latest", "all"]): The horizon for the data request. Options are "latest" or "all".

        Returns:
            pandas.DataFrame: A DataFrame containing the day-ahead hourly demand data for all requested locations.
        """

        if date == "latest":
            url = f"{BASE_URL}/reliabilityregionloadforecast/current"
            response = self.make_api_call(url)
            df = pd.DataFrame(
                response["ReliabilityRegionLoadForecasts"][
                    "ReliabilityRegionLoadForecast"
                ],
            )
            return self._handle_load_forecast(df, interval_minutes=60)
        elif horizons == "all":
            # NOTE(kladar) the "all" is all horizons available for a given interval.
            url = f"{BASE_URL}/reliabilityregionloadforecast/day/{date.strftime('%Y%m%d')}/all"
            response = self.make_api_call(url)
            df = pd.DataFrame(
                response["ReliabilityRegionLoadForecasts"][
                    "ReliabilityRegionLoadForecast"
                ],
            )
            return self._handle_load_forecast(df, interval_minutes=60)
        else:
            # NOTE(kladar) horizon expands data by 10x-20x for historical data, since
            # there can be a forecast every half hour for several days leading up to an interval.
            # Giving the option for just the "latest" forecast (aka shortest horizon) for a given historical interval.
            url = f"{BASE_URL}/reliabilityregionloadforecast/day/{date.strftime('%Y%m%d')}"
            response = self.make_api_call(url)
            df = pd.DataFrame(
                response["ReliabilityRegionLoadForecasts"][
                    "ReliabilityRegionLoadForecast"
                ],
            )
            return self._handle_load_forecast(df, interval_minutes=60)
