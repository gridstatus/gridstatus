import os
import time

import pandas as pd
import requests

from gridstatus import utils
from gridstatus.base import NoDataFoundException
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
        sleep_seconds: float = 0.2,
        max_retries: int = 3,
    ):
        self.username = os.getenv("ISONE_API_USERNAME")
        self.password = os.getenv("ISONE_API_PASSWORD")

        if not all([self.username, self.password]):
            raise ValueError(
                "Username and password must be provided or set as environment variables",
            )

        self.sleep_seconds = sleep_seconds
        self.initial_delay = min(max(0.1, sleep_seconds), 60.0)
        self.max_retries = min(max(0, max_retries), 10)

    def _handle_end_date(
        self,
        date: pd.Timestamp,
        end: pd.Timestamp | None,
        days_to_add_if_no_end: int,
    ) -> pd.Timestamp:
        """
        Handles a provided end date by either

        1. Using the provided end date converted to the default timezone
        2. Adding the number of days to the date and converting to the default timezone
        """
        if end:
            end = utils._handle_date(end, tz=self.default_timezone)
        else:
            # Have to convert to UTC to do addition, then convert back to local time
            # to avoid DST issues
            end = (
                (date.tz_convert("UTC") + pd.DateOffset(days=days_to_add_if_no_end))
                .normalize()
                .tz_localize(None)
                .tz_localize(self.default_timezone)
            )

        return end

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
        df["Interval Start"] = pd.to_datetime(df["BeginDate"])
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(
            minutes=interval_minutes,
        )
        df["Load"] = pd.to_numeric(df["Load"], errors="coerce")
        df["Location Id"] = pd.to_numeric(df["LocId"], errors="coerce")
        return df[["Interval Start", "Interval End", "Location", "Location Id", "Load"]]

    # @support_date_range("DAY_START")
    def get_realtime_hourly_demand(
        self,
        date: str = "latest",
        end_date: str | None = None,
        locations: list[str] = None,
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
            case ("latest", None) | (None, None):
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
                    for item in data:
                        item["Location"] = location
                        item["LocId"] = location_id
                    all_data.extend(data)

            case _:
                if not locations:
                    locations = [
                        loc
                        for loc in ZONE_LOCATIONID_MAP.keys()
                        if loc not in EXCLUDE_FROM_REALTIME_HOURLY_DEMAND
                    ]

                for location in locations:
                    location_id = ZONE_LOCATIONID_MAP.get(location)
                    if not location_id:
                        raise ValueError(
                            f"{location}: Not a known ISO NE Hub or Zone for this data",
                        )

                    url = f"{BASE_URL}/realtimehourlydemand/day/{date.strftime('%Y%m%d')}/location/{location_id}"
                    response = self.make_api_call(url)
                    data = response["HourlyRtDemand"]
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

    # @support_date_range("DAY_START")
    def get_dayahead_hourly_demand(
        self,
        date: str = "latest",
        end_date: str | None = None,
        locations: list[str] = None,
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
                    for item in data:
                        item["Location"] = location
                        item["LocId"] = location_id
                    all_data.extend(data)

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
                    data = response["HourlyDaDemand"]
                    for item in data:
                        item["Location"] = location
                        item["LocId"] = location_id
                    all_data.extend(data)

        if not all_data:
            raise NoDataFoundException(
                "No day-ahead hourly demand data found for the specified parameters.",
            )

        df = pd.DataFrame(all_data)
        return self._handle_demand(df, interval_minutes=60)
