import os
import time

import pandas as pd
import requests

from gridstatus import utils
from gridstatus.base import NoDataFoundException
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import logger as log

# API base URL
BASE_URL = "https://webservices.iso-ne.com/api/v1.1"

# Default page size for API requests
DEFAULT_PAGE_SIZE = 1000

# NOTE: the .Z. prefix is what is returned by the API so including it here
ZONE_LOCATIONID_MAP = {
    "NEPOOL": 32,
    "INTERNALHUB": 4000,
    "MAINE": 4001,
    "NEWHAMPSHIRE": 4002,
    "VERMONT": 4003,
    "CONNECTICUT": 4004,
    "RHODEISLAND": 4005,
    "SEMASS": 4006,
    "WCMASS": 4007,
    "NEMASSBOST": 4008,
}

EXCLUDE_FROM_REALTIME_HOURLY_DEMAND = [
    "NEPOOL",
    "INTERNALHUB",
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

    def make_api_call(
        self,
        url,
        api_params=None,
        parse_json=True,
        verbose=False,
    ):
        log.info(f"Requesting url: {url} with params: {api_params}")
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
        Get a list of all locations.

        Returns:
            pandas.DataFrame: A DataFrame containing location information.
        """
        url = f"{BASE_URL}/locations"
        response = self.make_api_call(url)
        print(response)
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

    def get_realtime_hourly_demand_current_static(self) -> pd.DataFrame:
        """
        Get the most recent real-time hourly demand data for default locations.

        Returns:
            pandas.DataFrame: A DataFrame containing the real-time hourly demand data for default locations.
        """
        url = f"{BASE_URL}/realtimehourlydemand/current"
        response = self.make_api_call(url)

        if (
            "HourlyRtDemands" not in response
            or "HourlyRtDemand" not in response["HourlyRtDemands"]
        ):
            raise NoDataFoundException("No real-time hourly demand data found.")

        formatted_data = [
            {
                "BeginDate": entry["BeginDate"],
                "Location": entry["Location"]["$"],
                "LocId": entry["Location"]["@LocId"],
                "Load": entry["Load"],
            }
            for entry in response["HourlyRtDemands"]["HourlyRtDemand"]
        ]

        df = pd.DataFrame(formatted_data)
        df["BeginDate"] = pd.to_datetime(df["BeginDate"])
        df["Load"] = pd.to_numeric(df["Load"], errors="coerce")
        df["LocId"] = pd.to_numeric(df["LocId"], errors="coerce")

        return df

    def get_realtime_hourly_demand_current(
        self,
        locations: list[str] = None,
    ) -> pd.DataFrame:
        """
        Get the most recent real-time hourly demand data for specified locations.

        Args:
            locations (list[str], optional): List of specific location names to request data for.
                                             If None, data for all locations will be retrieved.

        Returns:
            pandas.DataFrame: A DataFrame containing the real-time hourly demand data for all requested locations.
        """
        if not locations:
            locations = [
                loc
                for loc in ZONE_LOCATIONID_MAP.keys()
                if loc not in EXCLUDE_FROM_REALTIME_HOURLY_DEMAND
            ]

        all_data = []

        for location in locations:
            location_id = ZONE_LOCATIONID_MAP.get(location)
            if not location_id:
                log.warning(f"{location}: Not a known ISO NE Hub or Zone for this data")
                continue

            url = f"{BASE_URL}/realtimehourlydemand/current/location/{location_id}"
            response = self.make_api_call(url)
            print(response)
            data = response["HourlyRtDemand"]
            data["Location"] = location
            data["LocId"] = location_id
            all_data.append(data)

        df = pd.DataFrame(all_data)
        df["BeginDate"] = pd.to_datetime(df["BeginDate"])
        df["Load"] = pd.to_numeric(df["Load"], errors="coerce")
        df["LocId"] = pd.to_numeric(df["LocId"], errors="coerce")

        return df

    @support_date_range(frequency="D")
    def get_realtime_hourly_demand_historical_range(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        locations: list[str] = None,
    ) -> pd.DataFrame:
        """
        Get real-time hourly demand data for a specified date range and optional locations.

        Args:
            date (str or datetime): The start date for the data request.
            end (str or datetime, optional): The end date for the data request.
            locations (list[str], optional): List of specific locations to request data for.

        Returns:
            pandas.DataFrame: A DataFrame containing the real-time hourly demand data for all requested locations.
        """
        date = utils._handle_date(date, tz=self.default_timezone)
        end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        if not locations:
            locations = [
                loc
                for loc in ZONE_LOCATIONID_MAP.keys()
                if loc not in EXCLUDE_FROM_REALTIME_HOURLY_DEMAND
            ]

        all_data = []

        for location in locations:
            location_id = ZONE_LOCATIONID_MAP.get(location)
            if not location_id:
                log.warning(f"{location}: Not a known ISO NE Hub or Zone for this data")
                continue

            url = f"{BASE_URL}/realtimehourlydemand/day/{date.strftime('%Y%m%d')}/location/{location_id}"
            response = self.make_api_call(url)
            print(response)
            data = response["HourlyRtDemands"]["HourlyRtDemand"]
            for entry in data:
                entry["Location"] = location
                entry["LocId"] = location_id
            all_data.extend(data)

        df = pd.DataFrame(all_data)
        df["BeginDate"] = pd.to_datetime(df["BeginDate"])
        df["Load"] = pd.to_numeric(df["Load"], errors="coerce")
        df["LocId"] = pd.to_numeric(df["LocId"], errors="coerce")

        return df

    def get_dayahead_hourly_demand_current_static(self) -> pd.DataFrame:
        """
        Get the most recent day-ahead hourly demand data for default locations.

        Returns:
            pandas.DataFrame: A DataFrame containing the day-ahead hourly demand data for default locations.
        """
        url = f"{BASE_URL}/dayaheadhourlydemand/current"
        response = self.make_api_call(url)

        if (
            "HourlyDaDemands" not in response
            or "HourlyDaDemand" not in response["HourlyDaDemands"]
        ):
            raise NoDataFoundException("No day-ahead hourly demand data found.")

        formatted_data = [
            {
                "BeginDate": entry["BeginDate"],
                "Location": entry["Location"]["$"],
                "LocId": entry["Location"]["@LocId"],
                "Load": entry["Load"],
            }
            for entry in response["HourlyDaDemands"]["HourlyDaDemand"]
        ]

        df = pd.DataFrame(formatted_data)
        df["BeginDate"] = pd.to_datetime(df["BeginDate"])
        df["Load"] = pd.to_numeric(df["Load"], errors="coerce")
        df["LocId"] = pd.to_numeric(df["LocId"], errors="coerce")

        return df

    def get_dayahead_hourly_demand_current(
        self,
        locations: list[str] = None,
    ) -> pd.DataFrame:
        """
        Get the most recent day-ahead hourly demand data for specified locations.

        Args:
            locations (list[str], optional): List of specific location names to request data for.
                                             If None, data for all locations will be retrieved.

        Returns:
            pandas.DataFrame: A DataFrame containing the day-ahead hourly demand data for all requested locations.
        """
        if not locations:
            locations = list(ZONE_LOCATIONID_MAP.keys())

        all_data = []

        for location in locations:
            location_id = ZONE_LOCATIONID_MAP.get(location)
            if not location_id:
                log.warning(f"{location}: Not a known ISO NE Hub or Zone for this data")
                continue

            url = f"{BASE_URL}/dayaheadhourlydemand/current/location/{location_id}"

            response = self.make_api_call(url)
            print(response)
            data = response["HourlyDaDemand"]
            data["Location"] = location
            data["LocId"] = location_id
            all_data.append(data)

        if not all_data:
            raise NoDataFoundException(
                "No day-ahead hourly demand data found for any of the specified locations.",
            )

        df = pd.DataFrame(all_data)
        df["BeginDate"] = pd.to_datetime(df["BeginDate"])
        df["Load"] = pd.to_numeric(df["Load"], errors="coerce")
        df["LocId"] = pd.to_numeric(df["LocId"], errors="coerce")

        return df

    @support_date_range(frequency="D")
    def get_dayahead_hourly_demand_historical_range(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        locations: list[str] = None,
    ) -> pd.DataFrame:
        """
        Get day-ahead hourly demand data for a specified date range and optional locations.

        Args:
            date (str or datetime): The start date for the data request.
            end (str or datetime, optional): The end date for the data request.
            locations (list[str], optional): List of specific location names to request data for.

        Returns:
            pandas.DataFrame: A DataFrame containing the day-ahead hourly demand data for all requested locations.
        """
        date = utils._handle_date(date, tz=self.default_timezone)
        end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        if not locations:
            locations = list(ZONE_LOCATIONID_MAP.keys())

        all_data = []

        for location in locations:
            location_id = ZONE_LOCATIONID_MAP.get(location)
            if not location_id:
                log.warning(f"{location}: Not a known ISO NE Hub or Zone for this data")
                continue

            url = f"{BASE_URL}/dayaheadhourlydemand/day/{date.strftime('%Y%m%d')}/location/{location_id}"
            response = self.make_api_call(url)

            data = response["HourlyDaDemands"]["HourlyDaDemand"]
            for entry in data:
                entry["Location"] = location
                entry["LocId"] = location_id
            all_data.extend(data)

        df = pd.DataFrame(all_data)
        df["BeginDate"] = pd.to_datetime(df["BeginDate"])
        df["Load"] = pd.to_numeric(df["Load"], errors="coerce")
        df["LocId"] = pd.to_numeric(df["LocId"], errors="coerce")

        return df
