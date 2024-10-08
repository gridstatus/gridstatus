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

# Updated LOCATION_MAP with .Z. prefix as keys
ZONE_LOCATIONID_MAP = {
    ".Z.MAINE": 4001,
    ".Z.NEWHAMPSHIRE": 4002,
    ".Z.VERMONT": 4003,
    ".Z.CONNECTICUT": 4004,
    ".Z.RHODEISLAND": 4005,
    ".Z.SEMASS": 4006,
    ".Z.WCMASS": 4007,
    ".Z.NEMASSBOST": 4008,
}


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

    def _local_now(self):
        return pd.Timestamp.now(tz=self.default_timezone)

    def _local_start_of_today(self):
        return pd.Timestamp.now(tz=self.default_timezone).floor("d")

    def _handle_end_date(self, date, end, days_to_add_if_no_end):
        if end:
            end = utils._handle_date(end, tz=self.default_timezone)
        else:
            end = (
                (date.tz_convert("UTC") + pd.DateOffset(days=days_to_add_if_no_end))
                .normalize()
                .tz_localize(None)
                .tz_localize(self.default_timezone)
            )
        return end

    def _get_location_id(self, location: str) -> int:
        """
        Get the LocId for a given location name.

        Args:
            location (str): The name of the location, with or without the .Z. prefix.

        Returns:
            int: The corresponding LocId.

        Raises:
            ValueError: If the location is not found in the LOCATION_MAP.
        """
        location_upper = location.upper().replace(" ", "")
        if not location_upper.startswith(".Z."):
            location_upper = f".Z.{location_upper}"

        if location_upper not in ZONE_LOCATIONID_MAP:
            raise ValueError(
                f"Invalid location: {location}. Valid locations are: {', '.join(ZONE_LOCATIONID_MAP.keys())}",
            )
        return ZONE_LOCATIONID_MAP[location_upper]

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

        if "Locations" not in response or "Location" not in response["Locations"]:
            raise NoDataFoundException("No location data found.")

        locations = response["Locations"]["Location"]
        df = pd.DataFrame(locations)
        df["LocId"] = pd.to_numeric(df["@LocId"], errors="coerce")
        df.rename(columns={"$": "Name", "@LocId": "LocId"}, inplace=True)
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
        df["LocId"] = pd.to_numeric(df["@LocId"], errors="coerce")
        df.rename(columns={"$": "Name", "@LocId": "LocId"}, inplace=True)
        return df

    def get_all_locations(self) -> pd.DataFrame:
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
        df["LocId"] = pd.to_numeric(df["@LocId"], errors="coerce")
        df.rename(columns={"$": "Name", "@LocId": "LocId"}, inplace=True)
        return df

    def get_realtime_hourly_demand_current(self) -> pd.DataFrame:
        """
        Get the most recent real-time hourly demand data.

        Returns:
            pandas.DataFrame: A DataFrame containing the real-time hourly demand data.
        """
        # NOTE(kladar) - the base URL, /realtimehourlydemand, returns a service info page, not data
        # so using the /current
        url = f"{BASE_URL}/realtimehourlydemand/current"

        response = self.make_api_call(url)
        log.debug(f"Response: {response}")

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

    def get_realtime_hourly_demand_historical_range(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        location: str = None,
    ) -> pd.DataFrame:
        """
        Get real-time hourly demand data for a specified date range and optional location.

        Args:
            date (str or datetime): The start date for the data request.
            end (str or datetime, optional): The end date for the data request.
            location (str, optional): The specific location to request data for (with or without .Z. prefix).

        Returns:
            pandas.DataFrame: A DataFrame containing the real-time hourly demand data.
        """
        date = utils._handle_date(date, tz=self.default_timezone)
        end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        url = f"{BASE_URL}/realtimehourlydemand/day/{date.strftime('%Y%m%d')}"
        if location:
            location_id = self._get_location_id(location)
            url += f"/location/{location_id}"

        response = self.make_api_call(url)
        log.debug(f"Response: {response}")

        if (
            "HourlyRtDemands" not in response
            or "HourlyRtDemand" not in response["HourlyRtDemands"]
        ):
            raise NoDataFoundException(
                "No real-time hourly demand data found for the specified date range.",
            )

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
