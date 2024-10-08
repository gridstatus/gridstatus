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

    def get_realtime_hourly_demand(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
    ) -> pd.DataFrame:
        """
        Get real-time hourly demand data for a specified date range.

        Args:
            date (str or datetime): The start date for the data request.
            end (str or datetime, optional): The end date for the data request.

        Returns:
            pandas.DataFrame: A DataFrame containing the real-time hourly demand data.
        """
        date = utils._handle_date(date, tz=self.default_timezone)
        end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        url = f"{BASE_URL}/realtimehourlydemand/day/{date.strftime('%Y%m%d')}"

        response = self.make_api_call(url)
        log.info(f"Response: {response}")

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

    def get_realtime_hourly_demand_current(self) -> pd.DataFrame:
        """
        Get the most recent real-time hourly demand data.

        Returns:
            pandas.DataFrame: A DataFrame containing the real-time hourly demand data.
        """
        url = f"{BASE_URL}/realtimehourlydemand/current"

        response = self.make_api_call(url)
        log.info(f"Response: {response}")

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
        df.set_index("BeginDate", inplace=True)

        # Convert numeric columns to appropriate data types
        df["Load"] = pd.to_numeric(df["Load"], errors="coerce")
        df["LocId"] = pd.to_numeric(df["LocId"], errors="coerce")

        return df
