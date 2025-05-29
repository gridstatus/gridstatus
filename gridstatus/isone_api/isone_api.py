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
            return self.get_lmp_real_time_5_min_final("today")

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

        df["Publish Time"] = pd.to_datetime(df["CreationDate"])
        df["Interval Start"] = pd.to_datetime(df["MarketDate"])
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
