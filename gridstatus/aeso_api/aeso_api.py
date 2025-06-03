import json
import os
import urllib.request
from urllib.error import HTTPError, URLError

import pandas as pd

from gridstatus import utils
from gridstatus.aeso_api.aeso_api_constants import (
    ASSET_LIST_COLUMN_MAPPING,
    INTERCHANGE_COLUMN_MAPPING,
    RESERVES_COLUMN_MAPPING,
    SUPPLY_DEMAND_COLUMN_MAPPING,
)


class AESO:
    """
    API client for Alberta Electric System Operator (AESO) data.

    Handles authentication and provides methods to access various AESO datasets
    including supply and demand, market data, and operational information.
    """

    # NB: Data is also typically provided in UTC
    default_timezone = "America/Edmonton"

    def __init__(self, api_key: str | None = None):
        """
        Initialize the AESO API client.

        Args:
            api_key: AESO API key. If not provided, will try to get from AESO_API_KEY environment variable.
        """
        self.api_key = api_key or os.getenv("AESO_API_KEY")
        if not self.api_key:
            raise ValueError(
                "API key is required. Provide it directly or set AESO_API_KEY environment variable.",
            )

        self.base_url = "https://apimgw.aeso.ca/public"
        self.default_headers = {
            "Cache-Control": "no-cache",
            "API-KEY": self.api_key,
        }

    def _make_request(self, endpoint: str, method: str = "GET") -> dict[str,]:
        """
        Make a request to the AESO API.

        Args:
            endpoint: API endpoint path (will be appended to base_url)
            method: HTTP method (default: GET)

        Returns:
            Parsed JSON response as dictionary

        Raises:
            HTTPError: If the API returns an HTTP error status
            URLError: If there's a network/connection error
            ValueError: If the response is not valid JSON
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        req = urllib.request.Request(url, headers=self.default_headers)
        req.get_method = lambda: method

        try:
            response = urllib.request.urlopen(req)
            response_data = response.read()

            if response.getcode() != 200:
                raise HTTPError(
                    url,
                    response.getcode(),
                    f"API request failed with status {response.getcode()}",
                    response.headers,
                    None,
                )

            try:
                return json.loads(response_data.decode("utf-8"))
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON response from API: {e}")

        except HTTPError:
            raise
        except URLError as e:
            raise URLError(f"Failed to connect to AESO API: {e}")

    def get_supply_and_demand(self) -> pd.DataFrame:
        """
        Get current supply and demand summary data.

        Returns:
            DataFrame containing current supply and demand information
        """
        endpoint = "currentsupplydemand-api/v1/csd/summary/current"
        data = self._make_request(endpoint)

        df = pd.json_normalize(data["return"])

        df["Time"] = pd.to_datetime(
            df["last_updated_datetime_utc"] + "+0000",
            format="%Y-%m-%d %H:%M%z",
        ).dt.tz_convert(self.default_timezone)

        df = df.rename(
            columns={
                k: v for k, v in SUPPLY_DEMAND_COLUMN_MAPPING.items() if k in df.columns
            },
        )

        if "generation_data_list" in data["return"]:
            gen_df = pd.DataFrame(data["return"]["generation_data_list"])
            for _, row in gen_df.iterrows():
                fuel_type = row["fuel_type"].title().replace(" ", " ")
                df[f"{fuel_type} Maximum Capability"] = row[
                    "aggregated_maximum_capability"
                ]
                df[f"{fuel_type} Net Generation"] = row["aggregated_net_generation"]
                df[f"{fuel_type} Dispatched Contingency Reserve"] = row[
                    "aggregated_dispatched_contingency_reserve"
                ]

        if "interchange_list" in data["return"]:
            for interchange in data["return"]["interchange_list"]:
                path = interchange["path"].title().replace(" ", " ")
                df[f"{path} Flow"] = interchange["actual_flow"]

        df = df[list(SUPPLY_DEMAND_COLUMN_MAPPING.values())]

        return utils.move_cols_to_front(df, ["Time"])

    def get_fuel_mix(self) -> pd.DataFrame:
        """
        Get current generation by fuel type.

        Returns:
            DataFrame containing generation data by fuel type, with each fuel type as a column
            containing its net generation value
        """
        endpoint = "currentsupplydemand-api/v1/csd/summary/current"
        data = self._make_request(endpoint)

        df = pd.DataFrame(data["return"]["generation_data_list"])
        df["Time"] = pd.to_datetime(
            data["return"]["last_updated_datetime_utc"] + "+0000",
            format="%Y-%m-%d %H:%M%z",
        ).tz_convert(self.default_timezone)

        result_df = pd.DataFrame({"Time": [df["Time"].iloc[0]]})

        for _, row in df.iterrows():
            fuel_type = row["fuel_type"].title().replace(" ", " ")
            result_df[fuel_type] = [row["aggregated_net_generation"]]

        return result_df

    def get_interchange(self) -> pd.DataFrame:
        """
        Get current interchange flows with neighboring regions.

        Returns:
            DataFrame containing interchange data
        """
        endpoint = "currentsupplydemand-api/v1/csd/summary/current"
        data = self._make_request(endpoint)

        df = pd.DataFrame(data["return"]["interchange_list"])
        df["Time"] = pd.to_datetime(
            data["return"]["last_updated_datetime_utc"] + "+0000",
            format="%Y-%m-%d %H:%M%z",
        ).tz_convert(self.default_timezone)

        df = df.rename(columns=INTERCHANGE_COLUMN_MAPPING)
        df = df[list(INTERCHANGE_COLUMN_MAPPING.values())]

        return utils.move_cols_to_front(df, ["Time"])

    def get_reserves(self) -> pd.DataFrame:
        """
        Get current reserve data.

        Returns:
            DataFrame containing reserve information
        """
        endpoint = "currentsupplydemand-api/v1/csd/summary/current"
        data = self._make_request(endpoint)

        df = pd.json_normalize(data["return"])
        df["Time"] = pd.to_datetime(
            df["last_updated_datetime_utc"] + "+0000",
            format="%Y-%m-%d %H:%M%z",
        ).dt.tz_convert(self.default_timezone)

        df = df.rename(columns=RESERVES_COLUMN_MAPPING)
        df = df[list(RESERVES_COLUMN_MAPPING.values())]

        return utils.move_cols_to_front(df, ["Time"])

    # TODO(kladar): WIP until asset datasets which are next
    def get_asset_list(
        self,
        asset_id: str | None = None,
        pool_participant_id: str | None = None,
        operating_status: str | None = None,
        asset_type: str | None = None,
    ) -> pd.DataFrame:
        """
        Get list of assets in the AESO system.

        Args:
            asset_id: Filter by specific asset ID
            pool_participant_id: Filter by pool participant ID
            operating_status: Filter by operating status
            asset_type: Filter by asset type

        Returns:
            DataFrame containing asset information
        """
        endpoint = "assetlist-api/v1/assetlist"

        params = []
        if asset_id:
            params.append(f"asset_ID={asset_id}")
        if pool_participant_id:
            params.append(f"pool_participant_ID={pool_participant_id}")
        if operating_status:
            params.append(f"operating_status={operating_status}")
        if asset_type:
            params.append(f"asset_type={asset_type}")

        if params:
            endpoint += "?" + "&".join(params)

        data = self._make_request(endpoint)

        if not data["return"]:
            return pd.DataFrame(columns=list(ASSET_LIST_COLUMN_MAPPING.values()))

        df = pd.DataFrame(data["return"])
        df = df.rename(columns=ASSET_LIST_COLUMN_MAPPING)
        df = df[list(ASSET_LIST_COLUMN_MAPPING.values())]

        return df
