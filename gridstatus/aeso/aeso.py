import json
import os
from typing import Any, Literal

import pandas as pd
import requests
from requests.exceptions import HTTPError, RequestException

from gridstatus import utils
from gridstatus.aeso.aeso_constants import (
    ASSET_LIST_COLUMN_MAPPING,
    INTERCHANGE_COLUMN_MAPPING,
    RESERVES_COLUMN_MAPPING,
    SUPPLY_DEMAND_COLUMN_MAPPING,
)
from gridstatus.decorators import support_date_range


class AESO:
    """
    API client for Alberta Electric System Operator (AESO) data.

    Handles authentication and provides methods to access various AESO datasets
    including supply and demand, market data, and operational information.
    """

    # NB: Data is also typically provided in UTC
    default_timezone = "US/Mountain"

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

    def _make_request(self, endpoint: str, method: str = "GET") -> dict[str, Any]:
        """
        Make a request to the AESO API.

        Args:
            endpoint: API endpoint path (will be appended to base_url)
            method: HTTP method (default: GET)

        Returns:
            Parsed JSON response as dictionary

        Raises:
            HTTPError: If the API returns an HTTP error status
            RequestException: If there's a network/connection error
            ValueError: If the response is not valid JSON
        """
        url = f"{self.base_url}/{endpoint}"

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.default_headers,
            )
            response.raise_for_status()
            return response.json()
        except HTTPError as e:
            raise HTTPError(
                f"API request failed with status {response.status_code}: {str(e)}",
            )
        except RequestException as e:
            raise RequestException(f"Failed to connect to AESO API: {str(e)}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON response from API: {str(e)}")

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
            df["last_updated_datetime_utc"],
            utc=True,
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

        df = pd.json_normalize(
            data["return"],
            record_path="generation_data_list",
            meta=["last_updated_datetime_utc"],
        )
        df["Time"] = pd.to_datetime(
            df["last_updated_datetime_utc"],
            utc=True,
        ).dt.tz_convert(self.default_timezone)
        df["fuel_type"] = df["fuel_type"].str.title()

        result_df = df.pivot(
            index="Time",
            columns="fuel_type",
            values="aggregated_net_generation",
        ).reset_index()

        return result_df

    def get_interchange(self) -> pd.DataFrame:
        """
        Get current interchange flows with neighboring regions.

        Returns:
            DataFrame containing interchange data
        """
        endpoint = "currentsupplydemand-api/v1/csd/summary/current"
        data = self._make_request(endpoint)

        df = pd.json_normalize(
            data["return"],
            record_path="interchange_list",
            meta=["last_updated_datetime_utc"],
        )
        df["Time"] = pd.to_datetime(
            df["last_updated_datetime_utc"],
            utc=True,
        ).dt.tz_convert(self.default_timezone)
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
            df["last_updated_datetime_utc"],
            utc=True,
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
        df = pd.json_normalize(data["return"])

        if df.empty:
            return pd.DataFrame(columns=list(ASSET_LIST_COLUMN_MAPPING.values()))

        df = df.rename(columns=ASSET_LIST_COLUMN_MAPPING)
        df = df[list(ASSET_LIST_COLUMN_MAPPING.values())]

        return df

    @support_date_range(frequency=None)
    def get_pool_price(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get pool price data.

        Returns:
            DataFrame containing pool price data
        """
        if date == "latest":
            return self.get_pool_price(date="today")
        return self._get_pool_price_data(date, end, actual_or_forecast="actual")

    @support_date_range(frequency=None)
    def get_forecast_pool_price(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get pool price data.

        Returns:
            DataFrame containing pool price data
        """
        if date == "latest":
            return self.get_forecast_pool_price(date="today")
        return self._get_pool_price_data(date, end, actual_or_forecast="forecast")

    def _get_pool_price_data(
        self,
        start_date: str,
        end_date: str | None = None,
        actual_or_forecast: Literal["actual", "forecast"] = "actual",
    ) -> pd.DataFrame:
        """
        Get pool price data.

        Returns:
            DataFrame containing pool price data
        """
        start_date = pd.Timestamp(start_date).strftime("%Y-%m-%d")
        end_date = pd.Timestamp(end_date).strftime("%Y-%m-%d") if end_date else None

        endpoint = f"poolprice-api/v1.1/price/poolPrice?startDate={start_date}"
        if end_date:
            endpoint += f"&endDate={end_date}"
        data = self._make_request(endpoint)
        df = pd.json_normalize(data["return"]["Pool Price Report"])
        df["Interval Start"] = pd.to_datetime(
            df["begin_datetime_utc"],
            utc=True,
        ).dt.tz_convert(self.default_timezone)
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)
        df = df.rename(
            columns={
                "pool_price": "Pool Price",
                "forecast_pool_price": "Forecast Pool Price",
            },
        )

        if actual_or_forecast == "actual":
            df["Pool Price"] = pd.to_numeric(df["Pool Price"], errors="coerce")
            df = df[df["Pool Price"].notna()]
            return df[["Interval Start", "Interval End", "Pool Price"]]
        else:
            df["Forecast Pool Price"] = pd.to_numeric(
                df["Forecast Pool Price"],
                errors="coerce",
            )
            # NB: Publish times are a bit opaque from AESO, so we calculate a best estimate here.
            # Forecast pool price is provided for current and next two hours, updated every 5 minutes.
            # For future intervals: use request time floored to 5 minutes
            # For past/current intervals: use 5 minutes before interval start
            request_time = pd.Timestamp.now(tz=self.default_timezone)
            df["Publish Time"] = df.apply(
                lambda row: (
                    request_time.floor("5min")
                    if row["Interval Start"] > request_time
                    else row["Interval Start"] - pd.Timedelta(minutes=5)
                ),
                axis=1,
            )
            return df[
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "Forecast Pool Price",
                ]
            ]

    @support_date_range(frequency=None)
    def get_system_marginal_price(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get system marginal price data.

        Returns:
            DataFrame containing system marginal price data
        """
        if date == "latest":
            return self.get_system_marginal_price(date="today")

        start_date = pd.Timestamp(date).strftime("%Y-%m-%d")
        end_date = pd.Timestamp(end).strftime("%Y-%m-%d") if end else None

        endpoint = f"systemmarginalprice-api/v1.1/price/systemMarginalPrice?startDate={start_date}"
        if end_date:
            endpoint += f"&endDate={end_date}"
        data = self._make_request(endpoint)
        df = pd.json_normalize(data["return"]["System Marginal Price Report"])
        df["Time"] = pd.to_datetime(df["begin_datetime_utc"], utc=True).dt.tz_convert(
            self.default_timezone,
        )
        df = df.rename(
            columns={
                "system_marginal_price": "System Marginal Price",
                "volume": "Volume",
            },
        )
        df["System Marginal Price"] = pd.to_numeric(
            df["System Marginal Price"],
            errors="coerce",
        )
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")
        return df[["Time", "System Marginal Price", "Volume"]].sort_values(by="Time")
