import json
import os
import re
from typing import Any, Literal

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError, RequestException

from gridstatus import utils
from gridstatus.aeso.aeso_constants import (
    ASSET_LIST_COLUMN_MAPPING,
    RESERVES_COLUMN_MAPPING,
    SUPPLY_DEMAND_COLUMN_MAPPING,
)
from gridstatus.base import NotSupported
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import setup_gs_logger

logger = setup_gs_logger("aeso")


class AESO:
    """
    API client for Alberta Electric System Operator (AESO) data.

    Handles authentication and provides methods to access various AESO datasets
    including supply and demand, market data, and operational information.
    """

    # NB: Data is also typically provided in UTC
    default_timezone = "US/Mountain"
    MAX_NAVIGATION_ATTEMPTS = 100

    HISTORICAL_FORECAST_EARLIEST = pd.Timestamp("2023-03-01").tz_localize(
        default_timezone,
    )
    HISTORICAL_FORECAST_LATEST = pd.Timestamp("2025-04-01").tz_localize(
        default_timezone,
    )

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

    @support_date_range(frequency=None)
    def get_load(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get current load data.

        Returns:
            DataFrame containing load data
        """
        if date == "latest":
            return self.get_load(date="today")

        start_date = pd.Timestamp(date).strftime("%Y-%m-%d")
        end_date = pd.Timestamp(end).strftime("%Y-%m-%d") if end else None

        endpoint = (
            f"actualforecast-api/v1/load/albertaInternalLoad?startDate={start_date}"
        )
        if end_date:
            endpoint += f"&endDate={end_date}"

        data = self._make_request(endpoint)

        df = pd.json_normalize(data["return"]["Actual Forecast Report"])
        df["Interval Start"] = pd.to_datetime(
            df["begin_datetime_utc"],
            utc=True,
        ).dt.tz_convert(self.default_timezone)
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)
        df = df.rename(
            columns={
                "alberta_internal_load": "Load",
            },
        )
        df["Load"] = pd.to_numeric(df["Load"], errors="coerce")
        df = df.dropna(subset=["Load"])
        return df[["Interval Start", "Interval End", "Load"]]

    @support_date_range(frequency=None)
    def get_load_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get load forecast data.

        The AESO publishes load forecasts daily at 7am Mountain Time. The forecast covers
        the next 13 days. The publish time is determined as follows:
        - For historical data: 7am on the day of the interval if interval is after 7am,
          otherwise 7am the previous day
        - For future data: 7am today (if after 7am) or 7am yesterday (if before 7am)

        Returns:
            DataFrame containing load forecast data with publish times
        """
        if date == "latest":
            current_time = pd.Timestamp.now(tz=self.default_timezone)
            today_7am = current_time.floor("D") + pd.Timedelta(hours=7)
            publish_time = (
                today_7am
                if current_time >= today_7am
                else today_7am - pd.Timedelta(days=1)
            )
            end = publish_time + pd.Timedelta(days=13)
            df = self.get_load_forecast(date=publish_time, end=end)
            df = df[df["Publish Time"] == publish_time]
            return df

        start_date = pd.Timestamp(date).strftime("%Y-%m-%d")
        end_date = pd.Timestamp(end).strftime("%Y-%m-%d") if end else None

        endpoint = (
            f"actualforecast-api/v1/load/albertaInternalLoad?startDate={start_date}"
        )
        if end_date:
            endpoint += f"&endDate={end_date}"

        data = self._make_request(endpoint)
        df = pd.json_normalize(data["return"]["Actual Forecast Report"])
        df["Interval Start"] = pd.to_datetime(
            df["begin_datetime_utc"],
            utc=True,
        ).dt.tz_convert(self.default_timezone)
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)
        df = df.rename(
            columns={
                "alberta_internal_load": "Load",
                "forecast_alberta_internal_load": "Load Forecast",
            },
        )

        current_time = pd.Timestamp.now(tz=self.default_timezone)
        today_7am = current_time.floor("D") + pd.Timedelta(hours=7)

        def get_publish_time(row: pd.Series) -> pd.Timestamp:
            interval_day_7am = row["Interval Start"].floor("D") + pd.Timedelta(hours=7)
            if row["Interval Start"] > current_time:
                # NB: For future intervals, use today's 7am if after 7am, otherwise yesterday's 7am
                return (
                    today_7am
                    if current_time >= today_7am
                    else today_7am - pd.Timedelta(days=1)
                )
            else:
                # NB: For historical data, use 7am on the day of the interval if after 7am,
                # otherwise use 7am the previous day
                return (
                    interval_day_7am
                    if row["Interval Start"] >= interval_day_7am
                    else interval_day_7am - pd.Timedelta(days=1)
                )

        df["Publish Time"] = df.apply(get_publish_time, axis=1)
        df["Load"] = pd.to_numeric(df["Load"], errors="coerce")
        df["Load Forecast"] = pd.to_numeric(df["Load Forecast"], errors="coerce")
        return df[
            ["Interval Start", "Interval End", "Publish Time", "Load", "Load Forecast"]
        ]

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
            DataFrame containing interchange data with separate columns for each region's flow
            and a net interchange flow column
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

        df = df.pivot(
            index="Time",
            columns="path",
            values="actual_flow",
        ).reset_index()

        df = df.rename(
            columns={
                "British Columbia": "British Columbia Flow",
                "Montana": "Montana Flow",
                "Saskatchewan": "Saskatchewan Flow",
            },
        )

        flow_columns = [col for col in df.columns if col != "Time"]
        df["Net Interchange Flow"] = df[flow_columns].sum(axis=1)

        cols = ["Time", "Net Interchange Flow"] + flow_columns
        df = df[cols]

        return df

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
                "rolling_30day_avg": "Rolling 30 Day Average Pool Price",
            },
        )

        if actual_or_forecast == "actual":
            df["Pool Price"] = pd.to_numeric(df["Pool Price"], errors="coerce")
            df = df[df["Pool Price"].notna()]
            return df[
                [
                    "Interval Start",
                    "Interval End",
                    "Pool Price",
                    "Rolling 30 Day Average Pool Price",
                ]
            ]
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
    def get_daily_average_pool_price(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get daily average pool price data with on-peak and off-peak breakdowns.

        On-peak hours are defined as hours ending 8 through 23 (inclusive).
        Off-peak hours are all other hours.

        Returns:
            DataFrame containing daily average price data
        """
        if date == "latest":
            return self.get_daily_average_pool_price(date="today")

        hourly_df = self._get_pool_price_data(date, end, actual_or_forecast="actual")
        hourly_df["Is On Peak"] = hourly_df["Interval End"].dt.hour.isin(range(8, 24))
        hourly_df["Pool Price"] = pd.to_numeric(
            hourly_df["Pool Price"],
            errors="coerce",
        )
        hourly_df["Rolling 30 Day Average Pool Price"] = pd.to_numeric(
            hourly_df["Rolling 30 Day Average Pool Price"],
            errors="coerce",
        )

        agg_dict = {
            "Pool Price": "mean",
            "Is On Peak": lambda x: x.sum(),
        }

        daily_df = (
            hourly_df.groupby(hourly_df["Interval Start"].dt.date)
            .agg(agg_dict)
            .reset_index()
        )

        # NB: 30 rolling average is already an average, so this just passes
        # through the 23:00:00 value for each day, which is what the ETS system shows
        daily_30day_avg = (
            hourly_df[hourly_df["Interval Start"].dt.hour == 23]
            .groupby(hourly_df["Interval Start"].dt.date)
            .agg({"Rolling 30 Day Average Pool Price": "first"})
            .reset_index()
        )
        daily_30day_avg = daily_30day_avg.rename(
            columns={"Rolling 30 Day Average Pool Price": "30 Day Average"},
        )

        daily_averages = []
        for _, day_row in daily_df.iterrows():
            day_date = day_row["Interval Start"]
            day_hourly = hourly_df[hourly_df["Interval Start"].dt.date == day_date]

            on_peak_prices = day_hourly[day_hourly["Is On Peak"]]["Pool Price"]
            daily_on_peak_avg = (
                on_peak_prices.mean() if not on_peak_prices.empty else None
            )

            off_peak_prices = day_hourly[~day_hourly["Is On Peak"]]["Pool Price"]
            daily_off_peak_avg = (
                off_peak_prices.mean() if not off_peak_prices.empty else None
            )

            daily_averages.append(
                {
                    "date": day_date,
                    "Daily On Peak Average": daily_on_peak_avg,
                    "Daily Off Peak Average": daily_off_peak_avg,
                },
            )

        daily_avg_df = pd.DataFrame(daily_averages)

        result_df = daily_df.merge(
            daily_avg_df,
            left_on="Interval Start",
            right_on="date",
            how="left",
        )

        result_df = result_df.merge(
            daily_30day_avg,
            left_on="Interval Start",
            right_on="Interval Start",
            how="left",
        )

        result_df["Interval Start"] = pd.to_datetime(
            result_df["Interval Start"],
        ).dt.tz_localize(self.default_timezone)
        result_df["Interval End"] = result_df["Interval Start"] + pd.Timedelta(days=1)

        result_df = result_df.rename(
            columns={
                "Pool Price": "Daily Average",
                "Rolling 30 Day Average Pool Price": "30 Day Average",
            },
        )

        price_columns = [
            "Daily Average",
            "Daily On Peak Average",
            "Daily Off Peak Average",
            "30 Day Average",
        ]
        for col in price_columns:
            if col in result_df.columns:
                result_df[col] = result_df[col].round(2)

        return (
            result_df[
                [
                    "Interval Start",
                    "Interval End",
                    "Daily Average",
                    "Daily On Peak Average",
                    "Daily Off Peak Average",
                    "30 Day Average",
                ]
            ]
            .sort_values("Interval Start")
            .reset_index(drop=True)
        )

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
            DataFrame containing system marginal price data with minutely intervals
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

        df["Interval Start"] = pd.to_datetime(
            df["begin_datetime_utc"],
            utc=True,
        ).dt.tz_convert(self.default_timezone)
        df["Interval End"] = pd.to_datetime(
            df["end_datetime_utc"],
            utc=True,
        ).dt.tz_convert(self.default_timezone)

        # NB: The latest value that the AESO API returns always has an interval that ends at 23:59 of the current day.
        # We want to set the interval end to 1 minute after the interval start to make it consistent with the other intervals
        # and make the forward filling easier.
        # Example: If the latest interval start is 2025-06-12 23:00:00,
        # the interval end will be changed from 2025-06-12 23:59:00 to 2025-06-12 23:01:00.
        mask = df["Interval End"].dt.strftime("%H:%M") == "23:59"
        df.loc[mask, "Interval End"] = df.loc[mask, "Interval Start"] + pd.Timedelta(
            minutes=1,
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
        df = df.sort_values(by="Interval Start")

        # NB: Add current time as final interval if needed, but only for today's data
        current_time = pd.Timestamp.now(tz=self.default_timezone)
        today_start = current_time.floor("D")
        if (
            df["Interval Start"].min().floor("D") == today_start
            and df["Interval End"].max() < current_time
        ):
            last_row = df.iloc[-1].copy()
            last_row["Interval Start"] = df["Interval End"].max()
            last_row["Interval End"] = current_time.floor("min")
            df = pd.concat([df, pd.DataFrame([last_row])], ignore_index=True)

        start_time = df["Interval Start"].min()
        end_time = pd.Timestamp(end) if end else df["Interval End"].max()

        all_minutes = pd.date_range(
            start=start_time,
            end=end_time,
            freq="1min",
            tz=self.default_timezone,
            inclusive="left",
        )

        result_df = pd.DataFrame({"Interval Start": all_minutes})
        result_df["Interval End"] = result_df["Interval Start"] + pd.Timedelta(
            minutes=1,
        )

        result_df = pd.merge_asof(
            result_df,
            df[["Interval Start", "System Marginal Price", "Volume"]],
            on="Interval Start",
            direction="backward",
        )

        return result_df[
            ["Interval Start", "Interval End", "System Marginal Price", "Volume"]
        ]

    def get_unit_status(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get current unit status data for all assets in the AESO system.

        Returns:
            DataFrame containing unit status data with columns:
            - Time: Timestamp of the data
            - Asset: Asset identifier
            - Fuel Type: Type of fuel used
            - Sub Fuel Type: Sub-category of fuel type
            - Maximum Capability: Maximum generation capability in MW
            - Net Generation: Current net generation in MW
            - Dispatched Contingency Reserve: Amount of contingency reserve dispatched in MW
        """
        if date != "latest":
            raise NotSupported()

        endpoint = "currentsupplydemand-api/v1/csd/generation/assets/current"
        data = self._make_request(endpoint)

        df = pd.json_normalize(
            data["return"],
            record_path="asset_list",
            meta=["last_updated_datetime_utc"],
        )

        df["Time"] = pd.to_datetime(
            df["last_updated_datetime_utc"],
            utc=True,
        ).dt.tz_convert(self.default_timezone)

        df = df.rename(
            columns={
                "asset": "Asset",
                "fuel_type": "Fuel Type",
                "sub_fuel_type": "Sub Fuel Type",
                "maximum_capability": "Maximum Capability",
                "net_generation": "Net Generation",
                "dispatched_contingency_reserve": "Dispatched Contingency Reserve",
            },
        )

        numeric_columns = [
            "Maximum Capability",
            "Net Generation",
            "Dispatched Contingency Reserve",
        ]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df[
            [
                "Time",
                "Asset",
                "Fuel Type",
                "Sub Fuel Type",
                "Maximum Capability",
                "Net Generation",
                "Dispatched Contingency Reserve",
            ]
        ]

    @support_date_range(frequency="31D")
    def get_generator_outages_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get hourly generator outage data.

        Args:
            date: Start date for the data. Can be "latest" to get current data.
            end: End date for the data. If not provided, will get 24 months of data.
            verbose: Whether to print verbose output.

        Returns:
            DataFrame containing generator outage data
        """
        if date == "latest":
            current_time = pd.Timestamp.now(tz=self.default_timezone)
            return self.get_generator_outages_hourly(
                date=current_time,
                end=current_time + pd.DateOffset(months=4),
            )
        else:
            start_date = pd.Timestamp(date)
            if start_date.tz is None:
                start_date = start_date.tz_localize(self.default_timezone)
            else:
                start_date = start_date.tz_convert(self.default_timezone)

            end_date = pd.Timestamp(end) if end else None
            if end_date is not None:
                if end_date.tz is None:
                    end_date = end_date.tz_localize(self.default_timezone)
                else:
                    end_date = end_date.tz_convert(self.default_timezone)

        endpoint = f"aiesgencapacity-api/v1/AIESGenCapacity?startDate={start_date.strftime('%Y-%m-%d')}"
        if end_date:
            endpoint += f"&endDate={end_date.strftime('%Y-%m-%d')}"
        data = self._make_request(endpoint)

        fuel_type_mapping = {
            "GAS": "Gas",
            "COAL": "Coal",
            "HYDRO": "Hydro",
            "WIND": "Wind",
            "SOLAR": "Solar",
            "ENERGY STORAGE": "Energy Storage",
            "OTHER": "Biomass and Other",
        }

        sub_fuel_type_mapping = {
            "SIMPLE_CYCLE": "Simple Cycle",
            "COMBINED_CYCLE": "Combined Cycle",
            "COGENERATION": "Cogeneration",
            "GAS_FIRED_STEAM": "Gas Fired Steam",
            "COAL": "Coal",
            "HYDRO": "Hydro",
            "OTHER": "Biomass and Other",
            "ENERGY STORAGE": "Energy Storage",
            "SOLAR": "Solar",
            "WIND": "Wind",
        }

        rows = []
        for fuel_data in data["return"]:
            fuel_type = fuel_data["fuel_type"]
            sub_fuel_type = fuel_data["sub_fuel_type"]

            mapped_fuel_type = fuel_type_mapping.get(fuel_type, fuel_type)
            mapped_sub_fuel_type = sub_fuel_type_mapping.get(
                sub_fuel_type,
                sub_fuel_type,
            )

            for hour_data in fuel_data["Hours"]:
                interval_start = pd.to_datetime(
                    hour_data["begin_datetime_utc"],
                    utc=True,
                ).tz_convert(self.default_timezone)
                interval_end = interval_start + pd.Timedelta(hours=1)
                outage_grouping = hour_data["outage_grouping"]

                row = {
                    "Interval Start": interval_start,
                    "Interval End": interval_end,
                    "Fuel Type": mapped_fuel_type,
                    "Sub Fuel Type": mapped_sub_fuel_type,
                    "Operating Outage": outage_grouping["OP OUT"],
                    "Mothball Outage": outage_grouping["MBO OUT"],
                }
                rows.append(row)

        df = pd.DataFrame(rows)

        current_time = pd.Timestamp.now(tz=self.default_timezone).floor("h")
        df["Publish Time"] = df.apply(
            lambda row: (
                current_time
                if row["Interval Start"] > current_time
                else row["Interval Start"] - pd.Timedelta(hours=1)
            ),
            axis=1,
        )

        # NB: Pivot the data to get the by-fuel type outage.
        df_pivot = df.pivot_table(
            index=["Interval Start", "Interval End", "Publish Time"],
            columns=["Sub Fuel Type"],
            values="Operating Outage",
            aggfunc="sum",
        ).reset_index()

        # NB: Sum the operational outage by fuel type to get the total outage.
        df_pivot["Total Outage"] = df_pivot[
            [
                col
                for col in df_pivot.columns
                if col
                not in [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "Mothball Outage",
                    "Total Outage",
                ]
            ]
        ].sum(axis=1)

        # NB: Create the summed mothball outage and merge it back in.
        mbo_df = (
            df.groupby(["Interval Start", "Interval End", "Publish Time"])[
                "Mothball Outage"
            ]
            .sum()
            .reset_index()
        )
        df_pivot = pd.merge(
            df_pivot,
            mbo_df,
            on=["Interval Start", "Interval End", "Publish Time"],
            how="left",
        )
        df_pivot["Mothball Outage"] = df_pivot["Mothball Outage"].fillna(0)

        df_pivot = utils.move_cols_to_front(
            df_pivot,
            ["Interval Start", "Interval End", "Publish Time", "Total Outage"],
        )

        # NB: Need to have all columns present, and create them to be 0 if they don't exist
        expected_columns = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Total Outage",
            "Simple Cycle",
            "Combined Cycle",
            "Cogeneration",
            "Gas Fired Steam",
            "Coal",
            "Hydro",
            "Wind",
            "Solar",
            "Energy Storage",
            "Biomass and Other",
            "Mothball Outage",
        ]

        for col in expected_columns:
            if col not in df_pivot.columns:
                df_pivot[col] = 0

        return df_pivot[expected_columns]

    def get_transmission_outages(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
    ) -> pd.DataFrame:
        """
        Get transmission outages data.

        Args:
            date: Start date for the data. Can be "latest" to get current data.
            end: End date for the data. If not provided, will get data for the specified date.

        Returns:
            DataFrame containing transmission outage data
        """
        if date == "latest":
            url = "http://ets.aeso.ca/outage_reports/qryOpPlanTransmissionTable_1.html"
            logger.info("Fetching latest transmission outages data")
            response = requests.get(url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            csv_link = soup.find("a", href=lambda x: x and "csvData" in x)
            csv_href = csv_link["href"].replace("\\", "/")
            csv_url = self._generate_csv_url(csv_href)
            logger.info(f"Found CSV link: {csv_url}")

            publish_match = re.search(
                r"(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})",
                csv_href,
            )
            if publish_match:
                publish_date = publish_match.group(1)
                publish_time = publish_match.group(2).replace("-", ":")
                publish_datetime = pd.to_datetime(f"{publish_date} {publish_time}")
                publish_datetime = publish_datetime.tz_localize(self.default_timezone)

            df = pd.read_csv(csv_url)

            df["Interval Start"] = pd.to_datetime(
                df["From"],
                format="%d-%b-%y %H:%M",
            ).dt.tz_localize(self.default_timezone)
            df["Interval End"] = pd.to_datetime(
                df["To"],
                format="%d-%b-%y %H:%M",
            ).dt.tz_localize(self.default_timezone)
            df["Publish Time"] = publish_datetime

            df = df.rename(
                columns={
                    "Owner": "Transmission Owner",
                    "Date/Time Comments": "Date Time Comments",
                },
            )

            df = df[
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "Transmission Owner",
                    "Type",
                    "Element",
                    "Scheduled Activity",
                    "Date Time Comments",
                    "Interconnection",
                ]
            ]

            return df

        else:
            # NB: For historical data, we need to navigate backwards through Previous Version links
            # as there doesn't seem to be a better way to do it.
            start_date = pd.Timestamp(date)
            end_date = pd.Timestamp(end) if end else start_date
            # NB: Data is only available from 2024-01-31 onwards is seems
            earliest_available = pd.Timestamp("2024-01-31")

            if end_date.date() < earliest_available.date() or (
                start_date.date() < earliest_available.date() and end_date is None
            ):
                raise ValueError(
                    f"Requested date range is before available data. "
                    f"Transmission outage data is only available from {earliest_available.date()} onwards. "
                    f"Requested: {start_date.date()} to {end_date.date()}",
                )
            elif start_date.date() < earliest_available.date():
                logger.warning(
                    f"Requested start date {start_date.date()} is before available data. "
                    f"Data is only available from {earliest_available.date()} onwards. ",
                )

            logger.info(
                f"Fetching historical transmission outages from {start_date.date()} to {end_date.date()}",
            )

            # NB: Start from the latest file and navigate backwards
            current_url = (
                "http://ets.aeso.ca/outage_reports/qryOpPlanTransmissionTable_1.html"
            )
            historical_files = []
            jumped_to_archives = False

            for _ in range(
                self.MAX_NAVIGATION_ATTEMPTS,
            ):  # NB: Limit iterations to prevent infinite loops
                try:
                    response = requests.get(current_url)
                    response.raise_for_status()

                    soup = BeautifulSoup(response.text, "html.parser")

                    csv_link = soup.find("a", href=lambda x: x and "csvData" in x)
                    if csv_link:
                        csv_href = csv_link["href"].replace("\\", "/")

                        publish_match = re.search(
                            r"(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})",
                            csv_href,
                        )
                        if publish_match:
                            publish_date = publish_match.group(1)
                            publish_time = publish_match.group(2).replace("-", ":")
                            publish_datetime = pd.to_datetime(
                                f"{publish_date} {publish_time}",
                            )
                            publish_datetime = publish_datetime.tz_localize(
                                self.default_timezone,
                            )

                            if (
                                start_date.date()
                                <= publish_datetime.date()
                                <= end_date.date()
                            ):
                                csv_url = self._generate_csv_url(csv_href)

                                historical_files.append((csv_url, publish_datetime))
                                logger.info(
                                    f"Found file: {publish_datetime.date()} - {csv_url}",
                                )

                                if end is None:
                                    break

                            if (
                                end is None
                                and publish_datetime.date() < start_date.date()
                            ):
                                csv_url = self._generate_csv_url(csv_href)
                                historical_files.append((csv_url, publish_datetime))
                                logger.info(
                                    f"Found most recent file before {start_date.date()}: {publish_datetime.date()} - {csv_url}",
                                )
                                break

                            if publish_datetime.date() < start_date.date():
                                break

                            if (
                                publish_datetime.date()
                                <= pd.Timestamp("2025-01-22").date()
                                and not jumped_to_archives
                            ):
                                logger.info(
                                    "Reached known broken date range, jumping to 2025-01-17 to continue navigation",
                                )
                                current_url = "http://ets.aeso.ca/outage_reports/archives/_2025-01-17_14-10-25_qryOpPlanTransmissionTable_1.html"
                                jumped_to_archives = True
                                continue

                    prev_link = soup.find(
                        "a",
                        string=lambda text: text and "Previous Version" in text,
                    )
                    if not prev_link:
                        break

                    prev_href = prev_link.get("href")

                    if prev_href.startswith("http"):
                        current_url = prev_href
                    elif prev_href.startswith("file:///"):
                        # NOTE: There's a few that link to an engineer's local file path, so we navigate around that
                        filename = os.path.basename(prev_href)
                        current_url = (
                            f"http://ets.aeso.ca/outage_reports/archives/{filename}"
                        )
                    else:
                        prev_href_clean = prev_href.replace("\\", "/")
                        if prev_href_clean.startswith("archives/"):
                            current_url = (
                                f"http://ets.aeso.ca/outage_reports/{prev_href_clean}"
                            )
                        else:
                            current_url = f"http://ets.aeso.ca/outage_reports/archives/{prev_href_clean}"

                except Exception as e:
                    logger.error(f"Error accessing {current_url}: {e}")
                    break

            if not historical_files:
                raise ValueError("No historical files found")

            logger.info(f"Found {len(historical_files)} historical files to process")
            all_dfs = []
            for csv_url, publish_datetime in historical_files:
                try:
                    df_hist = pd.read_csv(csv_url, on_bad_lines="skip")
                    df_hist["Interval Start"] = pd.to_datetime(
                        df_hist["From"],
                        format="%d-%b-%y %H:%M",
                    ).dt.tz_localize(self.default_timezone)
                    df_hist["Interval End"] = pd.to_datetime(
                        df_hist["To"],
                        format="%d-%b-%y %H:%M",
                    ).dt.tz_localize(self.default_timezone)
                    df_hist["Publish Time"] = publish_datetime

                    df_hist = df_hist.rename(
                        columns={
                            "Owner": "Transmission Owner",
                            "Date/Time Comments": "Date Time Comments",
                        },
                    )

                    df_hist = df_hist[
                        [
                            "Interval Start",
                            "Interval End",
                            "Publish Time",
                            "Transmission Owner",
                            "Type",
                            "Element",
                            "Scheduled Activity",
                            "Date Time Comments",
                            "Interconnection",
                        ]
                    ]

                    all_dfs.append(df_hist)
                except Exception as e:
                    logger.error(f"Error accessing {csv_url}: {e}")
                    continue

            if all_dfs:
                df = pd.concat(all_dfs, ignore_index=True)
                df = df.sort_values("Publish Time", ascending=False).reset_index(
                    drop=True,
                )
                return df

    def _generate_csv_url(self, csv_href: str) -> str:
        csv_href = csv_href.replace("\\", "/")

        if csv_href.startswith("../"):
            return f"http://ets.aeso.ca/outage_reports/{csv_href[3:]}"
        elif csv_href.startswith("csvData/"):
            return f"http://ets.aeso.ca/outage_reports/{csv_href}"
        elif csv_href.startswith("file:///"):
            filename = os.path.basename(csv_href)
            return f"http://ets.aeso.ca/outage_reports/csvData/{filename}"
        else:
            return f"http://ets.aeso.ca/outage_reports/{csv_href}"

    @support_date_range(frequency=None)
    def get_wind_forecast_12_hour(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get 12-hour wind forecast data.

        Returns:
            DataFrame containing 12-hour wind forecast data with min, most likely, and max values
        """
        if date == "latest":
            return self._get_wind_solar_forecast_latest_data(
                forecast_type="wind",
                term="shortterm",
                date=date,
                end=end,
            )
        else:
            raise NotSupported(
                "Historical data is not supported for 12-hour wind forecasts at this time.",
            )

    @support_date_range(frequency=None)
    def get_wind_forecast_7_day(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get 7-day wind forecast data.

        Returns:
            DataFrame containing 7-day wind forecast data with min, most likely, and max values
        """
        if date == "latest":
            return self._get_wind_solar_forecast_latest_data(
                forecast_type="wind",
                term="longterm",
                date=date,
                end=end,
            )
        else:
            if (
                date < self.HISTORICAL_FORECAST_EARLIEST
                or (end if end else date) > self.HISTORICAL_FORECAST_LATEST
            ):
                raise NotSupported(
                    f"Historical wind forecast data is only available from {self.HISTORICAL_FORECAST_EARLIEST.date()} "
                    f"to {self.HISTORICAL_FORECAST_LATEST.date()}. Requested: {date.date()} to {(end if end else date).date()}",
                )

            return self._get_wind_solar_forecast_historical_data(
                forecast_type="wind",
                date=date,
                end=end,
            )

    @support_date_range(frequency=None)
    def get_solar_forecast_12_hour(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get 12-hour solar forecast data.

        Returns:
            DataFrame containing 12-hour solar forecast data with min, most likely, and max values
        """
        if date == "latest":
            return self._get_wind_solar_forecast_latest_data(
                forecast_type="solar",
                term="shortterm",
                date=date,
                end=end,
            )
        else:
            raise NotSupported(
                "Historical data is not supported for 12-hour solar forecasts at this time.",
            )

    @support_date_range(frequency=None)
    def get_solar_forecast_7_day(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get 7-day solar forecast data.

        Returns:
            DataFrame containing 7-day solar forecast data with min, most likely, and max values
        """
        if date == "latest":
            return self._get_wind_solar_forecast_latest_data(
                forecast_type="solar",
                term="longterm",
                date=date,
                end=end,
            )
        else:
            if (
                date < self.HISTORICAL_FORECAST_EARLIEST
                or (end if end else date) > self.HISTORICAL_FORECAST_LATEST
            ):
                raise NotSupported(
                    f"Historical solar forecast data is only available from {self.HISTORICAL_FORECAST_EARLIEST.date()} "
                    f"to {self.HISTORICAL_FORECAST_LATEST.date()}. Requested: {date.date()} to {(end if end else date).date()}",
                )

            return self._get_wind_solar_forecast_historical_data(
                forecast_type="solar",
                date=date,
                end=end,
            )

    def _get_wind_solar_forecast_latest_data(
        self,
        forecast_type: Literal["wind", "solar"],
        term: Literal["shortterm", "longterm"],
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
    ) -> pd.DataFrame:
        """
        Get wind or solar forecast data from AESO CSV reports.

        Args:
            forecast_type: Type of forecast ("wind" or "solar")
            term: Forecast term ("shortterm" for 12-hour or "longterm" for 7-day)
            date: Start date for filtering data
            end: End date for filtering data

        Returns:
            DataFrame containing forecast data
        """
        url = f"http://ets.aeso.ca/Market/Reports/Manual/Operations/prodweb_reports/wind_solar_forecast/{forecast_type}_rpt_{term}.csv"

        try:
            df = pd.read_csv(url)
        except Exception as e:
            raise RequestException(
                f"Failed to fetch {forecast_type} forecast data: {str(e)}",
            )

        df["Interval Start"] = pd.to_datetime(
            df["Forecast Transaction Date"],
            format="%Y-%m-%d %H:%M",
        ).dt.tz_localize(self.default_timezone)

        interval_length = (
            pd.Timedelta(minutes=10) if term == "shortterm" else pd.Timedelta(hours=1)
        )
        df["Interval End"] = df["Interval Start"] + interval_length

        # NB: Since the forecasts are published every 10 minutes for shortterm and every 1 hour for longterm,
        # we can calculate the publish time based on the presence of actuals values.
        # For past forecasted intervals (intervals with an actual value), we know the most recent forecast was published just before each interval.
        # For future forecasted values, the publish time for all of them is set to the first interval that has no actual value, since
        # that's when the forecast was last published.
        first_interval_without_actual = df[df["Actual"].isna()]
        if not first_interval_without_actual.empty:
            forecast_publish_time = first_interval_without_actual[
                "Interval Start"
            ].min()
            df["Publish Time"] = df.apply(
                lambda row: (
                    forecast_publish_time
                    if pd.isna(row["Actual"])
                    else row["Interval Start"] - interval_length
                ),
                axis=1,
            )
        else:
            df["Publish Time"] = df["Interval Start"] - interval_length

        df = df.rename(
            columns={
                "Min": "Minimum Generation Forecast",
                "Most Likely": "Most Likely Generation Forecast",
                "Max": "Maximum Generation Forecast",
                "MCR": f"Total {forecast_type.capitalize()} Capacity",
                "Pct Min": "Minimum Generation Percentage",
                "Pct Most Likely": "Most Likely Generation Percentage",
                "Pct Max": "Maximum Generation Percentage",
            },
        )

        numeric_columns = [
            "Minimum Generation Forecast",
            "Most Likely Generation Forecast",
            "Maximum Generation Forecast",
            f"Total {forecast_type.capitalize()} Capacity",
            "Minimum Generation Percentage",
            "Most Likely Generation Percentage",
            "Maximum Generation Percentage",
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df.sort_values("Interval Start").reset_index(drop=True)
        return df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Minimum Generation Forecast",
                "Most Likely Generation Forecast",
                "Maximum Generation Forecast",
                f"Total {forecast_type.capitalize()} Capacity",
                "Minimum Generation Percentage",
                "Most Likely Generation Percentage",
                "Maximum Generation Percentage",
            ]
        ]

    def _get_wind_solar_forecast_historical_data(
        self,
        forecast_type: Literal["wind", "solar"],
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
    ) -> pd.DataFrame:
        """
        Get historical wind or solar forecast data from AESO CSV archives.

        Args:
            forecast_type: Type of forecast ("wind" or "solar")
            date: Start date for filtering data
            end: End date for filtering data

        Returns:
            DataFrame containing historical forecast data
        """
        url = f"https://www.aeso.ca/assets/{forecast_type.upper()}_GEN_MAR_2023-MAR_2025-Day-ahead.csv"

        try:
            df = pd.read_csv(url)
        except Exception as e:
            raise RequestException(
                f"Failed to fetch historical {forecast_type} forecast data: {str(e)}",
            )

        forecast_prefix = forecast_type.upper()
        df["Interval Start"] = pd.to_datetime(
            df["FORECAST_DATE_MPT"],
            format="mixed",
        ).dt.tz_localize(self.default_timezone, ambiguous="infer")

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)
        df["Publish Time"] = df["Interval Start"] - pd.Timedelta(hours=24)

        if end:
            df = df[(df["Interval Start"] >= date) & (df["Interval Start"] <= end)]
        else:
            df = df[df["Interval Start"] >= date]

        df = df.rename(
            columns={
                f"{forecast_prefix}_MIN": "Minimum Generation Forecast",
                f"{forecast_prefix}_OPT": "Most Likely Generation Forecast",
                f"{forecast_prefix}_MAX": "Maximum Generation Forecast",
                f"{forecast_prefix}_MCR": f"Total {forecast_type.capitalize()} Capacity",
            },
        )

        numeric_columns = [
            "Minimum Generation Forecast",
            "Most Likely Generation Forecast",
            "Maximum Generation Forecast",
            f"Total {forecast_type.capitalize()} Capacity",
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        capacity_col = f"Total {forecast_type.capitalize()} Capacity"
        df["Minimum Generation Percentage"] = (
            df["Minimum Generation Forecast"] / df[capacity_col]
        ) * 100
        df["Most Likely Generation Percentage"] = (
            df["Most Likely Generation Forecast"] / df[capacity_col]
        ) * 100
        df["Maximum Generation Percentage"] = (
            df["Maximum Generation Forecast"] / df[capacity_col]
        ) * 100

        return (
            df[
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "Minimum Generation Forecast",
                    "Most Likely Generation Forecast",
                    "Maximum Generation Forecast",
                    f"Total {forecast_type.capitalize()} Capacity",
                    "Minimum Generation Percentage",
                    "Most Likely Generation Percentage",
                    "Maximum Generation Percentage",
                ]
            ]
            .sort_values("Interval Start")
            .reset_index(drop=True)
        )

    @support_date_range(frequency=None)
    def get_wind_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get actual wind generation data with hourly intervals.

        Returns:
            DataFrame containing actual wind generation data with hourly intervals
        """
        if date == "latest":
            return self._get_wind_solar_actual_latest_data(
                generation_type="wind",
                forecast_type="longterm",
            )
        else:
            if (
                date < self.HISTORICAL_FORECAST_EARLIEST
                or (end if end else date) > self.HISTORICAL_FORECAST_LATEST
            ):
                raise NotSupported(
                    f"Historical wind generation data is only available from {self.HISTORICAL_FORECAST_EARLIEST.date()} "
                    f"to {self.HISTORICAL_FORECAST_LATEST.date()}. Requested: {date.date()} to {(end if end else date).date()}",
                )

            return self._get_wind_solar_actual_historical_data(
                generation_type="wind",
                date=date,
                end=end,
            )

    @support_date_range(frequency=None)
    def get_solar_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get actual solar generation data with hourly intervals.

        Returns:
            DataFrame containing actual solar generation data with hourly intervals
        """
        if date == "latest":
            return self._get_wind_solar_actual_latest_data(
                generation_type="solar",
                forecast_type="longterm",
            )
        else:
            if (
                date < self.HISTORICAL_FORECAST_EARLIEST
                or (end if end else date) > self.HISTORICAL_FORECAST_LATEST
            ):
                raise NotSupported(
                    f"Historical solar generation data is only available from {self.HISTORICAL_FORECAST_EARLIEST.date()} "
                    f"to {self.HISTORICAL_FORECAST_LATEST.date()}. Requested: {date.date()} to {(end if end else date).date()}",
                )

            return self._get_wind_solar_actual_historical_data(
                generation_type="solar",
                date=date,
                end=end,
            )

    @support_date_range(frequency=None)
    def get_wind_10_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get actual wind generation data with 10-minute intervals.

        Returns:
            DataFrame containing actual wind generation data with 10-minute intervals
        """
        if date == "latest":
            return self._get_wind_solar_actual_latest_data(
                generation_type="wind",
                forecast_type="shortterm",
            )
        else:
            raise NotSupported(
                "Historical data is not supported for 10-minute wind generation. Use get_wind_hourly for historical data.",
            )

    @support_date_range(frequency=None)
    def get_solar_10_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get actual solar generation data with 10-minute intervals.

        Returns:
            DataFrame containing actual solar generation data with 10-minute intervals
        """
        if date == "latest":
            return self._get_wind_solar_actual_latest_data(
                generation_type="solar",
                forecast_type="shortterm",
            )
        else:
            raise NotSupported(
                "Historical data is not supported for 10-minute solar generation. Use get_solar_hourly for historical data.",
            )

    def _get_wind_solar_actual_latest_data(
        self,
        generation_type: Literal["wind", "solar"],
        forecast_type: Literal["shortterm", "longterm"] = "shortterm",
    ) -> pd.DataFrame:
        """
        Get actual wind or solar generation data from AESO CSV reports with 10-minute or hourly intervals.

        Args:
            generation_type: Type of generation ("wind" or "solar")
            forecast_type: Type of forecast ("shortterm" for 10-minute or "longterm" for hourly)

        Returns:
            DataFrame containing actual generation data with 10-minute or hourly intervals
        """
        url = f"http://ets.aeso.ca/Market/Reports/Manual/Operations/prodweb_reports/wind_solar_forecast/{generation_type}_rpt_{forecast_type}.csv"

        try:
            df = pd.read_csv(url)
        except Exception as e:
            raise RequestException(
                f"Failed to fetch {generation_type} generation data: {str(e)}",
            )

        df["Interval Start"] = pd.to_datetime(
            df["Forecast Transaction Date"],
            format="%Y-%m-%d %H:%M",
        ).dt.tz_localize(self.default_timezone)

        if forecast_type == "shortterm":
            df["Interval End"] = df["Interval Start"] + pd.Timedelta(minutes=10)
        else:
            df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)

        # NB: Only include rows with actual generation data
        df = df[df["Actual"].notna()].copy()

        df = df.rename(
            columns={
                "Actual": "Actual Generation",
                "MCR": f"Total {generation_type.capitalize()} Capacity",
            },
        )

        numeric_columns = [
            "Actual Generation",
            f"Total {generation_type.capitalize()} Capacity",
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return (
            df[
                [
                    "Interval Start",
                    "Interval End",
                    "Actual Generation",
                    f"Total {generation_type.capitalize()} Capacity",
                ]
            ]
            .sort_values("Interval Start")
            .reset_index(drop=True)
        )

    def _get_wind_solar_actual_historical_data(
        self,
        generation_type: Literal["wind", "solar"],
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
    ) -> pd.DataFrame:
        """
        Get historical wind or solar generation data from AESO CSV archives with hourly intervals.

        Args:
            generation_type: Type of generation ("wind" or "solar")
            date: Start date for filtering data
            end: End date for filtering data

        Returns:
            DataFrame containing historical generation data with hourly intervals
        """
        url = f"https://www.aeso.ca/assets/{generation_type.upper()}_GEN_MAR_2023-MAR_2025-Day-ahead.csv"

        try:
            df = pd.read_csv(url)
        except Exception as e:
            raise RequestException(
                f"Failed to fetch historical {generation_type} generation data: {str(e)}",
            )

        generation_prefix = generation_type.upper()
        df["Interval Start"] = pd.to_datetime(
            df["FORECAST_DATE_MPT"],
            format="mixed",
        ).dt.tz_localize(self.default_timezone, ambiguous="infer")

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)

        if end:
            df = df[(df["Interval Start"] >= date) & (df["Interval Start"] <= end)]
        else:
            df = df[df["Interval Start"] >= date]

        df = df.rename(
            columns={
                f"{generation_prefix}_ACTUAL": "Actual Generation",
                f"{generation_prefix}_MCR": f"Total {generation_type.capitalize()} Capacity",
            },
        )

        numeric_columns = [
            "Actual Generation",
            f"Total {generation_type.capitalize()} Capacity",
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return (
            df[
                [
                    "Interval Start",
                    "Interval End",
                    "Actual Generation",
                    f"Total {generation_type.capitalize()} Capacity",
                ]
            ]
            .sort_values("Interval Start")
            .reset_index(drop=True)
        )
