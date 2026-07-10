import json
import os
import re
from typing import Any, Literal

import pandas as pd
import polars as pl
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

    # Default timeout for API requests: (connect_timeout, read_timeout) in seconds
    REQUEST_TIMEOUT = (10, 120)

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
                timeout=self.REQUEST_TIMEOUT,
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
    ) -> pl.DataFrame:
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

        df = pl.DataFrame(data["return"]["Actual Forecast Report"])
        df = df.with_columns(
            pl.col("begin_datetime_utc")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(hours=1)).alias("Interval End"),
        )
        df = df.rename({"alberta_internal_load": "Load"})
        df = df.with_columns(pl.col("Load").cast(pl.Float64, strict=False))
        df = df.drop_nulls(subset=["Load"])
        return df.select(["Interval Start", "Interval End", "Load"])

    @support_date_range(frequency=None)
    def get_load_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get load forecast data.

        The AESO publishes load forecasts daily at 7am Mountain Time. The forecast covers
        the next 13 days. The publish time is determined as follows:

        - For historical data: 7am on the day of the interval if interval is after 7am,
          otherwise 7am the previous day
        - For future data: 7am today (if after 7am) or 7am yesterday (if before 7am)

        Returns:
            DataFrame containing load forecast data with publish times.
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
            df = df.filter(pl.col("Publish Time") == pl.lit(publish_time))
            return df

        start_date = pd.Timestamp(date).strftime("%Y-%m-%d")
        end_date = pd.Timestamp(end).strftime("%Y-%m-%d") if end else None

        endpoint = (
            f"actualforecast-api/v1/load/albertaInternalLoad?startDate={start_date}"
        )
        if end_date:
            endpoint += f"&endDate={end_date}"

        data = self._make_request(endpoint)
        df = pl.DataFrame(data["return"]["Actual Forecast Report"])
        df = df.with_columns(
            pl.col("begin_datetime_utc")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(hours=1)).alias("Interval End"),
        )
        df = df.rename(
            {
                "alberta_internal_load": "Load",
                "forecast_alberta_internal_load": "Load Forecast",
            },
        )

        current_time = pd.Timestamp.now(tz=self.default_timezone)
        today_7am = current_time.floor("D") + pd.Timedelta(hours=7)
        future_publish_time = (
            today_7am if current_time >= today_7am else today_7am - pd.Timedelta(days=1)
        )
        interval_day_7am = pl.col("Interval Start").dt.truncate("1d") + pl.duration(
            hours=7,
        )
        df = df.with_columns(
            pl.when(pl.col("Interval Start") > pl.lit(current_time))
            .then(pl.lit(future_publish_time))
            .when(pl.col("Interval Start") >= interval_day_7am)
            .then(interval_day_7am)
            .otherwise(interval_day_7am - pl.duration(days=1))
            .alias("Publish Time"),
        )
        df = df.with_columns(
            pl.col("Load").cast(pl.Float64, strict=False),
            pl.col("Load Forecast").cast(pl.Float64, strict=False),
        )
        return df.select(
            ["Interval Start", "Interval End", "Publish Time", "Load", "Load Forecast"],
        )

    def get_supply_and_demand(self) -> pl.DataFrame:
        """
        Get current supply and demand summary data.

        Returns:
            DataFrame containing current supply and demand information
        """
        endpoint = "currentsupplydemand-api/v2/csd/summary/current"
        data = self._make_request(endpoint)

        return_data = data["return"]
        exclude_keys = {"generation_data_list", "interchange_list"}
        flat = {k: v for k, v in return_data.items() if k not in exclude_keys}
        df = pl.DataFrame([flat])
        df = df.with_columns(
            pl.col("effective_datetime_utc")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Time"),
        )

        rename = {
            k: v for k, v in SUPPLY_DEMAND_COLUMN_MAPPING.items() if k in df.columns
        }
        df = df.rename(rename)

        if "generation_data_list" in return_data:
            gen_df = pl.DataFrame(return_data["generation_data_list"])
            for row in gen_df.iter_rows(named=True):
                fuel_type = row["fuel_type"].title().replace(" ", " ")
                df = df.with_columns(
                    pl.lit(row["aggregated_maximum_capability"]).alias(
                        f"{fuel_type} Maximum Capability",
                    ),
                    pl.lit(row["aggregated_net_generation"]).alias(
                        f"{fuel_type} Net Generation",
                    ),
                    pl.lit(row["aggregated_dispatched_contingency_reserve"]).alias(
                        f"{fuel_type} Dispatched Contingency Reserve",
                    ),
                )

        if "interchange_list" in return_data:
            for interchange in return_data["interchange_list"]:
                path = interchange["path"].title().replace(" ", " ")
                df = df.with_columns(
                    pl.lit(interchange["actual_flow"]).alias(f"{path} Flow"),
                )

        df = df.select(list(SUPPLY_DEMAND_COLUMN_MAPPING.values()))
        return utils.move_cols_to_front(df, ["Time"])

    def get_fuel_mix(self) -> pl.DataFrame:
        """
        Get current generation by fuel type.

        Returns:
            DataFrame containing generation data by fuel type, with each fuel type as a column
            containing its net generation value
        """
        endpoint = "currentsupplydemand-api/v2/csd/summary/current"
        data = self._make_request(endpoint)

        effective_datetime_utc = data["return"]["effective_datetime_utc"]
        records = [
            {**item, "effective_datetime_utc": effective_datetime_utc}
            for item in data["return"]["generation_data_list"]
        ]
        df = pl.DataFrame(records)
        df = df.with_columns(
            pl.col("effective_datetime_utc")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Time"),
            pl.col("fuel_type").str.to_titlecase().alias("fuel_type"),
        )

        result_df = df.pivot(
            "fuel_type",
            index="Time",
            values="aggregated_net_generation",
            aggregate_function="first",
        )

        return result_df

    def get_interchange(self) -> pl.DataFrame:
        """
        Get current interchange flows with neighboring regions.

        Returns:
            DataFrame containing interchange data with separate columns for each region's flow
            and a net interchange flow column
        """
        endpoint = "currentsupplydemand-api/v2/csd/summary/current"
        data = self._make_request(endpoint)

        effective_datetime_utc = data["return"]["effective_datetime_utc"]
        records = [
            {**item, "effective_datetime_utc": effective_datetime_utc}
            for item in data["return"]["interchange_list"]
        ]
        df = pl.DataFrame(records)
        df = df.with_columns(
            pl.col("effective_datetime_utc")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Time"),
        )

        df = df.pivot(
            "path",
            index="Time",
            values="actual_flow",
            aggregate_function="first",
        )

        df = df.rename(
            {
                "British Columbia": "British Columbia Flow",
                "Montana": "Montana Flow",
                "Saskatchewan": "Saskatchewan Flow",
            },
        )

        flow_columns = [col for col in df.columns if col != "Time"]
        df = df.with_columns(
            pl.sum_horizontal([pl.col(col) for col in flow_columns]).alias(
                "Net Interchange Flow",
            ),
        )

        cols = ["Time", "Net Interchange Flow"] + flow_columns
        return df.select(cols)

    def get_reserves(self) -> pl.DataFrame:
        """
        Get current reserve data.

        Returns:
            DataFrame containing reserve information
        """
        endpoint = "currentsupplydemand-api/v2/csd/summary/current"
        data = self._make_request(endpoint)

        df = pl.DataFrame([data["return"]])
        df = df.with_columns(
            pl.col("effective_datetime_utc")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Time"),
        )

        rename = {k: v for k, v in RESERVES_COLUMN_MAPPING.items() if k in df.columns}
        df = df.rename(rename)
        df = df.select(list(RESERVES_COLUMN_MAPPING.values()))

        return utils.move_cols_to_front(df, ["Time"])

    # TODO(kladar): WIP until asset datasets which are next
    def get_asset_list(
        self,
        asset_id: str | None = None,
        pool_participant_id: str | None = None,
        operating_status: str | None = None,
        asset_type: str | None = None,
    ) -> pl.DataFrame:
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
        df = pl.DataFrame(data["return"])

        if df.is_empty():
            return pl.DataFrame(
                schema=dict.fromkeys(ASSET_LIST_COLUMN_MAPPING.values(), pl.String),
            )

        df = df.rename(ASSET_LIST_COLUMN_MAPPING)
        return df.select(list(ASSET_LIST_COLUMN_MAPPING.values()))

    @support_date_range(frequency=None)
    def get_pool_price(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
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
    ) -> pl.DataFrame:
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
    ) -> pl.DataFrame:
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
        df = pl.DataFrame(data["return"]["Pool Price Report"])
        df = df.with_columns(
            pl.col("begin_datetime_utc")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(hours=1)).alias("Interval End"),
        )
        df = df.rename(
            {
                "pool_price": "Pool Price",
                "forecast_pool_price": "Forecast Pool Price",
                "rolling_30day_avg": "Rolling 30 Day Average Pool Price",
            },
        )

        if actual_or_forecast == "actual":
            df = df.with_columns(pl.col("Pool Price").cast(pl.Float64, strict=False))
            df = df.filter(pl.col("Pool Price").is_not_null())
            return df.select(
                [
                    "Interval Start",
                    "Interval End",
                    "Pool Price",
                    "Rolling 30 Day Average Pool Price",
                ],
            )
        else:
            df = df.with_columns(
                pl.col("Forecast Pool Price").cast(pl.Float64, strict=False),
            )
            # NB: Publish times are a bit opaque from AESO, so we calculate a best estimate here.
            # Forecast pool price is provided for current and next two hours, updated every 5 minutes.
            # For future intervals: use request time floored to 5 minutes
            # For past/current intervals: use 5 minutes before interval start
            request_time = pd.Timestamp.now(tz=self.default_timezone)
            df = df.with_columns(
                pl.when(pl.col("Interval Start") > pl.lit(request_time))
                .then(pl.lit(request_time.floor("5min")))
                .otherwise(pl.col("Interval Start") - pl.duration(minutes=5))
                .alias("Publish Time"),
            )
            return df.select(
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "Forecast Pool Price",
                ],
            )

    @support_date_range(frequency=None)
    def get_daily_average_pool_price(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
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
        hourly_df = hourly_df.with_columns(
            pl.col("Interval End")
            .dt.hour()
            .is_in(list(range(8, 24)))
            .alias(
                "Is On Peak",
            ),
            pl.col("Pool Price").cast(pl.Float64, strict=False),
            pl.col("Rolling 30 Day Average Pool Price").cast(pl.Float64, strict=False),
        )

        daily_df = hourly_df.group_by(
            pl.col("Interval Start").dt.date().alias("date"),
        ).agg(
            pl.col("Pool Price").mean().alias("Daily Average"),
        )

        daily_30day_avg = (
            hourly_df.filter(pl.col("Interval Start").dt.hour() == 23)
            .group_by(pl.col("Interval Start").dt.date().alias("date"))
            .agg(
                pl.col("Rolling 30 Day Average Pool Price")
                .first()
                .alias("30 Day Average"),
            )
        )

        daily_on_peak = (
            hourly_df.filter(pl.col("Is On Peak"))
            .group_by(pl.col("Interval Start").dt.date().alias("date"))
            .agg(pl.col("Pool Price").mean().alias("Daily On Peak Average"))
        )

        daily_off_peak = (
            hourly_df.filter(~pl.col("Is On Peak"))
            .group_by(pl.col("Interval Start").dt.date().alias("date"))
            .agg(pl.col("Pool Price").mean().alias("Daily Off Peak Average"))
        )

        result_df = daily_df.join(daily_on_peak, on="date", how="left")
        result_df = result_df.join(daily_off_peak, on="date", how="left")
        result_df = result_df.join(daily_30day_avg, on="date", how="left")

        result_df = result_df.with_columns(
            pl.col("date")
            .cast(pl.Datetime)
            .dt.replace_time_zone(self.default_timezone)
            .alias("Interval Start"),
        )
        result_df = result_df.with_columns(
            (pl.col("Interval Start") + pl.duration(days=1)).alias("Interval End"),
        )

        price_columns = [
            "Daily Average",
            "Daily On Peak Average",
            "Daily Off Peak Average",
            "30 Day Average",
        ]
        round_exprs = [
            pl.col(col).round(2) for col in price_columns if col in result_df.columns
        ]
        result_df = result_df.with_columns(round_exprs)

        return result_df.select(
            [
                "Interval Start",
                "Interval End",
                "Daily Average",
                "Daily On Peak Average",
                "Daily Off Peak Average",
                "30 Day Average",
            ],
        ).sort("Interval Start")

    @support_date_range(frequency=None)
    def get_system_marginal_price(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
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
        df = pl.DataFrame(data["return"]["System Marginal Price Report"])

        df = df.with_columns(
            pl.col("begin_datetime_utc")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Interval Start"),
            pl.col("end_datetime_utc")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Interval End"),
        )

        # NB: The latest value that the AESO API returns always has an interval that ends at 23:59 of the current day.
        # We want to set the interval end to 1 minute after the interval start to make it consistent with the other intervals
        # and make the forward filling easier.
        # Example: If the latest interval start is 2025-06-12 23:00:00,
        # the interval end will be changed from 2025-06-12 23:59:00 to 2025-06-12 23:01:00.
        df = df.with_columns(
            pl.when(pl.col("Interval End").dt.strftime("%H:%M") == "23:59")
            .then(pl.col("Interval Start") + pl.duration(minutes=1))
            .otherwise(pl.col("Interval End"))
            .alias("Interval End"),
        )

        df = df.rename(
            {
                "system_marginal_price": "System Marginal Price",
                "volume": "Volume",
            },
        )
        df = df.with_columns(
            pl.col("System Marginal Price").cast(pl.Float64, strict=False),
            pl.col("Volume").cast(pl.Float64, strict=False),
        )
        df = df.sort("Interval Start")

        # NB: Add current time as final interval if needed, but only for today's data
        current_time = pd.Timestamp.now(tz=self.default_timezone)
        today_start = current_time.floor("D")
        min_start = pd.Timestamp(df.select(pl.col("Interval Start").min()).item())
        max_end = pd.Timestamp(df.select(pl.col("Interval End").max()).item())
        if min_start.floor("D") == today_start and max_end < current_time:
            last_row = df.tail(1).with_columns(
                pl.lit(max_end).alias("Interval Start"),
                pl.lit(current_time.floor("min")).alias("Interval End"),
            )
            df = pl.concat([df, last_row])

        start_time = pd.Timestamp(df.select(pl.col("Interval Start").min()).item())
        end_time = (
            pd.Timestamp(end)
            if end
            else pd.Timestamp(
                df.select(pl.col("Interval End").max()).item(),
            )
        )

        all_minutes = pl.datetime_range(
            start=start_time,
            end=end_time,
            interval="1m",
            time_zone=self.default_timezone,
            closed="left",
            eager=True,
        )

        result_df = pl.DataFrame({"Interval Start": all_minutes})
        result_df = result_df.with_columns(
            (pl.col("Interval Start") + pl.duration(minutes=1)).alias("Interval End"),
        )

        result_df = result_df.sort("Interval Start").join_asof(
            df.select(["Interval Start", "System Marginal Price", "Volume"]).sort(
                "Interval Start",
            ),
            on="Interval Start",
            strategy="backward",
        )

        return result_df.select(
            ["Interval Start", "Interval End", "System Marginal Price", "Volume"],
        )

    def get_unit_status(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get current unit status data for all assets in the AESO system.

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

        last_updated = data["return"]["last_updated_datetime_utc"]
        records = [
            {**item, "last_updated_datetime_utc": last_updated}
            for item in data["return"]["asset_list"]
        ]
        df = pl.DataFrame(records)

        df = df.with_columns(
            pl.col("last_updated_datetime_utc")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Time"),
        )

        df = df.rename(
            {
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
        df = df.with_columns(
            [pl.col(col).cast(pl.Float64, strict=False) for col in numeric_columns],
        )

        return df.select(
            [
                "Time",
                "Asset",
                "Fuel Type",
                "Sub Fuel Type",
                "Maximum Capability",
                "Net Generation",
                "Dispatched Contingency Reserve",
            ],
        )

    @support_date_range(frequency="31D")
    def get_generator_outages_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
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

        df = pl.DataFrame(rows)

        current_time = pd.Timestamp.now(tz=self.default_timezone).floor("h")
        df = df.with_columns(
            pl.when(pl.col("Interval Start") > pl.lit(current_time))
            .then(pl.lit(current_time))
            .otherwise(pl.col("Interval Start") - pl.duration(hours=1))
            .alias("Publish Time"),
        )

        # NB: Pivot the data to get the by-fuel type outage.
        df_pivot = df.pivot(
            "Sub Fuel Type",
            index=["Interval Start", "Interval End", "Publish Time"],
            values="Operating Outage",
            aggregate_function="sum",
        )

        # NB: Sum the operational outage by fuel type to get the total outage.
        pivot_value_cols = [
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
        df_pivot = df_pivot.with_columns(
            pl.sum_horizontal([pl.col(col) for col in pivot_value_cols]).alias(
                "Total Outage",
            ),
        )

        # NB: Create the summed mothball outage and merge it back in.
        mbo_df = df.group_by(["Interval Start", "Interval End", "Publish Time"]).agg(
            pl.col("Mothball Outage").sum().alias("Mothball Outage"),
        )
        df_pivot = df_pivot.join(
            mbo_df,
            on=["Interval Start", "Interval End", "Publish Time"],
            how="left",
        )
        df_pivot = df_pivot.with_columns(
            pl.col("Mothball Outage").fill_null(0),
        )

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
                df_pivot = df_pivot.with_columns(pl.lit(0).alias(col))

        return df_pivot.select(expected_columns)

    def _format_transmission_outages(
        self,
        df: pl.DataFrame,
        publish_datetime: pd.Timestamp,
    ) -> pl.DataFrame:
        df = df.with_columns(
            pl.col("From")
            .str.to_datetime(format="%d-%b-%y %H:%M", strict=False)
            .dt.replace_time_zone(self.default_timezone)
            .alias("Interval Start"),
            pl.col("To")
            .str.to_datetime(format="%d-%b-%y %H:%M", strict=False)
            .dt.replace_time_zone(self.default_timezone)
            .alias("Interval End"),
            pl.lit(publish_datetime).alias("Publish Time"),
        )
        df = df.rename(
            {
                "Owner": "Transmission Owner",
                "Date/Time Comments": "Date Time Comments",
            },
        )
        return df.select(
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
            ],
        )

    def get_transmission_outages(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
    ) -> pl.DataFrame:
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

            df = pl.read_csv(csv_url)
            return self._format_transmission_outages(df, publish_datetime)

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
                    df_hist = utils.read_csv_exotic_via_pandas(
                        csv_url,
                        on_bad_lines="skip",
                    )
                    all_dfs.append(
                        self._format_transmission_outages(df_hist, publish_datetime),
                    )
                except Exception as e:
                    logger.error(f"Error accessing {csv_url}: {e}")
                    continue

            if all_dfs:
                return utils.concat_dataframes(all_dfs).sort(
                    "Publish Time",
                    descending=True,
                )

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
    ) -> pl.DataFrame:
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
    ) -> pl.DataFrame:
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
    ) -> pl.DataFrame:
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
    ) -> pl.DataFrame:
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
    ) -> pl.DataFrame:
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
            df = pl.read_csv(url)
        except Exception as e:
            raise RequestException(
                f"Failed to fetch {forecast_type} forecast data: {str(e)}",
            )

        df = df.with_columns(
            pl.col("Forecast Transaction Date")
            .str.to_datetime(format="%Y-%m-%d %H:%M", strict=False)
            .alias("Interval Start"),
        )
        df = utils.localize_ambiguous_infer_polars(
            df,
            "Interval Start",
            self.default_timezone,
        )

        interval_duration = (
            pl.duration(minutes=10) if term == "shortterm" else pl.duration(hours=1)
        )
        df = df.with_columns(
            (pl.col("Interval Start") + interval_duration).alias("Interval End"),
        )

        # NB: Since the forecasts are published every 10 minutes for shortterm and every 1 hour for longterm,
        # we can calculate the publish time based on the presence of actuals values.
        # For past forecasted intervals (intervals with an actual value), we know the most recent forecast was published just before each interval.
        # For future forecasted values, the publish time for all of them is set to the first interval that has no actual value, since
        # that's when the forecast was last published.
        first_interval_without_actual = df.filter(pl.col("Actual").is_null())
        if first_interval_without_actual.height > 0:
            forecast_publish_time = first_interval_without_actual.select(
                pl.col("Interval Start").min(),
            ).item()
            df = df.with_columns(
                pl.when(pl.col("Actual").is_null())
                .then(pl.lit(forecast_publish_time))
                .otherwise(pl.col("Interval Start") - interval_duration)
                .alias("Publish Time"),
            )
        else:
            df = df.with_columns(
                (pl.col("Interval Start") - interval_duration).alias("Publish Time"),
            )

        df = df.rename(
            {
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

        cast_exprs = [
            pl.col(col).cast(pl.Float64, strict=False)
            for col in numeric_columns
            if col in df.columns
        ]
        df = df.with_columns(cast_exprs)
        return df.select(
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
            ],
        ).sort("Interval Start")

    def _get_wind_solar_forecast_historical_data(
        self,
        forecast_type: Literal["wind", "solar"],
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
    ) -> pl.DataFrame:
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
            pdf = pd.read_csv(url)
        except Exception as e:
            raise RequestException(
                f"Failed to fetch historical {forecast_type} forecast data: {str(e)}",
            )

        pdf["Interval Start"] = pd.to_datetime(
            pdf["FORECAST_DATE_MPT"],
            format="mixed",
        )
        df = pl.from_pandas(pdf)
        df = utils.localize_ambiguous_infer_polars(
            df,
            "Interval Start",
            self.default_timezone,
        )

        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(hours=1)).alias("Interval End"),
            (pl.col("Interval Start") - pl.duration(hours=24)).alias("Publish Time"),
        )

        if end:
            df = df.filter(
                (pl.col("Interval Start") >= pl.lit(date))
                & (pl.col("Interval Start") <= pl.lit(end)),
            )
        else:
            df = df.filter(pl.col("Interval Start") >= pl.lit(date))

        forecast_prefix = forecast_type.upper()
        df = df.rename(
            {
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

        cast_exprs = [
            pl.col(col).cast(pl.Float64, strict=False)
            for col in numeric_columns
            if col in df.columns
        ]
        df = df.with_columns(cast_exprs)

        capacity_col = f"Total {forecast_type.capitalize()} Capacity"
        df = df.with_columns(
            (pl.col("Minimum Generation Forecast") / pl.col(capacity_col) * 100).alias(
                "Minimum Generation Percentage",
            ),
            (
                pl.col("Most Likely Generation Forecast") / pl.col(capacity_col) * 100
            ).alias("Most Likely Generation Percentage"),
            (pl.col("Maximum Generation Forecast") / pl.col(capacity_col) * 100).alias(
                "Maximum Generation Percentage",
            ),
        )

        return df.select(
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
            ],
        ).sort("Interval Start")

    @support_date_range(frequency=None)
    def get_wind_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
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
    ) -> pl.DataFrame:
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
    ) -> pl.DataFrame:
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
    ) -> pl.DataFrame:
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
    ) -> pl.DataFrame:
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
            df = pl.read_csv(url)
        except Exception as e:
            raise RequestException(
                f"Failed to fetch {generation_type} generation data: {str(e)}",
            )

        df = df.with_columns(
            pl.col("Forecast Transaction Date")
            .str.to_datetime(format="%Y-%m-%d %H:%M", strict=False)
            .alias("Interval Start"),
        )
        df = utils.localize_ambiguous_infer_polars(
            df,
            "Interval Start",
            self.default_timezone,
        )

        interval_duration = (
            pl.duration(minutes=10)
            if forecast_type == "shortterm"
            else pl.duration(hours=1)
        )
        df = df.with_columns(
            (pl.col("Interval Start") + interval_duration).alias("Interval End"),
        )

        # NB: Only include rows with actual generation data
        df = df.filter(pl.col("Actual").is_not_null())

        df = df.rename(
            {
                "Actual": "Actual Generation",
                "MCR": f"Total {generation_type.capitalize()} Capacity",
            },
        )

        numeric_columns = [
            "Actual Generation",
            f"Total {generation_type.capitalize()} Capacity",
        ]

        cast_exprs = [
            pl.col(col).cast(pl.Float64, strict=False)
            for col in numeric_columns
            if col in df.columns
        ]
        df = df.with_columns(cast_exprs)

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Actual Generation",
                f"Total {generation_type.capitalize()} Capacity",
            ],
        ).sort("Interval Start")

    def _get_wind_solar_actual_historical_data(
        self,
        generation_type: Literal["wind", "solar"],
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
    ) -> pl.DataFrame:
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
            pdf = pd.read_csv(url)
        except Exception as e:
            raise RequestException(
                f"Failed to fetch historical {generation_type} generation data: {str(e)}",
            )

        pdf["Interval Start"] = pd.to_datetime(
            pdf["FORECAST_DATE_MPT"],
            format="mixed",
        )
        df = pl.from_pandas(pdf)
        df = utils.localize_ambiguous_infer_polars(
            df,
            "Interval Start",
            self.default_timezone,
        )

        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(hours=1)).alias("Interval End"),
        )

        if end:
            df = df.filter(
                (pl.col("Interval Start") >= pl.lit(date))
                & (pl.col("Interval Start") <= pl.lit(end)),
            )
        else:
            df = df.filter(pl.col("Interval Start") >= pl.lit(date))

        generation_prefix = generation_type.upper()
        df = df.rename(
            {
                f"{generation_prefix}_ACTUAL": "Actual Generation",
                f"{generation_prefix}_MCR": f"Total {generation_type.capitalize()} Capacity",
            },
        )

        numeric_columns = [
            "Actual Generation",
            f"Total {generation_type.capitalize()} Capacity",
        ]

        cast_exprs = [
            pl.col(col).cast(pl.Float64, strict=False)
            for col in numeric_columns
            if col in df.columns
        ]
        df = df.with_columns(cast_exprs)

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Actual Generation",
                f"Total {generation_type.capitalize()} Capacity",
            ],
        ).sort("Interval Start")
