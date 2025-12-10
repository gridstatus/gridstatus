import datetime
import os
import time
from itertools import chain
from typing import Any, Callable, Dict, List, Literal

import pandas as pd
import requests

from gridstatus import utils
from gridstatus.base import Markets, NoDataFoundException, NotSupported
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import setup_gs_logger

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CERTIFICATES_CHAIN_FILE = os.path.join(
    CURRENT_DIR,
    "public_certificates/miso_api/intermediate_and_root.pem",
)


PRICING_PRODUCT = "pricing"
BASE_PRICING_URL = "https://apim.misoenergy.org/pricing/v1"
LOAD_GENERATION_AND_INTERCHANGE_PRODUCT = "load_generation_and_interchange"
BASE_LOAD_GENERATION_AND_INTERCHANGE_URL = "https://apim.misoenergy.org/lgi/v1"

PRELIMINARY_STRING = "Preliminary"
FINAL_STRING = "Final"
FIVE_MINUTE_RESOLUTION = "5min"
HOURLY_RESOLUTION = "hourly"
DAILY_RESOLUTION = "daily"
EX_POST = "expost"
EX_ANTE = "exante"

logger = setup_gs_logger()


class MISOAPI:
    def __init__(
        self,
        pricing_api_key: str | None = None,
        load_generation_and_interchange_api_key: str | None = None,
        initial_sleep_seconds: int = 1,
        max_retries: int = 3,
        exponential_base: int = 2,
    ) -> None:
        """
        Class for querying the MISO API. Currently supports only pricing data.

        Arguments:
        pricing_api_key (str): The API key for the pricing API. Can be a comma-separated
            list of keys if you have multiple keys.
        initial_sleep_seconds (int): The number of seconds to wait between each request.
            Used to address rate limiting (429 responses).
        max_retries (int): The maximum number of retries for failed requests.
            Uses exponential backoff between retries. Used to address the common
            503 errors from the MISO API.
        exponential_base (int): The base for exponential backoff calculation.
            Sleep time = exponential_base^(attempt+1) seconds. Default is 2,
            which gives delays of 2, 4, 8 seconds for attempts 0, 1, 2.
        """
        self.pricing_api_key = pricing_api_key or os.getenv(
            "MISO_API_PRICING_SUBSCRIPTION_KEY",
            "",
        )
        self.pricing_api_keys = (self.pricing_api_key or "").split(",")
        # Used to rotate through the pricing API keys
        self.current_pricing_key_index = 0

        self.load_generation_and_interchange_api_key = (
            load_generation_and_interchange_api_key
            or os.getenv(
                "MISO_API_LOAD_GENERATION_AND_INTERCHANGE_SUBSCRIPTION_KEY",
                "",
            )
        )
        self.load_generation_and_interchange_api_keys = (
            self.load_generation_and_interchange_api_key or ""
        ).split(",")

        # Used to rotate through the load generation and interchange API keys
        self.current_load_generation_and_interchange_key_index = 0

        self.default_timezone = "EST"
        self.initial_sleep_seconds = initial_sleep_seconds
        self.max_retries = max_retries
        self.exponential_base = exponential_base

    def get_lmp_day_ahead_hourly_ex_ante(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_day_ahead_hourly,
            market=Markets.DAY_AHEAD_HOURLY_EX_ANTE,
            version=EX_ANTE,
            verbose=verbose,
            **kwargs,
        )

    def get_lmp_day_ahead_hourly_ex_post(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_day_ahead_hourly,
            market=Markets.DAY_AHEAD_HOURLY_EX_POST,
            version=EX_POST,
            verbose=verbose,
            **kwargs,
        )

    def get_lmp_real_time_hourly_ex_post_prelim(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_real_time_hourly_ex_post,
            market=Markets.REAL_TIME_HOURLY_EX_POST_PRELIM,
            prelim_or_final=PRELIMINARY_STRING,
            verbose=verbose,
            **kwargs,
        )

    def get_lmp_real_time_hourly_ex_post_final(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_real_time_hourly_ex_post,
            market=Markets.REAL_TIME_HOURLY_EX_POST_FINAL,
            prelim_or_final=FINAL_STRING,
            verbose=verbose,
            **kwargs,
        )

    def get_lmp_real_time_5_min_ex_ante(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_real_time_5_min_ex_ante,
            market=Markets.REAL_TIME_5_MIN_EX_ANTE,
            verbose=verbose,
            **kwargs,
        )

    def get_lmp_real_time_5_min_ex_post_prelim(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_real_time_5_min_ex_post,
            market=Markets.REAL_TIME_5_MIN_EX_POST_PRELIM,
            prelim_or_final=PRELIMINARY_STRING,
            verbose=verbose,
            **kwargs,
        )

    def get_lmp_real_time_5_min_ex_post_final(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_real_time_5_min_ex_post,
            market=Markets.REAL_TIME_5_MIN_EX_POST_FINAL,
            prelim_or_final=FINAL_STRING,
            verbose=verbose,
            **kwargs,
        )

    # NOTE: this method does not use the support_date_range decorator. Instead
    # it takes the output of a decorated function and processes that output all at once
    # which is more efficient than processing each iteration of the decorator
    def _get_pricing_data(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None,
        retrieval_func: Callable[..., List[Dict[str, Any]]],
        market: Markets,
        verbose: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame:
        data_lists = retrieval_func(date, end, verbose=verbose, **kwargs)

        data_list = self._flatten(data_lists)

        return self._process_pricing_data(data_list, market=market)

    @support_date_range(frequency="HOUR_START", return_raw=True)
    def _get_lmp_day_ahead_hourly(
        self,
        date: datetime.datetime,
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        version: str = EX_POST,
        verbose: bool = False,
    ) -> List[Dict[str, Any]]:
        interval = str(date.hour + 1)
        date_str = date.strftime("%Y-%m-%d")

        url = (
            f"{BASE_PRICING_URL}/day-ahead/{date_str}/lmp-{version}?interval={interval}"
        )

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    @support_date_range(frequency="HOUR_START", return_raw=True)
    def _get_lmp_real_time_hourly_ex_post(
        self,
        date: datetime.datetime,
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        prelim_or_final: str = PRELIMINARY_STRING,
        verbose: bool = False,
    ) -> List[Dict[str, Any]]:
        interval = str(date.hour + 1)
        date_str = date.strftime("%Y-%m-%d")
        version = EX_POST
        resolution = HOURLY_RESOLUTION

        url = f"{BASE_PRICING_URL}/real-time/{date_str}/lmp-{version}?interval={interval}&preliminaryFinal={prelim_or_final}&timeResolution={resolution}"  # noqa

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    @support_date_range(frequency="5_MIN", return_raw=True)
    def _get_lmp_real_time_5_min_ex_ante(
        self,
        date: datetime.datetime,
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> List[Dict[str, Any]]:
        # Interval format is hh:mm at the start of the interval
        interval = date.floor("5min").strftime("%H:%M")  # type: ignore[attr-defined]
        date_str = date.strftime("%Y-%m-%d")
        version = EX_ANTE

        url = (
            f"{BASE_PRICING_URL}/real-time/{date_str}/lmp-{version}?interval={interval}"
        )

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    @support_date_range(frequency="5_MIN", return_raw=True)
    def _get_lmp_real_time_5_min_ex_post(
        self,
        date: datetime.datetime,
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        prelim_or_final: str = PRELIMINARY_STRING,
        verbose: bool = False,
    ) -> List[Dict[str, Any]]:
        # Interval format is hh:mm at the start of the interval
        interval = date.floor("5min").strftime("%H:%M")  # type: ignore[attr-defined]
        date_str = date.strftime("%Y-%m-%d")
        version = EX_POST
        resolution = FIVE_MINUTE_RESOLUTION

        url = f"{BASE_PRICING_URL}/real-time/{date_str}/lmp-{version}?interval={interval}&preliminaryFinal={prelim_or_final}&timeResolution={resolution}"  # noqa

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    def _get_node_to_type_mapping(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        # use miso pricing api (Aggregated Pnode) to get location types
        df = self.get_pricing_nodes(start, end)

        return df[["Node", "Location Type"]]

    def _process_pricing_data(
        self,
        data_list: List[Dict[str, Any]],
        market: Markets,
    ) -> pd.DataFrame:
        df = self._data_list_to_df(data_list)

        start = df["Interval Start"].min()
        end = df["Interval End"].max()

        node_to_type_mapping = (
            self._get_node_to_type_mapping(start, end)
            .set_index("Node")["Location Type"]
            .to_dict()
        )

        df["Location Type"] = df["node"].map(node_to_type_mapping)
        df["Market"] = market.value

        df = df.rename(
            columns={
                "node": "Location",
                "lmp": "LMP",
                "mcc": "Congestion",
                "mec": "Energy",
                "mlc": "Loss",
            },
        )

        # Column ordering
        df = df[
            [
                "Interval Start",
                "Interval End",
                "Market",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
        ]

        return df

    @support_date_range(frequency="DAY_START")
    def get_interchange_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("h")

        if isinstance(date, str):
            # This should not happen after the above check, but for type safety
            raise ValueError("Invalid date format")

        if isinstance(date, tuple):
            # This should not happen after decorator processing, but for type safety
            raise ValueError("Tuple date format not supported here")

        date_str = date.strftime("%Y-%m-%d")

        historical_scheduled_url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/historical/{date_str}/interchange/net-scheduled"  # noqa

        historical_scheduled_data_list = self._get_url(
            historical_scheduled_url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        historical_scheduled_df = self._data_list_to_df(
            historical_scheduled_data_list,
        ).pivot(
            columns="adjacentBa",
            index=["Interval Start", "Interval End"],
            values="nsi",
        )

        historical_scheduled_df.columns = historical_scheduled_df.columns + " Scheduled"

        historical_scheduled_df = historical_scheduled_df.reset_index()

        real_time_actual_url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/real-time/{date_str}/interchange/net-actual"  # noqa

        real_time_actual_data_list = self._get_url(
            real_time_actual_url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        real_time_actual_df = self._data_list_to_df(real_time_actual_data_list)

        # Historical data has this as ONT, so convert for consistency
        real_time_actual_df["adjacentBa"] = real_time_actual_df["adjacentBa"].replace(
            "IESO",
            "ONT",
        )

        real_time_actual_df = real_time_actual_df.pivot(
            columns="adjacentBa",
            index=["Interval Start", "Interval End"],
            values="nai",
        )
        real_time_actual_df.columns = real_time_actual_df.columns + " Actual"
        real_time_actual_df = real_time_actual_df.reset_index()

        real_time_scheduled_url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/real-time/{date_str}/interchange/net-scheduled"  # noqa

        real_time_scheduled_data_list = self._get_url(
            real_time_scheduled_url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        real_time_scheduled_df = self._data_list_to_df(real_time_scheduled_data_list)

        real_time_scheduled_df = real_time_scheduled_df.rename(
            columns={
                "nsiForward": "Net Scheduled Interchange Forward",
                "nsiRealTime": "Net Scheduled Interchange Real Time",
                "nsiDelta": "Net Scheduled Interchange Delta",
            },
        )

        data = pd.merge(
            historical_scheduled_df,
            real_time_actual_df,
            on=["Interval Start", "Interval End"],
            how="outer",
        )

        data = pd.merge(
            data,
            real_time_scheduled_df,
            on=["Interval Start", "Interval End"],
            how="outer",
        )

        data = data[data["Interval Start"] >= date]

        if end is not None:
            data = data[data["Interval End"] <= end]

        for col in data.columns:
            if col not in ["Interval Start", "Interval End"]:
                data[col] = data[col].astype(float)

        return data[
            [
                "Interval Start",
                "Interval End",
                "Net Scheduled Interchange Forward",
                "Net Scheduled Interchange Real Time",
                "Net Scheduled Interchange Delta",
                "MHEB Scheduled",
                "MHEB Actual",
                "ONT Scheduled",
                "ONT Actual",
                "SWPP Scheduled",
                "SWPP Actual",
                "TVA Scheduled",
                "TVA Actual",
                "AECI Scheduled",
                "AECI Actual",
                "SOCO Scheduled",
                "SOCO Actual",
                "LGEE Scheduled",
                "LGEE Actual",
                "PJM Scheduled",
                "PJM Actual",
                "OTHER Scheduled",
                "SPA Actual",
            ]
        ].reset_index(drop=True)

    def _get_day_ahead_cleared_demand(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        time_resolution: str = DAILY_RESOLUTION,
    ) -> pd.DataFrame:
        date_str = date.strftime("%Y-%m-%d")

        url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/day-ahead/{date_str}/demand?timeResolution={time_resolution}"

        data_list = self._get_url(
            url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        df = self._data_list_to_df(
            data_list,
        )

        df = df.rename(
            columns={
                "region": "Region",
                "fixed": "Fixed Bids Cleared",
                "priceSens": "Price Sensitive Bids Cleared",
                "virtual": "Virtual Bids Cleared",
            },
        )

        data = df.reset_index()

        data = data[data["Interval Start"] >= date]

        if end is not None:
            data = data[data["Interval End"] <= end]

        for col in data.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                data[col] = data[col].astype(float)

        return data[
            [
                "Interval Start",
                "Interval End",
                "Region",
                "Fixed Bids Cleared",
                "Price Sensitive Bids Cleared",
                "Virtual Bids Cleared",
            ]
        ].reset_index(drop=True)

    @support_date_range(frequency="DAY_START")
    def get_day_ahead_cleared_demand_daily(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        return self._get_day_ahead_cleared_demand(
            date,
            end=end,
            verbose=verbose,
            time_resolution=DAILY_RESOLUTION,
        )

    @support_date_range(frequency="DAY_START")
    def get_day_ahead_cleared_demand_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        return self._get_day_ahead_cleared_demand(
            date,
            end=end,
            verbose=verbose,
            time_resolution=HOURLY_RESOLUTION,
        )

    def _get_day_ahead_cleared_generation_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        generation_type: str = "physical",
    ) -> pd.DataFrame:
        """
        Shared logic for getting day-ahead cleared generation (physical or virtual) hourly.
        generation_type: "physical" or "virtual"
        """
        date_str = date.strftime("%Y-%m-%d")

        url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/day-ahead/{date_str}/generation/cleared/{generation_type}"

        data_list = self._get_url(
            url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        df = self._data_list_to_df(
            data_list,
        )

        df = df.rename(
            columns={"region": "Region", "supply": "Supply Cleared"},
        )

        data = df.reset_index()

        data = data[data["Interval Start"] >= date]

        if end is not None:
            data = data[data["Interval End"] <= end]

        for col in data.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                data[col] = data[col].astype(float)

        return data[
            ["Interval Start", "Interval End", "Region", "Supply Cleared"]
        ].reset_index(drop=True)

    @support_date_range(frequency="DAY_START")
    def get_day_ahead_cleared_generation_physical_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        return self._get_day_ahead_cleared_generation_hourly(
            date,
            end,
            verbose,
            generation_type="physical",
        )

    @support_date_range(frequency="DAY_START")
    def get_day_ahead_cleared_generation_virtual_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        return self._get_day_ahead_cleared_generation_hourly(
            date,
            end,
            verbose,
            generation_type="virtual",
        )

    @support_date_range(frequency="DAY_START")
    def get_day_ahead_net_scheduled_interchange_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        date_str = date.strftime("%Y-%m-%d")

        url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/day-ahead/{date_str}/interchange/net-scheduled"

        data_list = self._get_url(
            url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        df = self._data_list_to_df(
            data_list,
        )

        df = df.rename(
            columns={
                "region": "Region",
                "nsi": "Net Scheduled Interchange",
            },
        )

        data = df.reset_index()

        data = data[data["Interval Start"] >= date]

        if end is not None:
            data = data[data["Interval End"] <= end]

        for col in data.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                data[col] = data[col].astype(float)

        return data[
            ["Interval Start", "Interval End", "Region", "Net Scheduled Interchange"]
        ].reset_index(drop=True)

    def _get_day_ahead_offered_generation_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        ecotype: str = "ecomax",
    ) -> pd.DataFrame:
        """
        Shared logic for getting day-ahead offered generation ecomax/ecomin hourly.
        ecotype: "ecomax" or "ecomin"
        """
        date_str = date.strftime("%Y-%m-%d")
        url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/day-ahead/{date_str}/generation/offered/{ecotype}"

        data_list = self._get_url(
            url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        df = self._data_list_to_df(
            data_list,
        )

        df = df.rename(
            columns={
                "region": "Region",
                "mustRun": "Must Run",
                "economic": "Economic",
                "emergency": "Emergency",
            },
        )

        data = df.reset_index()

        data = data[data["Interval Start"] >= date]

        if end is not None:
            data = data[data["Interval End"] <= end]

        for col in data.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                data[col] = data[col].astype(float)

        return data[
            [
                "Interval Start",
                "Interval End",
                "Region",
                "Must Run",
                "Economic",
                "Emergency",
            ]
        ].reset_index(drop=True)

    @support_date_range(frequency="DAY_START")
    def get_day_ahead_offered_generation_ecomax_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        return self._get_day_ahead_offered_generation_hourly(
            date,
            end,
            verbose,
            ecotype="ecomax",
        )

    @support_date_range(frequency="DAY_START")
    def get_day_ahead_offered_generation_ecomin_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        return self._get_day_ahead_offered_generation_hourly(
            date,
            end,
            verbose,
            ecotype="ecomin",
        )

    @support_date_range(frequency="DAY_START")
    def get_day_ahead_generation_fuel_type_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        date_str = date.strftime("%Y-%m-%d")

        url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/day-ahead/{date_str}/generation/fuel-type"

        data_list = self._get_url(
            url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        df = self._data_list_to_df(
            data_list,
        )

        if "interval" in df.columns:
            df = df.drop(columns=["interval"])

        # Expand the 'fuelTypes' dictionary column into separate columns
        fuel_types_df = df["fuelTypes"].apply(pd.Series)
        df = pd.concat([df.drop(columns=["fuelTypes"]), fuel_types_df], axis=1)

        df = df.rename(
            columns={
                "region": "Region",
                "totalMw": "Total",
                "coal": "Coal",
                "gas": "Gas",
                "nuclear": "Nuclear",
                "water": "Water",
                "wind": "Wind",
                "solar": "Solar",
                "other": "Other",
                "storage": "Storage",
            },
        )

        data = df.reset_index()

        data = data[data["Interval Start"] >= date]

        if end is not None:
            data = data[data["Interval End"] <= end]

        for col in data.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                data[col] = data[col].astype(float)

        return data[
            [
                "Interval Start",
                "Interval End",
                "Region",
                "Total",
                "Coal",
                "Gas",
                "Nuclear",
                "Water",
                "Wind",
                "Solar",
                "Other",
                "Storage",
            ]
        ].reset_index(drop=True)

    def _get_real_time_cleared_demand(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        time_resolution: str = DAILY_RESOLUTION,
    ) -> pd.DataFrame:
        date_str = date.strftime("%Y-%m-%d")

        url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/real-time/{date_str}/demand/forecast?timeResolution={time_resolution}"

        data_list = self._get_url(
            url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        df = self._data_list_to_df(
            data_list,
        )

        df = df.rename(
            columns={"demand": "Cleared Demand"},
        )

        data = df.reset_index()

        data = data[data["Interval Start"] >= date]

        if end is not None:
            data = data[data["Interval End"] <= end]

        for col in data.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                data[col] = data[col].astype(float)

        return data[
            [
                "Interval Start",
                "Interval End",
                "Cleared Demand",
            ]
        ].reset_index(drop=True)

    @support_date_range(frequency="DAY_START")
    def get_real_time_cleared_demand_daily(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        return self._get_real_time_cleared_demand(
            date,
            end=end,
            verbose=verbose,
            time_resolution=DAILY_RESOLUTION,
        )

    @support_date_range(frequency="DAY_START")
    def get_real_time_cleared_demand_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.today(tz=self.default_timezone).floor(
                "d",
            ) - pd.Timedelta(days=1)  # Yesterday

        return self._get_real_time_cleared_demand(
            date,
            end=end,
            verbose=verbose,
            time_resolution=HOURLY_RESOLUTION,
        )

    def _get_real_time_cleared_generation(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        time_resolution: str = HOURLY_RESOLUTION,
    ) -> pd.DataFrame:
        date_str = date.strftime("%Y-%m-%d")
        url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/real-time/{date_str}/generation/cleared/supply?timeResolution={time_resolution}"

        data_list = self._get_url(
            url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        df = self._data_list_to_df(
            data_list,
        )

        df = df.rename(
            columns={"generation": "Generation Cleared"},
        )

        data = df.reset_index()

        data = data[data["Interval Start"] >= date]

        if end is not None:
            data = data[data["Interval End"] <= end]

        for col in data.columns:
            if col not in ["Interval Start", "Interval End"]:
                data[col] = data[col].astype(float)

        data = data.sort_values(["Interval Start", "Interval End"])

        return data[
            ["Interval Start", "Interval End", "Generation Cleared"]
        ].reset_index(drop=True)

    @support_date_range(frequency="DAY_START")
    def get_real_time_cleared_generation_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        NOTE: This function is not ready for use yet. MISO Real-Time Cleared Generation API returns wrong timestamp.
        The timestamps are off by 5 hours, seems to be a timezone issue, UTC instead of EST.
        """
        raise NotImplementedError(
            "get_real_time_cleared_generation_hourly is not ready for use yet.",
        )
        if date == "latest":
            date = pd.Timestamp.today(tz=self.default_timezone).floor(
                "d",
            ) - pd.Timedelta(days=1)  # Yesterday

        return self._get_real_time_cleared_generation(
            date,
            end,
            verbose,
            time_resolution=HOURLY_RESOLUTION,
        )

    @support_date_range(frequency="DAY_START")
    def get_real_time_offered_generation_ecomax_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        date_str = date.strftime("%Y-%m-%d")

        url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/real-time/{date_str}/generation/offered/ecomax"

        data_list = self._get_url(
            url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        df = self._data_list_to_df(
            data_list,
        )

        df = df.rename(
            columns={
                "offerForwardEcoMax": "Offered FRAC Economic Max",
                "offerRealTimeEcoMax": "Offered Real Time Economic Max",
                "offerEcoMaxDelta": "Offered Economic Max Delta",
            },
        )

        data = df.reset_index()

        data = data[data["Interval Start"] >= date]

        if end is not None:
            data = data[data["Interval End"] <= end]

        for col in data.columns:
            if col not in ["Interval Start", "Interval End"]:
                data[col] = data[col].astype(float)

        data = data.sort_values(["Interval Start", "Interval End"])

        return data[
            [
                "Interval Start",
                "Interval End",
                "Offered FRAC Economic Max",
                "Offered Real Time Economic Max",
                "Offered Economic Max Delta",
            ]
        ].reset_index(drop=True)

    @support_date_range(frequency="DAY_START")
    def get_real_time_committed_generation_ecomax_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        date_str = date.strftime("%Y-%m-%d")

        url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/real-time/{date_str}/generation/committed/ecomax"

        data_list = self._get_url(
            url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        df = self._data_list_to_df(
            data_list,
        )

        df = df.rename(
            columns={
                "committedForwardEcoMax": "Committed FRAC Economic Max",
                "committedRealTimeEcoMax": "Committed Real Time Economic Max",
                "committedEcoMaxDelta": "Committed Economic Max Delta",
            },
        )

        data = df.reset_index()

        data = data[data["Interval Start"] >= date]

        if end is not None:
            data = data[data["Interval End"] <= end]

        for col in data.columns:
            if col not in ["Interval Start", "Interval End"]:
                data[col] = data[col].astype(float)

        data = data.sort_values(["Interval Start", "Interval End"])

        return data[
            [
                "Interval Start",
                "Interval End",
                "Committed FRAC Economic Max",
                "Committed Real Time Economic Max",
                "Committed Economic Max Delta",
            ]
        ].reset_index(drop=True)

    @support_date_range(frequency="DAY_START")
    def get_real_time_generation_fuel_type_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        date_str = date.strftime("%Y-%m-%d")

        url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/real-time/{date_str}/generation/fuel-type"

        data_list = self._get_url(
            url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        df = self._data_list_to_df(
            data_list,
        )

        if "interval" in df.columns:
            df = df.drop(columns=["interval"])

        # Expand the 'fuelTypes' dictionary column into separate columns
        fuel_types_df = df["fuelTypes"].apply(pd.Series)
        df = pd.concat([df.drop(columns=["fuelTypes"]), fuel_types_df], axis=1)

        df = df.rename(
            columns={
                "region": "Region",
                "totalMw": "Total",
                "coal": "Coal",
                "gas": "Gas",
                "nuclear": "Nuclear",
                "water": "Water",
                "wind": "Wind",
                "solar": "Solar",
                "other": "Other",
                "storage": "Storage",
            },
        )

        data = df.reset_index()

        data = data[data["Interval Start"] >= date]

        if end is not None:
            data = data[data["Interval End"] <= end]

        for col in data.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                data[col] = data[col].astype(float)

        return data[
            [
                "Interval Start",
                "Interval End",
                "Region",
                "Total",
                "Coal",
                "Gas",
                "Nuclear",
                "Water",
                "Wind",
                "Solar",
                "Other",
                "Storage",
            ]
        ].reset_index(drop=True)

    def _get_actual_load(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        time_resolution: str = HOURLY_RESOLUTION,
        geo_resolution: Literal["region", "localResourceZone"] = "region",
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d") - pd.Timedelta(
                days=1,
            )

        date_str = date.strftime("%Y-%m-%d")

        url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/real-time/{date_str}/demand/actual?timeResolution={time_resolution}&geoResolution={geo_resolution}"

        data_list = self._get_url(
            url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        df = self._data_list_to_df(
            data_list,
        )

        if "interval" in df.columns:
            df = df.drop(columns=["interval"])

        if geo_resolution == "region":
            df["region"] = df["region"].str.upper()
            subseries_index = "Region"
            df = df.rename(columns={"region": subseries_index, "load": "Load"})
        elif geo_resolution == "localResourceZone":
            subseries_index = "Local Resource Zone"
            df = df.rename(
                columns={"localResourceZone": subseries_index, "load": "Load"},
            )

        data = df.reset_index()

        data = data[data["Interval Start"] >= date]

        if end is not None:
            data = data[data["Interval End"] <= end]

        for col in data.columns:
            if col not in ["Interval Start", "Interval End", subseries_index]:
                data[col] = data[col].astype(float)

        data = data.sort_values(["Interval Start", subseries_index])
        return data[
            ["Interval Start", "Interval End", subseries_index, "Load"]
        ].reset_index(drop=True)

    @support_date_range(frequency="DAY_START")
    def get_actual_load_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        geo_resolution: Literal["region", "localResourceZone"] = "region",
    ) -> pd.DataFrame:
        return self._get_actual_load(
            date,
            end=end,
            verbose=verbose,
            time_resolution=HOURLY_RESOLUTION,
            geo_resolution=geo_resolution,
        )

    @support_date_range(frequency="DAY_START")
    def get_actual_load_hourly_pivoted(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get actual load by local resource zone in hourly intervals,
        pivoted with zones as columns.

        Returns columns: Interval Start, Interval End, LRZ1, LRZ2 7, LRZ3 5,
        LRZ4, LRZ6, LRZ8 9 10, MISO
        """
        df = self.get_actual_load_hourly(
            date=date,
            end=end,
            geo_resolution="localResourceZone",
            verbose=verbose,
        )

        # Replace underscores with spaces in zone names
        df["Local Resource Zone"] = df["Local Resource Zone"].str.replace("_", " ")

        # Pivot to get zones as columns
        df = df.pivot_table(
            index=["Interval Start", "Interval End"],
            columns="Local Resource Zone",
            values="Load",
        ).reset_index()

        # Add MISO total
        df["MISO"] = df[["LRZ1", "LRZ2 7", "LRZ3 5", "LRZ4", "LRZ6", "LRZ8 9 10"]].sum(
            axis=1,
        )

        return df

    @support_date_range(frequency="DAY_START")
    def get_actual_load_daily(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        geo_resolution: Literal["region", "localResourceZone"] = "region",
    ) -> pd.DataFrame:
        return self._get_actual_load(
            date,
            end=end,
            verbose=verbose,
            time_resolution=DAILY_RESOLUTION,
            geo_resolution=geo_resolution,
        )

    def _get_medium_term_load_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        publish_time: str | pd.Timestamp | None = None,
        time_resolution: str = HOURLY_RESOLUTION,
    ) -> pd.DataFrame:
        """
        publish_time: Optional publish_time to get forecast from.
            It must be earlier than both:
                - the current date (now)
                - and the forecast date (the date for which you're requesting predictions).

            If you don't specify this publish_time:
                - When your requested date is before today, it defaults to date - 1 day
                - When your requested date is today or in the future, it defaults to today - 1 day

            Example:

            - If today = 2025-10-14
                - You request forecasts for 2025-10-10 → publish_time = 2025-10-09
                - You request forecasts for 2025-10-14 (today) → publish_time = 2025-10-13
                - You request forecasts for 2025-10-15 (future) → publish_time = 2025-10-13

        Basically: it uses yesterday's forecast run unless you override it.
        """
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d") + pd.Timedelta(
                days=6,
            )  # Forecast date must be within 7 days of the publish_time.

        date_str = date.strftime("%Y-%m-%d")

        url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/forecast/{date_str}/load?timeResolution={time_resolution}"
        if publish_time is not None:
            publish_time = utils._handle_date(publish_time, self.default_timezone)
            init_str = publish_time.strftime("%Y-%m-%d")
            url += f"&init={init_str}"

        data_list = self._get_url(
            url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        df = self._data_list_to_df(
            data_list,
        )

        if "interval" in df.columns:
            df = df.drop(columns=["interval"])

        df = df.rename(
            columns={
                "region": "Region",
                "localResourceZone": "Local Resource Zone",
                "loadForecast": "Load Forecast",
            },
        )

        miso_publish_time = min(
            date.normalize() - pd.DateOffset(days=1),
            pd.Timestamp.now(tz=self.default_timezone).normalize()
            - pd.DateOffset(days=1),
        )
        df["Publish Time"] = (
            publish_time.normalize() if publish_time is not None else miso_publish_time
        )

        data = df.reset_index()

        data = data[data["Interval Start"] >= date]

        if end is not None:
            data = data[data["Interval End"] <= end]

        for col in data.columns:
            if col not in [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Region",
                "Local Resource Zone",
            ]:
                data[col] = data[col].astype(float)

        data = data.sort_values(
            ["Interval Start", "Publish Time", "Region", "Local Resource Zone"],
        )
        return data[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Region",
                "Local Resource Zone",
                "Load Forecast",
            ]
        ].reset_index(drop=True)

    @support_date_range(frequency="DAY_START")
    def get_medium_term_load_forecast_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        publish_time: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        return self._get_medium_term_load_forecast(
            date,
            end=end,
            verbose=verbose,
            publish_time=publish_time,
            time_resolution=HOURLY_RESOLUTION,
        )

    @support_date_range(frequency="DAY_START")
    def get_medium_term_load_forecast_hourly_aggregated(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        publish_time: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        """
        Get medium term load forecast aggregated from individual zones (Z1-Z10)
        to LRZ aggregates and pivoted with zones as columns.

        Returns columns: Interval Start, Interval End, Publish Time,
        LRZ1 MTLF, LRZ2_7 MTLF, LRZ3_5 MTLF, LRZ4 MTLF, LRZ6 MTLF,
        LRZ8_9_10 MTLF, MISO MTLF
        """
        df = self.get_medium_term_load_forecast_hourly(
            date=date,
            end=end,
            verbose=verbose,
            publish_time=publish_time,
        )

        # Map individual zones to aggregated LRZ format
        zone_map = {
            "Z1": "LRZ1 MTLF",
            "Z2": "LRZ2_7 MTLF",
            "Z7": "LRZ2_7 MTLF",
            "Z3": "LRZ3_5 MTLF",
            "Z5": "LRZ3_5 MTLF",
            "Z4": "LRZ4 MTLF",
            "Z6": "LRZ6 MTLF",
            "Z8": "LRZ8_9_10 MTLF",
            "Z9": "LRZ8_9_10 MTLF",
            "Z10": "LRZ8_9_10 MTLF",
        }
        df["LRZ"] = df["Local Resource Zone"].map(zone_map)

        # Aggregate and pivot to match expected format
        df_agg = (
            df.groupby(["Interval Start", "Interval End", "Publish Time", "LRZ"])[
                "Load Forecast"
            ]
            .sum()
            .reset_index()
        )
        df = df_agg.pivot_table(
            index=["Interval Start", "Interval End", "Publish Time"],
            columns="LRZ",
            values="Load Forecast",
        ).reset_index()

        # Add MISO total
        df["MISO MTLF"] = df[
            [
                "LRZ1 MTLF",
                "LRZ2_7 MTLF",
                "LRZ3_5 MTLF",
                "LRZ4 MTLF",
                "LRZ6 MTLF",
                "LRZ8_9_10 MTLF",
            ]
        ].sum(axis=1)

        return df

    @support_date_range(frequency="DAY_START")
    def get_medium_term_load_forecast_daily(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        publish_time: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        return self._get_medium_term_load_forecast(
            date,
            end=end,
            verbose=verbose,
            publish_time=publish_time,
            time_resolution=DAILY_RESOLUTION,
        )

    @support_date_range(frequency="DAY_START")
    def get_outage_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get hourly outage forecast. The API only returns hourly data for today and
        future days. Historical outage forecast data is not supported.

        Note: Outage forecast is only available for future dates (today and beyond).
        """
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        # Check if date is in the past
        today = pd.Timestamp.now(tz=self.default_timezone).floor("d")
        if date < today:
            raise NotSupported(
                "Outage forecast is only available for future dates. "
                f"Requested date {date.date()} is before today ({today.date()}). "
                "Historical outage forecast data is not supported by the MISO API.",
            )

        date_str = date.strftime("%Y-%m-%d")

        url = f"{BASE_LOAD_GENERATION_AND_INTERCHANGE_URL}/forecast/{date_str}/outage"

        data_list = self._get_url(
            url,
            product=LOAD_GENERATION_AND_INTERCHANGE_PRODUCT,
            verbose=verbose,
        )

        df = self._data_list_to_df(data_list)

        df = df.rename(
            columns={
                "region": "Region",
                "onOutage": "Outage Forecast",
            },
        )

        df = df[df["Interval Start"] >= date]

        if end is not None:
            df = df[df["Interval End"] <= end]

        df["Outage Forecast"] = df["Outage Forecast"].astype(float)

        return (
            df[
                [
                    "Interval Start",
                    "Interval End",
                    "Region",
                    "Outage Forecast",
                ]
            ]
            .sort_values(by=["Interval Start", "Region"])
            .reset_index(drop=True)
        )

    @support_date_range(frequency="DAY_START")
    def get_look_ahead_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        publish_time: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        """
        Get look-ahead hourly data combining medium-term load forecast and outage forecast.
        Look-ahead data is only available for future dates (today and beyond).
        Historical look-ahead data is not supported.

        Returns DataFrame with columns: Interval Start, Interval End, Publish Time, Region, MTLF, Outage
        This matches the output of MISO().get_look_ahead_hourly().
        """
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        # Check if date is in the past
        today = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        if date < today:
            raise NotSupported(
                "Look-ahead forecast is only available for today and future dates. "
                f"Requested date {date.date()} is before today ({today.date()}). "
                "Historical look-ahead forecast data is not supported by the MISO API.",
            )

        # Get medium-term load forecast
        load_forecast = self.get_medium_term_load_forecast_hourly(
            date,
            end=end,
            verbose=verbose,
            publish_time=publish_time,
        )

        # Get outage forecast
        outage_forecast = self.get_outage_forecast(
            date,
            end=end,
            verbose=verbose,
        )

        # Aggregate load forecast by Region (sum across Local Resource Zones)
        load_agg = (
            load_forecast.groupby(
                ["Interval Start", "Interval End", "Publish Time", "Region"],
            )["Load Forecast"]
            .sum()
            .reset_index()
        )

        # Merge the two datasets
        merged = pd.merge(
            load_agg,
            outage_forecast,
            on=["Interval Start", "Interval End", "Region"],
            how="outer",
        )

        # Rename columns to match MISO().get_look_ahead_hourly() output
        merged = merged.rename(
            columns={
                "Load Forecast": "MTLF",
                "Outage Forecast": "Outage",
            },
        )

        # Sort and return
        return (
            merged[
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "Region",
                    "MTLF",
                    "Outage",
                ]
            ]
            .sort_values(["Interval Start", "Region"])
            .reset_index(drop=True)
        )

    def _get_url(
        self,
        url: str,
        product: str,
        verbose: bool = False,
    ) -> List[Dict[str, Any]]:
        headers = self._headers(product=product)
        data_list = []

        if verbose:
            logger.info(f"Getting data from {url}")

        # First request with retry logic
        response = self._make_request_with_retry(
            url=url,
            headers=headers,
            params=None,
            verbose=verbose,
        )

        data = response.json()
        data_list.extend(data["data"])

        last_page = data["page"]["lastPage"]
        total_pages = data["page"]["totalPages"]
        page_number = data["page"]["pageNumber"]

        # Make sure to sleep after the first request
        time.sleep(self.initial_sleep_seconds)

        while page_number < total_pages and not last_page:
            page_number += 1

            if verbose:
                logger.info(f"Getting page {page_number} of {total_pages}")

            params = {"pageNumber": page_number}

            response = self._make_request_with_retry(
                url=url,
                headers=headers,
                params=params,
                verbose=verbose,
            )

            data = response.json()

            last_page = data["page"]["lastPage"]
            data_list.extend(data["data"])
            time.sleep(self.initial_sleep_seconds)

        return data_list

    def _make_request_with_retry(
        self,
        url: str,
        headers: Dict[str, str],
        params: Dict[str, Any] | None = None,
        verbose: bool = False,
    ) -> requests.Response:
        """Make a request with exponential backoff retry logic.

        Only retries on 429 (Too Many Requests) and 5xx server errors.
        Other HTTP errors are raised immediately without retry.

        Arguments:
            url: The URL to request.
            headers: The headers to include in the request.
            params: Optional query parameters.
            verbose: Whether to log verbose output.

        Returns:
            The successful response.

        Raises:
            requests.HTTPError: If a non-retryable error occurs or all retries
                are exhausted.
        """
        last_exception: Exception | None = None

        for attempt in range(self.max_retries + 1):
            response = requests.get(
                url,
                params=params,
                headers=headers,
                verify=CERTIFICATES_CHAIN_FILE,
            )

            if response.ok:
                return response

            # Only retry on 429 (rate limiting) and 5xx server errors
            is_retryable = response.status_code == 429 or (
                500 <= response.status_code < 600
            )
            if not is_retryable:
                response.raise_for_status()

            last_exception = requests.HTTPError(
                f"{response.status_code} Error: {response.reason}",
                response=response,
            )

            if attempt < self.max_retries:
                sleep_seconds = self.exponential_base ** (attempt + 1)
                logger.warning(
                    f"Request failed with status {response.status_code}. "
                    f"Retrying in {sleep_seconds} seconds "
                    f"(attempt {attempt + 1}/{self.max_retries})...",
                )
                time.sleep(sleep_seconds)

        # If we've exhausted all retries, raise the last exception
        raise last_exception  # type: ignore[misc]

    def _data_list_to_df(self, data_list: List[Dict[str, Any]]) -> pd.DataFrame:
        df = pd.DataFrame(data_list)

        if "timeInterval" not in df.columns and "interval" in df.columns:
            df["timeInterval"] = df["interval"]
            df = df.drop(columns=["interval"])

        # Split timeInterval dict into separate columns for 'start' and 'end'
        df = pd.concat(
            [
                df["timeInterval"].apply(pd.Series)[["start", "end"]],
                df.drop(columns=["timeInterval"]),
            ],
            axis=1,
        )

        df["Interval Start"] = pd.to_datetime(df["start"]).dt.tz_localize(
            self.default_timezone,
        )
        df["Interval End"] = pd.to_datetime(df["end"]).dt.tz_localize(
            self.default_timezone,
        )

        return df.drop(columns=["start", "end"]).reset_index(drop=True)

    def _get_next_key(self, product: str) -> str:
        """Get the next API key in the rotation."""
        if product == PRICING_PRODUCT:
            key_index = self.current_pricing_key_index
            self.current_pricing_key_index = (key_index + 1) % len(
                self.pricing_api_keys,
            )
            return self.pricing_api_keys[key_index]
        elif product == LOAD_GENERATION_AND_INTERCHANGE_PRODUCT:
            key_index = self.current_load_generation_and_interchange_key_index
            self.current_load_generation_and_interchange_key_index = (
                key_index + 1
            ) % len(self.load_generation_and_interchange_api_keys)
            return self.load_generation_and_interchange_api_keys[key_index]
        else:
            raise ValueError(f"Unknown product: {product}")

    def _headers(self, product: str) -> Dict[str, str]:
        return {
            "Ocp-Apim-Subscription-Key": (self._get_next_key(product)),
            "Cache-Control": "no-cache",
        }

    def _flatten(
        self,
        list_of_lists: List[List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        if len(list_of_lists) == 0:
            raise NoDataFoundException()

        return (
            list(chain(*list_of_lists))
            if isinstance(list_of_lists[0], list)
            else list_of_lists
        )

    def get_as_mcp_day_ahead_ex_ante(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        use_daily_requests: bool = False,
    ) -> pd.DataFrame:
        return self._get_mcp_data(
            date,
            end,
            retrieval_func=self._get_as_mcp_day_ahead,
            daily_retrieval_func=self._get_as_mcp_day_ahead_daily,
            use_daily_requests=use_daily_requests,
            version=EX_ANTE,
            verbose=verbose,
        )

    def get_as_mcp_day_ahead_ex_post(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        use_daily_requests: bool = False,
    ) -> pd.DataFrame:
        return self._get_mcp_data(
            date,
            end,
            retrieval_func=self._get_as_mcp_day_ahead,
            daily_retrieval_func=self._get_as_mcp_day_ahead_daily,
            use_daily_requests=use_daily_requests,
            version=EX_POST,
            verbose=verbose,
        )

    def get_as_mcp_real_time_5_min_ex_ante(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        use_daily_requests: bool = False,
    ) -> pd.DataFrame:
        return self._get_mcp_data(
            date,
            end,
            retrieval_func=self._get_as_mcp_real_time_5_min_ex_ante,
            daily_retrieval_func=self._get_as_mcp_real_time_5_min_ex_ante_daily,
            use_daily_requests=use_daily_requests,
            verbose=verbose,
        )

    def get_as_mcp_real_time_5_min_ex_post_prelim(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        use_daily_requests: bool = False,
    ) -> pd.DataFrame:
        return self._get_mcp_data(
            date,
            end,
            retrieval_func=self._get_as_mcp_real_time_ex_post_5_min,
            daily_retrieval_func=self._get_as_mcp_real_time_ex_post_5_min_daily,
            use_daily_requests=use_daily_requests,
            prelim_or_final=PRELIMINARY_STRING,
            verbose=verbose,
        )

    def get_as_mcp_real_time_hourly_ex_post_prelim(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        use_daily_requests: bool = False,
    ) -> pd.DataFrame:
        return self._get_mcp_data(
            date,
            end,
            retrieval_func=self._get_as_mcp_real_time_ex_post_hourly,
            daily_retrieval_func=self._get_as_mcp_real_time_ex_post_hourly_daily,
            use_daily_requests=use_daily_requests,
            prelim_or_final=PRELIMINARY_STRING,
            verbose=verbose,
        )

    def get_as_mcp_real_time_5_min_ex_post_final(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        use_daily_requests: bool = False,
    ) -> pd.DataFrame:
        return self._get_mcp_data(
            date,
            end,
            retrieval_func=self._get_as_mcp_real_time_ex_post_5_min,
            daily_retrieval_func=self._get_as_mcp_real_time_ex_post_5_min_daily,
            use_daily_requests=use_daily_requests,
            prelim_or_final=FINAL_STRING,
            verbose=verbose,
        )

    def get_as_mcp_real_time_hourly_ex_post_final(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        use_daily_requests: bool = False,
    ) -> pd.DataFrame:
        return self._get_mcp_data(
            date,
            end,
            retrieval_func=self._get_as_mcp_real_time_ex_post_hourly,
            daily_retrieval_func=self._get_as_mcp_real_time_ex_post_hourly_daily,
            use_daily_requests=use_daily_requests,
            prelim_or_final=FINAL_STRING,
            verbose=verbose,
        )

    def _get_mcp_data(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None,
        retrieval_func: Callable[..., List[List[Dict[str, Any]]]],
        daily_retrieval_func: Callable[..., List[List[Dict[str, Any]]]] | None = None,
        use_daily_requests: bool = False,
        verbose: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame:
        if use_daily_requests:
            if daily_retrieval_func is None:
                raise ValueError(
                    "daily_retrieval_func must be provided when use_daily_requests=True",
                )
            data_lists = daily_retrieval_func(date, end, verbose=verbose, **kwargs)
        else:
            data_lists = retrieval_func(date, end, verbose=verbose, **kwargs)

        data_list = self._flatten(data_lists)

        return self._process_as_mcp_data(data_list)

    @support_date_range(frequency="HOUR_START", return_raw=True)
    def _get_as_mcp_day_ahead(
        self,
        date: datetime.datetime,
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        version: str = EX_POST,
        verbose: bool = False,
    ) -> List[Dict[str, Any]]:
        interval = str(date.hour + 1)
        date_str = date.strftime("%Y-%m-%d")

        url = (
            f"{BASE_PRICING_URL}/day-ahead/{date_str}/asm-{version}?interval={interval}"
        )

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    @support_date_range(frequency="5_MIN", return_raw=True)
    def _get_as_mcp_real_time_5_min_ex_ante(
        self,
        date: datetime.datetime,
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> List[Dict[str, Any]]:
        interval = date.floor("5min").strftime("%H:%M")  # type: ignore[attr-defined]
        date_str = date.strftime("%Y-%m-%d")
        version = EX_ANTE

        url = (
            f"{BASE_PRICING_URL}/real-time/{date_str}/asm-{version}?interval={interval}"
        )

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    @support_date_range(frequency="5_MIN", return_raw=True)
    def _get_as_mcp_real_time_ex_post_5_min(
        self,
        date: datetime.datetime,
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        prelim_or_final: str = PRELIMINARY_STRING,
        verbose: bool = False,
    ) -> List[Dict[str, Any]]:
        interval = date.floor("5min").strftime("%H:%M")  # type: ignore[attr-defined]
        date_str = date.strftime("%Y-%m-%d")
        version = EX_POST
        time_resolution = FIVE_MINUTE_RESOLUTION

        url = f"{BASE_PRICING_URL}/real-time/{date_str}/asm-{version}?interval={interval}&preliminaryFinal={prelim_or_final}&timeResolution={time_resolution}"

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    @support_date_range(frequency="HOUR_START", return_raw=True)
    def _get_as_mcp_real_time_ex_post_hourly(
        self,
        date: datetime.datetime,
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        prelim_or_final: str = PRELIMINARY_STRING,
        verbose: bool = False,
    ) -> List[Dict[str, Any]]:
        interval = str(date.hour + 1)
        date_str = date.strftime("%Y-%m-%d")
        version = EX_POST
        time_resolution = HOURLY_RESOLUTION

        url = f"{BASE_PRICING_URL}/real-time/{date_str}/asm-{version}?interval={interval}&preliminaryFinal={prelim_or_final}&timeResolution={time_resolution}"

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    @support_date_range(frequency="DAY_START", return_raw=True)
    def _get_as_mcp_day_ahead_daily(
        self,
        date: datetime.datetime,
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        version: str = EX_POST,
        verbose: bool = False,
    ) -> List[Dict[str, Any]]:
        date_str = date.strftime("%Y-%m-%d")

        url = f"{BASE_PRICING_URL}/day-ahead/{date_str}/asm-{version}"

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    @support_date_range(frequency="DAY_START", return_raw=True)
    def _get_as_mcp_real_time_5_min_ex_ante_daily(
        self,
        date: datetime.datetime,
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> List[Dict[str, Any]]:
        date_str = date.strftime("%Y-%m-%d")
        version = EX_ANTE

        url = f"{BASE_PRICING_URL}/real-time/{date_str}/asm-{version}"

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    @support_date_range(frequency="DAY_START", return_raw=True)
    def _get_as_mcp_real_time_ex_post_5_min_daily(
        self,
        date: datetime.datetime,
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        prelim_or_final: str = PRELIMINARY_STRING,
        verbose: bool = False,
    ) -> List[Dict[str, Any]]:
        date_str = date.strftime("%Y-%m-%d")
        version = EX_POST
        time_resolution = FIVE_MINUTE_RESOLUTION

        url = f"{BASE_PRICING_URL}/real-time/{date_str}/asm-{version}?preliminaryFinal={prelim_or_final}&timeResolution={time_resolution}"

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    @support_date_range(frequency="DAY_START", return_raw=True)
    def _get_as_mcp_real_time_ex_post_hourly_daily(
        self,
        date: datetime.datetime,
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        prelim_or_final: str = PRELIMINARY_STRING,
        verbose: bool = False,
    ) -> List[Dict[str, Any]]:
        date_str = date.strftime("%Y-%m-%d")
        version = EX_POST
        time_resolution = HOURLY_RESOLUTION

        url = f"{BASE_PRICING_URL}/real-time/{date_str}/asm-{version}?preliminaryFinal={prelim_or_final}&timeResolution={time_resolution}"

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    def _process_as_mcp_data(
        self,
        data_list: List[Dict[str, Any]],
    ) -> pd.DataFrame:
        df = self._data_list_to_df(data_list)

        df = df.rename(
            columns={
                "zone": "Zone",
                "product": "Product",
                "mcp": "MCP",
            },
        )

        df_pivot = (
            df.pivot(
                index=["Interval Start", "Interval End", "Zone"],
                columns="Product",
                values="MCP",
            )
            .reset_index()
            .rename(columns={"Ramp-up": "Ramp Up", "Ramp-down": "Ramp Down"})
        )

        df_pivot.columns.name = None

        return df_pivot

    def _miso_quarterly_days(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> List[pd.Timestamp]:
        """
        MISO pricing nodes are updated quarterly on March 1st, June 1st, September 1st, and December 1st.
        This function generates a list of these dates within the specified start and end range.
        If no quarterly date falls within the range, return the most recent quarterly date before start.
        """
        # Create timezone-aware start date for date_range
        range_start = pd.Timestamp(
            f"{start.year - 1}-03-01",
            tz=self.default_timezone,
        )

        dates = pd.date_range(
            start=range_start,
            end=end,
            freq="QS-MAR",
        )

        dates_in_range = dates[(dates >= start) & (dates <= end)]
        if len(dates_in_range) > 0:
            return dates_in_range.tolist()

        return [dates[dates < start][-1]]

    def get_pricing_nodes(
        self,
        date: str | pd.Timestamp | None = "latest",
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieve MISO pricing nodes for a specific date or date range.
        MISO pricing nodes change quarterly on March 1st, June 1st, September 1st, and December 1st.
        New pricing nodes become effective on these dates, some pricing nodes are retired/removed and some nodes change names/node ids.
        Parameters:
            date: The date for which to retrieve pricing nodes. If None, defaults to "latest".
                  Can be a pd.Timestamp or "latest".
            end: Optional end date for a date range. If provided, retrieves pricing nodes for all
                 quarterly updates between date and end.
            verbose: If True, prints additional information during data retrieval.
        """
        if date == "latest" or date is None:
            date = pd.Timestamp.now(tz=self.default_timezone).floor("d")

        end = (
            utils._handle_date(end, self.default_timezone) if end is not None else None
        )

        if end is None:
            # Single date request
            date_str = date.strftime("%Y-%m-%d")

            url = f"{BASE_PRICING_URL}/aggregated-pnode?date={date_str}"

            data_list = self._get_url(
                url,
                product=PRICING_PRODUCT,
                verbose=verbose,
            )

            df = pd.DataFrame(data_list)

            df = df.rename(columns={"node": "Node", "nodeType": "Location Type"})

            return df

        else:
            # Ensure date is a pd.Timestamp with timezone.
            date = utils._handle_date(date, self.default_timezone)

            # Get quarterly dates (March 1st, June 1st, September 1st, December 1st)
            quarterly_dates = self._miso_quarterly_days(date, end)

            dfs = []

            for date in quarterly_dates:
                date_str = date.strftime("%Y-%m-%d")

                url = f"{BASE_PRICING_URL}/aggregated-pnode?date={date_str}"

                data_list = self._get_url(
                    url,
                    product=PRICING_PRODUCT,
                    verbose=verbose,
                )

                df = pd.DataFrame(data_list)

                df = df.rename(columns={"node": "Node", "nodeType": "Location Type"})

                dfs.append(df)

            df = (
                pd.concat(dfs, ignore_index=True)
                .drop_duplicates()
                .reset_index(drop=True)
            )

            return df
