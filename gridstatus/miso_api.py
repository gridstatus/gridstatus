import datetime
import os
import time
from itertools import chain
from typing import Callable, Dict, List

import pandas as pd
import requests

from gridstatus.base import Markets, NoDataFoundException
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import setup_gs_logger
from gridstatus.miso import MISO

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
EX_POST = "expost"
EX_ANTE = "exante"

logger = setup_gs_logger()


class MISOAPI:
    def __init__(
        self,
        pricing_api_key: str = None,
        load_generation_and_interchange_api_key: str = None,
        initial_sleep_seconds: int = 1,
    ):
        """
        Class for querying the MISO API. Currently supports only pricing data.

        Arguments:
        pricing_api_key (str): The API key for the pricing API. Can be a comma-separated
        list of keys if you have multiple keys.
        initial_sleep_seconds (int): The number of seconds to wait between each request.
        Used to prevent rate limiting.
        """
        self.pricing_api_key = pricing_api_key or os.getenv(
            "MISO_API_PRICING_SUBSCRIPTION_KEY",
            "",
        )
        self.pricing_api_keys = self.pricing_api_key.split(",")
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
            self.load_generation_and_interchange_api_key.split(",")
        )

        # Used to rotate through the load generation and interchange API keys
        self.current_load_generation_and_interchange_key_index = 0

        self.default_timezone = "EST"
        self.initial_sleep_seconds = initial_sleep_seconds

    def get_lmp_day_ahead_hourly_ex_ante(self, date, end=None, verbose=False):
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_day_ahead_hourly,
            market=Markets.DAY_AHEAD_HOURLY_EX_ANTE,
            version=EX_ANTE,
            verbose=verbose,
        )

    def get_lmp_day_ahead_hourly_ex_post(self, date, end=None, verbose=False):
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_day_ahead_hourly,
            market=Markets.DAY_AHEAD_HOURLY_EX_POST,
            version=EX_POST,
            verbose=verbose,
        )

    def get_lmp_real_time_hourly_ex_post_prelim(self, date, end=None, verbose=False):
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_real_time_hourly_ex_post,
            market=Markets.REAL_TIME_HOURLY_EX_POST_PRELIM,
            prelim_or_final=PRELIMINARY_STRING,
            verbose=verbose,
        )

    def get_lmp_real_time_hourly_ex_post_final(self, date, end=None, verbose=False):
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_real_time_hourly_ex_post,
            market=Markets.REAL_TIME_HOURLY_EX_POST_FINAL,
            prelim_or_final=FINAL_STRING,
            verbose=verbose,
        )

    def get_lmp_real_time_5_min_ex_ante(self, date, end=None, verbose=False):
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_real_time_5_min_ex_ante,
            market=Markets.REAL_TIME_5_MIN_EX_ANTE,
            verbose=verbose,
        )

    def get_lmp_real_time_5_min_ex_post_prelim(self, date, end=None, verbose=False):
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_real_time_5_min_ex_post,
            market=Markets.REAL_TIME_5_MIN_EX_POST_PRELIM,
            prelim_or_final=PRELIMINARY_STRING,
            verbose=verbose,
        )

    def get_lmp_real_time_5_min_ex_post_final(self, date, end=None, verbose=False):
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_real_time_5_min_ex_post,
            market=Markets.REAL_TIME_5_MIN_EX_POST_FINAL,
            prelim_or_final=FINAL_STRING,
            verbose=verbose,
        )

    # NOTE: this method does not use the support_date_range decorator. Instead
    # it takes the output of a decorated function and processes that output all at once
    # which is more efficient than processing each iteration of the decorator
    def _get_pricing_data(
        self,
        date,
        end,
        retrieval_func: Callable,
        market: Markets,
        verbose: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        data_lists = retrieval_func(date, end, verbose=verbose, **kwargs)

        data_list = self._flatten(data_lists)

        return self._process_pricing_data(data_list, market=market)

    @support_date_range(frequency="HOUR_START", return_raw=True)
    def _get_lmp_day_ahead_hourly(
        self,
        date,
        end=None,
        version: str = EX_POST,
        verbose=False,
    ):
        # 0-padded hour. 00 doesn't exist so add 1 to the hour
        interval = str(date.hour + 1).zfill(2)
        date_str = date.strftime("%Y-%m-%d")

        url = (
            f"{BASE_PRICING_URL}/day-ahead/{date_str}/lmp-{version}?interval={interval}"
        )

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    @support_date_range(frequency="HOUR_START", return_raw=True)
    def _get_lmp_real_time_hourly_ex_post(
        self,
        date,
        end=None,
        prelim_or_final: str = PRELIMINARY_STRING,
        verbose=False,
    ):
        # 0-padded hour. 00 doesn't exist so add 1 to the hour
        interval = str(date.hour + 1).zfill(2)
        date_str = date.strftime("%Y-%m-%d")
        version = EX_POST
        resolution = HOURLY_RESOLUTION

        url = f"{BASE_PRICING_URL}/real-time/{date_str}/lmp-{version}?interval={interval}&preliminaryFinal={prelim_or_final}&timeResolution={resolution}"  # noqa

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    @support_date_range(frequency="5_MIN", return_raw=True)
    def _get_lmp_real_time_5_min_ex_ante(self, date, end=None, verbose=False):
        # Interval format is hh:mm at the start of the interval
        interval = date.floor("5min").strftime("%H:%M")
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
        date,
        end=None,
        prelim_or_final: str = PRELIMINARY_STRING,
        verbose=False,
    ):
        # Interval format is hh:mm at the start of the interval
        interval = date.floor("5min").strftime("%H:%M")
        date_str = date.strftime("%Y-%m-%d")
        version = EX_POST
        resolution = FIVE_MINUTE_RESOLUTION

        url = f"{BASE_PRICING_URL}/real-time/{date_str}/lmp-{version}?interval={interval}&preliminaryFinal={prelim_or_final}&timeResolution={resolution}"  # noqa

        data_list = self._get_url(url, product=PRICING_PRODUCT, verbose=verbose)

        return data_list

    def _process_pricing_data(
        self,
        data_list: List[Dict],
        market: Markets,
    ) -> pd.DataFrame:
        df = self._data_list_to_df(data_list)

        node_to_type_mapping = (
            MISO()
            ._get_node_to_type_mapping()
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
        date: str | datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime | None = None,
        verbose: bool = False,
    ):
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("h")

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

    def _get_url(
        self,
        url,
        product: str,
        verbose: bool = False,
        max_retries: int = 3,
    ) -> List:
        headers = self._headers(product=product)
        data_list = []

        if verbose:
            logger.info(f"Getting data from {url}")

        response = requests.get(url, headers=headers, verify=CERTIFICATES_CHAIN_FILE)
        response.raise_for_status()

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

            page_url = f"{url}&pageNumber={page_number}"

            attempt = 0
            response = requests.get(
                page_url,
                headers=headers,
                verify=CERTIFICATES_CHAIN_FILE,
            )

            while response.status_code != 200 and attempt < max_retries:
                attempt += 1

                logger.warning(
                    f"Request failed with {response.status_code}. Retrying in "
                    f"{2**attempt} seconds...",
                )

                time.sleep(self.initial_sleep_seconds**attempt)
                response = requests.get(
                    page_url,
                    headers=headers,
                    verify=CERTIFICATES_CHAIN_FILE,
                )

            data = response.json()

            last_page = data["page"]["lastPage"]
            data_list.extend(data["data"])
            time.sleep(self.initial_sleep_seconds)

        return data_list

    def _data_list_to_df(self, data_list: List[Dict]) -> pd.DataFrame:
        df = pd.DataFrame(data_list)

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

    def _headers(self, product: str) -> Dict:
        return {
            "Ocp-Apim-Subscription-Key": (self._get_next_key(product)),
            "Cache-Control": "no-cache",
        }

    def _flatten(self, list_of_lists: List[List]) -> List:
        if len(list_of_lists) == 0:
            raise NoDataFoundException()

        return (
            list(chain(*list_of_lists))
            if isinstance(list_of_lists[0], list)
            else list_of_lists
        )
