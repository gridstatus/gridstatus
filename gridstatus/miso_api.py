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

PRICING_PRODUCT = "pricing"
BASE_PRICING_URL = "https://apim.misoenergy.org/pricing/v1"
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
        load_api_key: str = None,
    ):
        self.pricing_api_key = pricing_api_key or os.getenv(
            "MISO_API_PRICING_SUBSCRIPTION_KEY",
        )
        self.load_api_key = load_api_key or os.getenv("MISO_API_LOAD_SUBSCRIPTION_KEY")
        self.default_timezone = "EST"

    def get_lmp_day_ahead_hourly_ex_ante(self, date, end=None, verbose=False):
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_day_ahead_hourly,
            market=Markets.DAY_AHEAD_HOURLY,
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
            market=Markets.REAL_TIME_HOURLY_PRELIM,
            prelim_or_final=PRELIMINARY_STRING,
            verbose=verbose,
        )

    def get_lmp_real_time_hourly_ex_post_final(self, date, end=None, verbose=False):
        return self._get_pricing_data(
            date,
            end,
            retrieval_func=self._get_lmp_real_time_hourly_ex_post,
            market=Markets.REAL_TIME_HOURLY_FINAL,
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
    ):
        data_lists = retrieval_func(date, end, verbose=verbose, **kwargs)

        data_list = self._flatten(data_lists)

        return self.process_pricing_data(data_list, market=market)

    @support_date_range(frequency="HOUR_START", convert_to_dataframe=False)
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

        data_list = self._get(url, verbose=verbose)

        return data_list

    @support_date_range(frequency="HOUR_START", convert_to_dataframe=False)
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

        data_list = self._get(url, verbose=verbose)

        return data_list

    @support_date_range(frequency="5_MIN", convert_to_dataframe=False)
    def _get_lmp_real_time_5_min_ex_ante(self, date, end=None, verbose=False):
        # Interval format is hh:mm at the start of the interval
        interval = date.floor("5min").strftime("%H:%M")
        date_str = date.strftime("%Y-%m-%d")
        version = EX_ANTE

        url = (
            f"{BASE_PRICING_URL}/real-time/{date_str}/lmp-{version}?interval={interval}"
        )

        data_list = self._get(url, verbose=verbose)

        return data_list

    @support_date_range(frequency="5_MIN", convert_to_dataframe=False)
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

        data_list = self._get(url, verbose=verbose)

        return data_list

    def process_pricing_data(self, data_list: List[Dict], market: Markets):
        df = pd.DataFrame(data_list)

        # Split timeInterval dict into separate columns for 'start' and 'end'
        df = pd.concat(
            [
                df["timeInterval"].apply(pd.Series)[["start", "end"]],
                df.drop(columns=["timeInterval"]),
            ],
            axis=1,
        )

        df["Interval Start"] = pd.to_datetime(df["start"], utc=True).dt.tz_convert(
            self.default_timezone,
        )
        df["Interval End"] = pd.to_datetime(df["end"], utc=True).dt.tz_convert(
            self.default_timezone,
        )

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

    def _get(self, url, verbose=False, max_retries=3):
        headers = self._headers()
        data_list = []

        if verbose:
            logger.info(f"Getting data from {url}")

        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()

        data = response.json()
        data_list.extend(data["data"])

        last_page = data["page"]["lastPage"]
        total_pages = data["page"]["totalPages"]
        page_number = data["page"]["pageNumber"]

        while page_number < total_pages and not last_page:
            page_number += 1

            if verbose:
                logger.info(f"Getting page {page_number} of {total_pages}")

            page_url = f"{url}&pageNumber={page_number}"

            # Retry logic
            attempt = 0
            response = requests.get(page_url, headers=headers, verify=False)

            while response.status_code != 200 and attempt < max_retries:
                attempt += 1

                logger.warning(
                    f"Request failed with {response.status_code}. Retrying in "
                    f"{2**attempt} seconds...",
                )

                time.sleep(2**attempt)

                response = requests.get(page_url, headers=headers, verify=False)
            data = response.json()

            last_page = data["page"]["lastPage"]
            data_list.extend(data["data"])

        return data_list

    def _headers(self, product: str = PRICING_PRODUCT) -> Dict:
        return {
            "Ocp-Apim-Subscription-Key": (
                self.pricing_api_key
                if product == PRICING_PRODUCT
                else self.load_api_key
            ),
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
