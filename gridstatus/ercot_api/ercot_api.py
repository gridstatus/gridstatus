import argparse
import os
import random
import time
from typing import Optional

import numpy as np
import pandas as pd
import requests
import requests.status_codes as status_codes
from tqdm import tqdm

from gridstatus import utils
from gridstatus.base import Markets, NoDataFoundException
from gridstatus.decorators import support_date_range
from gridstatus.ercot import ELECTRICAL_BUS_LOCATION_TYPE, Ercot
from gridstatus.ercot_api.api_parser import _timestamp_parser, parse_all_endpoints
from gridstatus.gs_logging import log

# API to hit with subscription key to get token
TOKEN_URL = "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"  # noqa
BASE_URL = "https://api.ercot.com/api/public-reports"

# How long a token lasts for before needing to be refreshed
TOKEN_EXPIRATION_SECONDS = 3600

# Number of historical links to fetch at once. The max is 1_000
DEFAULT_HISTORICAL_SIZE = 1_000

# Number of results to fetch per page. It's not clear what the max is (1_000_000 works)
DEFAULT_PAGE_SIZE = 100_000

# Number of days in past when we should use the historical method
# NOTE: this seems to vary per dataset. If you get an error about no data available and
# your date is less than this many days in the past, try decreasing this number to
# search for historical data more recently.
HISTORICAL_DAYS_THRESHOLD = 90


# This file is publicly available and contains the full list of endpoints and their parameters # noqa
ENDPOINTS_MAP_URL = "https://raw.githubusercontent.com/ercot/api-specs/main/pubapi/pubapi-apim-api.json"  # noqa

# https://data.ercot.com/data-product-archive/NP4-188-CD
AS_PRICES_ENDPOINT = "/np4-188-cd/dam_clear_price_for_cap"

# We only use the historical API for AS REPORTS because those downloads are easier
# to parse (all the files are included in one zip file)
# https://data.ercot.com/data-product-archive/NP3-911-ER
AS_REPORTS_EMIL_ID = "np3-911-er"

# https://data.ercot.com/data-product-archive/NP4-33-CD
AS_PLAN_ENDPOINT = "/np4-33-cd/dam_as_plan"

#  https://data.ercot.com/data-product-archive/NP6-788-CD
LMP_BY_SETTLEMENT_POINT_ENDPOINT = "/np6-788-cd/lmp_node_zone_hub"

# https://data.ercot.com/data-product-archive/NP6-787-CD
LMP_BY_BUS_ENDPOINT = "/np6-787-cd/lmp_electrical_bus"

# https://data.ercot.com/data-product-archive/NP3-233-CD
HOURLY_RESOURCE_OUTAGE_CAPACITY_REPORTS_ENDPOINT = "/np3-233-cd/hourly_res_outage_cap"

# https://data.ercot.com/data-product-archive/NP4-183-CD
DAM_LMP_ENDPOINT = "/np4-183-cd/dam_hourly_lmp"

# https://data.ercot.com/data-product-archive/NP4-191-CD
SHADOW_PRICES_DAM_ENDPOINT = "/np4-191-cd/dam_shadow_prices"

# https://data.ercot.com/data-product-archive/NP6-86-CD
SHADOW_PRICES_SCED_ENDPOINT = "/np6-86-cd/shdw_prices_bnd_trns_const"

# Wind Power Production
# https://data.ercot.com/data-product-archive/NP4-732-CD
HOURLY_WIND_POWER_PRODUCTION_ENDPOINT = "/np4-732-cd/wpp_hrly_avrg_actl_fcast"

# Solar Power Production - Hourly Averaged Actual and Forecasted Values by Geographical Region # noqa
# https://data.ercot.com/data-product-archive/NP4-745-CD
HOURLY_SOLAR_POWER_PRODUCTION_BY_GEOGRAPHICAL_REGION_REPORT_ENDPOINT = (
    "/np4-745-cd/spp_hrly_actual_fcast_geo"
)

# Settlement Point Price for each Settlement Point, produced from SCED LMPs every 15 minutes. # noqa
# https://data.ercot.com/data-product-archive/NP6-905-CD
SETTLEMENT_POINT_PRICE_REAL_TIME_15_MIN = "/np6-905-cd/spp_node_zone_hub"

# Day ahead settlement point prices
# https://data.ercot.com/data-product-archive/NP4-190-CD
SPP_DAY_AHEAD_HOURLY = "/np4-190-cd/dam_stlmnt_pnt_prices"


class ErcotAPI:
    """
    Class to authenticate with and make requests to the ERCOT Data API (api.ercot.com)

    WARNING: the API appears to be a WIP and may change without notice.

    To authenticate, you need a username and password plus a subscription key.

    To register, create an account here: https://apiexplorer.ercot.com/
    To obtain a subscription key, follow the instructions here: https://developer.ercot.com/applications/pubapi/ERCOT%20Public%20API%20Registration%20and%20Authentication/
    """  # noqa

    default_timezone = "US/Central"

    def __init__(
        self,
        username: str = None,
        password: str = None,
        subscription_key: str = None,
        sleep_seconds: float = 0.2,
        max_retries: int = 3,
    ):
        self.username = username or os.getenv("ERCOT_API_USERNAME")
        self.password = password or os.getenv("ERCOT_API_PASSWORD")
        self.subscription_key = subscription_key or os.getenv(
            "ERCOT_API_SUBSCRIPTION_KEY",
        )

        if not all([self.username, self.password, self.subscription_key]):
            raise ValueError(
                "Username, password, and subscription key must be provided or set as environment variables",  # noqa
            )

        self.client_id = "fec253ea-0d06-4272-a5e6-b478baeecd70"  # From the docs
        self.endpoints_map = self._get_endpoints_map()
        self.token_url = TOKEN_URL
        self.token = None
        self.token_expiry = None
        self.ercot = Ercot()

        self.sleep_seconds = sleep_seconds
        self.initial_delay = min(max(0.1, sleep_seconds), 60.0)
        self.max_retries = min(max(0, max_retries), 10)

    def _local_now(self):
        return pd.Timestamp("now", tz=self.default_timezone)

    def _local_start_of_today(self):
        return pd.Timestamp("now", tz=self.default_timezone).floor("d")

    def _handle_end_date(self, date, end, days_to_add_if_no_end):
        """
        Handles a provided end date by either

        1. Using the provided end date converted to the default timezone
        2. Adding the number of days to the date and converting to the default timezone
        """
        if end:
            end = utils._handle_date(end, tz=self.default_timezone)
        else:
            # Have to convert to UTC to do addition, then convert back to local time
            # to avoid DST issues
            end = (
                (date.tz_convert("UTC") + pd.DateOffset(days=days_to_add_if_no_end))
                .normalize()
                .tz_localize(None)
                .tz_localize(self.default_timezone)
            )

        return end

    def get_token(self):
        payload = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
            "response_type": "id_token",
            "scope": "openid fec253ea-0d06-4272-a5e6-b478baeecd70 offline_access",
            "client_id": self.client_id,
        }

        response = requests.post(self.token_url, data=payload)
        response_data = response.json()

        if "id_token" in response_data:
            self.token = response_data["id_token"]
            self.token_expiry = time.time() + TOKEN_EXPIRATION_SECONDS

        else:
            raise Exception("Failed to obtain token")

    def refresh_token_if_needed(self):
        if not self.token or time.time() >= self.token_expiry:
            self.get_token()

    def headers(self):
        self.refresh_token_if_needed()

        # Both forms of authentication are required
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Ocp-Apim-Subscription-Key": self.subscription_key,
        }

        return headers

    def make_api_call(
        self,
        url,
        api_params=None,
        parse_json=True,
        verbose=False,
    ):
        log(f"Requesting url: {url} with params: {api_params}", verbose)
        # make request with exponential backoff retry strategy
        retries = 0
        delay = self.initial_delay
        while retries <= self.max_retries:
            response = requests.get(url, params=api_params, headers=self.headers())

            retries += 1
            if response.status_code == status_codes.codes.OK:
                break
            elif (
                response.status_code == status_codes.codes.TOO_MANY_REQUESTS
                and retries <= self.max_retries
            ):
                log(
                    f"Warn: Rate-limited: waiting {delay} seconds before retry {retries}/{self.max_retries} "  # noqa
                    f"requesting url: {url} with params: {api_params}",
                    verbose,
                )
                time.sleep(delay + random.uniform(0, delay * 0.1))
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
                log(error_message, verbose)
                response.raise_for_status()

        if parse_json:
            return response.json()
        else:
            return response.content

    def get_public_reports(self, verbose=False):
        # General information about the public reports
        return self.make_api_call(BASE_URL, verbose=verbose)

    @support_date_range(frequency=None)
    def get_hourly_wind_report(self, date, end=None, verbose=False):
        """Get Wind Power Production - Hourly Averaged Actual and Forecasted Values

        Arguments:
            date (str): the date to fetch reports for.
            end (str, optional): the end date to fetch reports for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with hourly wind power production reports
        """
        if date == "latest":
            date = self._local_now() - pd.Timedelta(hours=1)
            end = self._local_now()

        end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        # Only use the historical API because it allows us to filter on posted time (
        # publish time)
        data = self.get_historical_data(
            endpoint=HOURLY_WIND_POWER_PRODUCTION_ENDPOINT,
            start_date=date,
            end_date=end,
            verbose=verbose,
            add_post_datetime=True,
        )

        return self._handle_hourly_wind_report(data, verbose=verbose)

    def _handle_hourly_wind_report(self, data, verbose=False):
        data = Ercot().parse_doc(data, verbose=verbose)

        data.columns = data.columns.str.replace("_", " ")

        data["Publish Time"] = pd.to_datetime(data["postDatetime"]).dt.tz_localize(
            self.default_timezone,
        )

        data = (
            utils.move_cols_to_front(
                data,
                ["Interval Start", "Interval End", "Publish Time"],
            )
            .drop(columns=["Time", "postDatetime"])
            .sort_values(["Interval Start", "Publish Time"])
        )

        return data

    @support_date_range(frequency=None)
    def get_hourly_solar_report(self, date, end=None, verbose=False):
        """Get Solar Power Production - Hourly Averaged Actual and Forecasted Values by
        Geographical Region

        Arguments:
            date (str): the date to fetch reports for.
            end (str, optional): the end date to fetch reports for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with hourly wind power production reports
        """
        if date == "latest":
            date = self._local_now() - pd.Timedelta(hours=1)
            end = self._local_now()

        end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        # Only use the historical API because it allows us to filter on posted time (
        # publish time)
        data = self.get_historical_data(
            endpoint=HOURLY_SOLAR_POWER_PRODUCTION_BY_GEOGRAPHICAL_REGION_REPORT_ENDPOINT,  # noqa
            start_date=date,
            end_date=end,
            verbose=verbose,
            add_post_datetime=True,
        )

        return self._handle_hourly_solar_report(data, verbose=verbose)

    def _handle_hourly_solar_report(self, data, verbose=False):
        data = Ercot().parse_doc(data, verbose=verbose)

        data.columns = data.columns.str.replace("_", " ")

        data["Publish Time"] = pd.to_datetime(data["postDatetime"]).dt.tz_localize(
            self.default_timezone,
        )

        data = (
            utils.move_cols_to_front(
                data,
                ["Interval Start", "Interval End", "Publish Time"],
            )
            .drop(columns=["Time", "postDatetime"])
            .sort_values(["Interval Start", "Publish Time"])
        )

        return data

    @support_date_range(frequency=None)
    def get_as_prices(self, date, end=None, verbose=False):
        """Get Ancillary Services Prices

        Arguments:
            date (str): the date to fetch prices for. Can be "latest" to fetch the next
                day's prices.
            end (str, optional): the end date to fetch prices for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with ancillary services prices
        """
        if date == "latest":
            return self.get_as_prices("today", verbose=verbose)

        end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        if self._should_use_historical(date):
            data = self.get_historical_data(
                # Need to subtract 1 because we filter by posted date and this data
                # is day-ahead
                endpoint=AS_PRICES_ENDPOINT,
                start_date=date - pd.Timedelta(days=1),
                end_date=end - pd.Timedelta(days=1),
                verbose=verbose,
            )
        else:
            api_params = {
                "deliveryDateFrom": date,
                "deliveryDateTo": end,
            }

            data = self.hit_ercot_api(
                endpoint=AS_PRICES_ENDPOINT,
                page_size=DEFAULT_PAGE_SIZE,
                verbose=verbose,
                **api_params,
            )

        return self._handle_as_prices(data, verbose=verbose)

    def _handle_as_prices(self, data, verbose=False):
        data = self.ercot.parse_doc(data, verbose=verbose)
        data = self.ercot._finalize_as_price_df(data, pivot=True)

        return (
            data.sort_values(["Interval Start"])
            .drop(columns=["Time"])
            .reset_index(
                drop=True,
            )
        )

    @support_date_range(frequency=None)
    def get_as_reports(self, date, end=None, verbose=False):
        """Get Ancillary Services Reports. Data contains 48 hours disclosures

        Arguments:
            date (str): the date to fetch reports for.
            end (str, optional): the end date to fetch reports for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with ancillary services reports
        """
        if date == "latest" or utils.is_today(date, tz=self.default_timezone):
            raise ValueError("Cannot get AS reports for 'latest' or 'today'")

        offset = pd.DateOffset(days=2)

        # Published with a 2-day delay
        report_date = date.normalize() + offset

        if end:
            end = end + offset
        else:
            end = self._handle_end_date(report_date, end, days_to_add_if_no_end=1)

        links_and_posted_datetimes = self._get_historical_data_links(
            emil_id=AS_REPORTS_EMIL_ID,
            start_date=report_date,
            end_date=end,
            verbose=verbose,
        )

        urls = [tup[0] for tup in links_and_posted_datetimes]

        dfs = [
            self.ercot._handle_as_reports_file(
                url,
                verbose=verbose,
                headers=self.headers(),
            )
            for url in urls
        ]

        return (
            pd.concat(dfs)
            .reset_index(drop=True)
            .drop(columns=["Time"])
            .sort_values("Interval Start")
        )

    @support_date_range(frequency=None)
    def get_as_plan(self, date, end=None, verbose=False):
        """Get Ancillary Service requirements by type and quantity for each hour of the
        current day plus the next 6 days.

        Arguments:
            date (str): the date to fetch plans for.
            end (str, optional): the end date to fetch plans for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with ancillary services plans
        """
        if date == "latest":
            return self.get_as_plan("today", verbose=verbose)

        if not end:
            end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        data = self.get_historical_data(
            endpoint=AS_PLAN_ENDPOINT,
            start_date=date,
            end_date=end,
            add_post_datetime=True,
            verbose=verbose,
        )

        df = Ercot().parse_doc(data)
        df["Publish Time"] = pd.to_datetime(df["postDatetime"])

        return Ercot()._handle_as_plan(df)

    @support_date_range(frequency=None)
    def get_lmp_by_settlement_point(self, date, end=None, verbose=False):
        """Get Locational Marginal Prices by Settlement Point

        Arguments:
            date (str): the date to fetch prices for. Can be "latest" to fetch the next
                day's prices.
            end (str, optional): the end date to fetch prices for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with locational marginal prices
        """
        if date == "latest":
            return self.get_lmp_by_settlement_point("today", verbose=verbose)

        end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        if self._should_use_historical(date):
            data = self.get_historical_data(
                endpoint=LMP_BY_SETTLEMENT_POINT_ENDPOINT,
                start_date=date,
                end_date=end,
                verbose=verbose,
            )
        else:
            api_params = {
                "SCEDTimestampFrom": date,
                "SCEDTimestampTo": end,
            }

            data = self.hit_ercot_api(
                endpoint=LMP_BY_SETTLEMENT_POINT_ENDPOINT,
                page_size=DEFAULT_PAGE_SIZE,
                verbose=verbose,
                **api_params,
            )

        data = self.ercot._handle_lmp_df(df=data, verbose=verbose)

        return data.sort_values(["Interval Start", "Location"]).reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_hourly_resource_outage_capacity(self, date, end=None, verbose=False):
        """Get Hourly Resource Outage Capacity Reports. Fetches all reports
        published on the given date. Reports extend out 168 hours from the
        start of the day.

        Arguments:
            date (str): the date to fetch reports for.
            end (str, optional): the end date to fetch reports for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with hourly resource outage capacity reports
        """
        if date == "latest":
            return self.get_hourly_resource_outage_capacity("today", verbose=verbose)

        end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        data = self.get_historical_data(
            endpoint=HOURLY_RESOURCE_OUTAGE_CAPACITY_REPORTS_ENDPOINT,
            start_date=date,
            end_date=end,
            verbose=verbose,
            add_post_datetime=True,
        )

        data["Publish Time"] = pd.to_datetime(data["postDatetime"]).dt.tz_localize(
            self.default_timezone,
            ambiguous="NaT",
        )

        data = self.ercot.parse_doc(
            data,
            # there is no DST flag column and the data set ignores DST
            # so, we will default to assuming it is DST. We will also
            # set nonexistent times to NaT and drop them
            dst_ambiguous_default=True,
            nonexistent="NaT",
            verbose=verbose,
        ).dropna(subset=["Interval Start", "Publish Time"])

        data = utils.move_cols_to_front(
            data,
            ["Interval Start", "Interval End", "Publish Time"],
        )

        return (
            self.ercot._handle_hourly_resource_outage_capacity_df(df=data)
            .sort_values(["Interval Start", "Publish Time"])
            .reset_index(drop=True)
            .drop(columns=["Time", "postDatetime"])
        )

    @support_date_range(frequency=None)
    def get_lmp_by_bus(self, date, end=None, verbose=False):
        """Get the Locational Marginal Price for each Electrical Bus, normally produced
            by SCED every five minutes.

        Arguments:
            date (str): the date to fetch prices for.
            end (str, optional): the end date to fetch prices for. Defaults to None.
        Returns:
            pandas.DataFrame: A DataFrame with lmps by bus

        Data from https://data.ercot.com/data-product-archive/NP6-787-CD
        """
        if date == "latest":
            # Set the date to a few minutes ago to ensure we get the latest data
            date = pd.Timestamp.now(tz=self.default_timezone) - pd.Timedelta(minutes=15)
            end = None

        if self._should_use_historical(date):
            end = self._handle_end_date(date, end, days_to_add_if_no_end=1)
            data = self.get_historical_data(
                endpoint=LMP_BY_BUS_ENDPOINT,
                start_date=date,
                end_date=end,
                verbose=verbose,
            )
        else:
            api_params = {
                "SCEDTimestampFrom": date,
                "SCEDTimestampTo": end,
            }

            data = self.hit_ercot_api(
                endpoint=LMP_BY_BUS_ENDPOINT,
                page_size=DEFAULT_PAGE_SIZE,
                verbose=verbose,
                **api_params,
            )

        return self._handle_lmp_by_bus(data, verbose=verbose)

    def _handle_lmp_by_bus(self, data, verbose=False):
        data = self.ercot._handle_sced_timestamp(data, verbose=verbose)

        data = data.rename(columns={"ElectricalBus": "Location"})
        data["Location Type"] = ELECTRICAL_BUS_LOCATION_TYPE
        data["Market"] = Markets.REAL_TIME_SCED.value

        data = utils.move_cols_to_front(
            data,
            [
                "Interval Start",
                "Interval End",
                "SCED Timestamp",
                "Market",
                "Location",
                "Location Type",
                "LMP",
            ],
        )

        return data.sort_values(["SCED Timestamp", "Location"]).reset_index(
            drop=True,
        )

    @support_date_range(frequency=None)
    def get_lmp_by_bus_dam(self, date, end=None, verbose=False):
        """
        Retrieves the hourly Day Ahead Market (DAM) Location Marginal Prices (LMPs)
        for the given date range.

        Data source: https://data.ercot.com/data-product-archive/NP4-183-CD
        (requires login)
        """
        if date == "latest":
            return self.get_lmp_by_bus_dam("today", verbose=verbose)

        # The Ercot API needs to have a start and end filter date, so we must set it.
        # To ensure we get all the data for the given date, we set the end date to the
        # date plus one if it is not provided.
        end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        if self._should_use_historical(date):
            # For historical data, we need to subtract a day because we filter by
            # posted date and this is day-ahead data
            data = self.get_historical_data(
                endpoint=DAM_LMP_ENDPOINT,
                start_date=date - pd.Timedelta(days=1),
                end_date=end - pd.Timedelta(days=1),
                verbose=verbose,
            )
        else:
            # For non-historical data, we do not need to subtract a day because filter
            # by delivery date
            api_params = {
                "deliveryDateFrom": date,
                "deliveryDateTo": end,
            }

            data = self.hit_ercot_api(
                endpoint=DAM_LMP_ENDPOINT,
                page_size=DEFAULT_PAGE_SIZE,
                verbose=verbose,
                **api_params,
            )

        return self.parse_dam_doc(data)

    def parse_dam_doc(self, data):
        data = (
            self.ercot.parse_doc(
                data.rename(
                    columns=dict(
                        deliveryDate="DeliveryDate",
                        hourEnding="HourEnding",
                        busName="BusName",
                    ),
                ),
            )
            .rename(columns={"BusName": "Location"})
            .drop(columns=["Time"])
            .sort_values(["Interval Start"])
            .reset_index(drop=True)
            .assign(
                Market=Markets.DAY_AHEAD_HOURLY.name,
                **{"Location Type": ELECTRICAL_BUS_LOCATION_TYPE},
            )
        )

        data = utils.move_cols_to_front(
            data,
            [
                "Interval Start",
                "Interval End",
                "Market",
                "Location",
                "Location Type",
                "LMP",
            ],
        )

        return data

    @support_date_range(frequency=None)
    def get_shadow_prices_dam(self, date, end=None, verbose=False):
        """Get Day-Ahead Market Shadow Prices

        Arguments:
            date (str): the date to fetch shadow prices for. Can be "latest" to fetch
                the next day's shadow prices.
            end (str, optional): the end date to fetch shadow prices for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with day-ahead market shadow prices
        """  # noqa
        if date == "latest":
            return self.get_shadow_prices_dam("today", verbose=verbose)

        # The Ercot API needs to have a start and end filter date, so we must set it.
        # To ensure we get all the data for the given date, we set the end date to the
        # date plus one if it is not provided.
        end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        if self._should_use_historical(date):
            # For the historical data, we need to subtract a day because we filter by
            # posted date
            data = self.get_historical_data(
                endpoint=SHADOW_PRICES_DAM_ENDPOINT,
                start_date=date - pd.Timedelta(days=1),
                end_date=end - pd.Timedelta(days=1),
                verbose=verbose,
            )
        else:
            # For non-historical data, we do not need to subtract a day because filter
            # by delivery date
            api_params = {
                "deliveryDateFrom": date,
                "deliveryDateTo": end,
            }

            data = self.hit_ercot_api(
                endpoint=SHADOW_PRICES_DAM_ENDPOINT,
                page_size=DEFAULT_PAGE_SIZE,
                verbose=verbose,
                **api_params,
            )

        return self._handle_shadow_prices_dam(data, verbose=verbose)

    def _handle_shadow_prices_dam(self, data, verbose=False):
        data = self.ercot.parse_doc(data, verbose=verbose)
        data = data.rename(columns=self._shadow_prices_column_name_mapper())
        data = self._construct_limiting_facility_column(data)
        # Fill all empty strings in the dataframe with NaN
        data = data.replace("", pd.NA)

        data = utils.move_cols_to_front(
            data,
            [
                "Interval Start",
                "Interval End",
                "Constraint ID",
                "Constraint Name",
                "Contingency Name",
                "Limiting Facility",
            ],
        )

        data = data.drop(columns=["Delivery Time", "Time"])

        return data.sort_values(["Interval Start", "Constraint ID"]).reset_index(
            drop=True,
        )

    @support_date_range(frequency=None)
    def get_shadow_prices_sced(self, date, end=None, verbose=False):
        """Get Real-Time Market Shadow Prices

        Arguments:
            date (str): the date to fetch shadow prices for. Can be "latest".
            end (str, optional): the end date to fetch shadow prices for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with real-time market shadow prices
        """  # noqa
        if date == "latest":
            return self.get_shadow_prices_sced("today", verbose=verbose)

        # The Ercot API needs to have a start and end filter date, so we must set it.
        # To ensure we get all the data for the given date, we set the end date to the
        # date plus one if it is not provided.
        end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        if self._should_use_historical(date):
            data = self.get_historical_data(
                endpoint=SHADOW_PRICES_SCED_ENDPOINT,
                start_date=date,
                end_date=end,
                verbose=verbose,
            )
        else:
            api_params = {
                "SCEDTimestampFrom": date,
                "SCEDTimestampTo": end,
            }

            data = self.hit_ercot_api(
                endpoint=SHADOW_PRICES_SCED_ENDPOINT,
                page_size=DEFAULT_PAGE_SIZE,
                verbose=verbose,
                **api_params,
            )

        return self._handle_shadow_prices_sced(data, verbose=verbose)

    def _handle_shadow_prices_sced(self, data, verbose=False):
        data = self.ercot._handle_sced_timestamp(data, verbose=verbose)
        data = data.rename(columns=self._shadow_prices_column_name_mapper())
        data = self._construct_limiting_facility_column(data)
        # Fill all empty strings in the dataframe with NaN
        data = data.replace("", pd.NA)

        data = utils.move_cols_to_front(
            data,
            [
                "Interval Start",
                "Interval End",
                "SCED Timestamp",
                "Constraint ID",
                "Constraint Name",
                "Contingency Name",
                "Limiting Facility",
            ],
        )

        return data.sort_values(["SCED Timestamp", "Constraint ID"]).reset_index(
            drop=True,
        )

    def _construct_limiting_facility_column(self, data):
        data["Limiting Facility"] = np.where(
            data["Contingency Name"] != "BASE CASE",
            data["From Station"].astype(str)
            + "_"
            + data["From Station kV"].astype(str)
            + "_"
            + data["To Station"].astype(str)
            + "_"
            + data["To Station kV"].astype(str),
            pd.NA,
        )

        return data

    def _shadow_prices_column_name_mapper(self):
        return {
            "CCTStatus": "CCT Status",
            "ConstraintId": "Constraint ID",  # API is inconsistent with capitalization
            "ConstraintID": "Constraint ID",
            "ConstraintLimit": "Constraint Limit",
            "ConstraintName": "Constraint Name",
            "ConstraintValue": "Constraint Value",
            "ContingencyName": "Contingency Name",
            "DeliveryTime": "Delivery Time",
            "FromStation": "From Station",
            "FromStationkV": "From Station kV",
            "MaxShadowPrice": "Max Shadow Price",
            "ShadowPrice": "Shadow Price",
            "SystemLambda": "System Lambda",
            "ToStation": "To Station",
            "ToStationkV": "To Station kV",
            "ViolatedMW": "Violated MW",
            "ViolationAmount": "Violation Amount",
        }

    @support_date_range(frequency=None)
    def get_spp_real_time_15_min(self, date, end=None, verbose=False):
        """Get Real Time 15-Minute Settlement Point Prices

        Arguments:
            date (str): the date to fetch prices for.
            end (str, optional): the end date to fetch prices for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with settlement point prices
        """
        if date == "latest":
            return self.get_spp_by_settlement_point("today", verbose=verbose)

        end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        data = self.get_historical_data(
            endpoint=SETTLEMENT_POINT_PRICE_REAL_TIME_15_MIN,
            # These offsets are necessary so the start and end records are correct
            start_date=date + pd.Timedelta(minutes=15),
            end_date=end + pd.Timedelta(minutes=15),
            verbose=verbose,
        )

        data = Ercot().parse_doc(data, verbose=verbose)

        data = Ercot()._finalize_spp_df(
            data,
            market=Markets.REAL_TIME_15_MIN,
            locations="ALL",
            location_type="ALL",
            verbose=verbose,
        )

        return data.sort_values(["Interval Start", "Location"]).reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_spp_day_ahead_hourly(self, date, end=None, verbose=False):
        """Get Day Ahead Hourly Settlement Point Prices

        Arguments:
            date (str): the date to fetch prices for.
            end (str, optional): the end date to fetch prices for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with settlement point prices
        """
        if date == "latest":
            return self.get_spp_day_ahead_hourly("today", verbose=verbose)

        end = self._handle_end_date(date, end, days_to_add_if_no_end=1)

        # Subtract 1 from the dates because this is published day-ahead
        date = date - pd.Timedelta(days=1)
        end = end - pd.Timedelta(days=1)

        data = self.get_historical_data(
            endpoint=SPP_DAY_AHEAD_HOURLY,
            start_date=date,
            end_date=end,
            verbose=verbose,
        )

        data = Ercot().parse_doc(data, verbose=verbose)

        data = Ercot()._finalize_spp_df(
            data,
            market=Markets.DAY_AHEAD_HOURLY,
            locations="ALL",
            location_type="ALL",
            verbose=verbose,
        )

        return data.sort_values(["Interval Start", "Location"]).reset_index(drop=True)

    def get_historical_data(
        self,
        endpoint,
        start_date,
        end_date,
        read_as_csv=True,
        add_post_datetime=False,
        verbose=False,
    ):
        """Retrieves historical data from the given emil_id from start to end date.
        The historical data endpoint only allows filtering by the postDatetimeTo and
        postDatetimeFrom parameters. The retrieval process has two steps:

        1. Get the links to download the historical data
        2. Download the historical data from the links

        NOTE: this message is exclusive of the end date. For example, if you want to
        get data for 2021-01-01 to 2021-01-02, you should set the end date to
        2021-01-03.

        Arguments:
            endpoint [str]: a string representing a specific ERCOT API endpoint.
            start_date [datetime]: the start datetime for the historical data. Used
                as the postDatetimeFrom query parameter.
            end_date [datetime]: the end date for the historical data. Used as the
                postDatetimeTo query parameter.
            read_as_csv [bool]: if True, will read the data as a csv. Otherwise, will
                return the bytes.
            add_post_datetime [bool]: if True, will add the postDatetime to the
                dataframe. This is used for getting publish times.
            verbose [bool]: if True, will print out status messages

        Returns:
            [pandas.DataFrame]: a dataframe of historical data
        """
        emil_id = endpoint.split("/")[1]
        links_and_post_datetimes = self._get_historical_data_links(
            emil_id,
            start_date,
            end_date,
            verbose,
        )

        if not links_and_post_datetimes:
            raise NoDataFoundException(
                f"No historical data links found for {endpoint} with",
                f"time range {start_date} to {end_date}",
            )

        links, post_datetimes = zip(*links_and_post_datetimes)

        dfs = []
        retries = 0
        max_retries = 3

        for link, posted_datetime in tqdm(
            zip(links, post_datetimes),
            desc="Fetching historical data",
            ncols=80,
            disable=not verbose,
            total=len(links),
        ):
            while retries < max_retries:
                try:
                    # Data comes back as a compressed zip file.
                    response = self.make_api_call(
                        link,
                        verbose=verbose,
                        parse_json=False,
                    )

                    # Convert the bytes to a file-like object
                    bytes = pd.io.common.BytesIO(response)

                    if not read_as_csv:
                        dfs.append(bytes)
                    else:
                        # Decompress the zip file and read the csv
                        df = pd.read_csv(bytes, compression="zip")

                        if add_post_datetime:
                            df["postDatetime"] = posted_datetime

                        dfs.append(df)

                    # Necessary to avoid rate limiting
                    time.sleep(self.sleep_seconds)
                    break  # Exit the loop if the operation is successful

                except Exception as e:
                    if "Rate limit is exceeded" in response.decode("utf-8"):
                        # Rate limited, so sleep for a longer time
                        log(
                            f"Rate limited. Sleeping {self.sleep_seconds * 10} seconds",
                            verbose,
                        )
                        time.sleep(self.sleep_seconds * 10)
                    else:
                        log(f"Link: {link} failed with error: {e}", verbose)
                        time.sleep(self.sleep_seconds)  # Wait before retrying

                    retries += 1

            if retries == max_retries:
                log(
                    f"Max retries reached. Link: {link} failed after {max_retries} attempts.",  # noqa
                    verbose,
                )

        return pd.concat(dfs) if read_as_csv else dfs

    def _get_historical_data_links(self, emil_id, start_date, end_date, verbose=False):
        """Retrieves links to download historical data for the given emil_id from
        start to end date.

        Returns:
            [list]: a list of links to download historical data
        """
        urlstring = f"{BASE_URL}/archive/{emil_id}"

        page_num = 1

        api_params = {
            "postDatetimeFrom": _timestamp_parser(start_date),
            "postDatetimeTo": _timestamp_parser(end_date),
            "size": DEFAULT_HISTORICAL_SIZE,
            "page": page_num,
        }

        response = self.make_api_call(urlstring, api_params=api_params, verbose=verbose)

        meta = response["_meta"]
        total_pages = meta["totalPages"]
        archives = response["archives"]

        with self._create_progress_bar(
            total_pages,
            "Fetching historical links",
            verbose=verbose,
        ) as pbar:
            while page_num < total_pages:
                page_num += 1
                api_params["page"] = page_num

                response = self.make_api_call(
                    urlstring,
                    api_params=api_params,
                    verbose=verbose,
                )
                archives.extend(response["archives"])

                pbar.update(1)

        links_and_post_datetimes = [
            (
                archive.get("_links").get("endpoint").get("href"),
                archive.get("postDatetime"),
            )
            for archive in archives
        ]

        log(f"Found {len(links_and_post_datetimes)} archives", verbose)

        return links_and_post_datetimes

    def hit_ercot_api(
        self,
        endpoint: str,
        page_size: int = DEFAULT_PAGE_SIZE,
        max_pages: Optional[int] = None,
        verbose: bool = False,
        **api_params,
    ) -> pd.DataFrame:
        """Retrieves data from the given endpoint of the ERCOT API

        Arguments:
            endpoint: a string representing a specific ERCOT API endpoint.
                examples:
                - "/np6-345-cd/act_sys_load_by_wzn",
                - "/np6-787-cd/lmp_electrical_bus"
            page_size: specifies the number of results to return per page, defaulting
                to DEFAULT_PAGE_SIZE because otherwise this will be very slow when
                fetching large datasets.
            max_pages: if provided, will stop paginating after reaching this number.
                Useful in testing to avoid long-running queries, but may result in
                incomplete data.
            verbose: if True, will print out status messages
            api_params: additional arguments and values to pass along to the endpoint.
                These are generally filters that limit the data returned.

        Raises:
            KeyError if the given endpoint does not exist

        Returns:
            a dataframe of results
        """
        api_params = {k: v for k, v in api_params.items() if v is not None}
        parsed_api_params = self._parse_api_params(endpoint, page_size, api_params)
        urlstring = f"{BASE_URL}{endpoint}"

        current_page = 1
        # Make a first request to get the total number of pages and first data
        parsed_api_params["page"] = current_page
        response = self.make_api_call(
            urlstring,
            api_params=parsed_api_params,
            verbose=verbose,
        )
        # The data comes back as a list of lists. We get the columns to
        # create a dataframe after we have all the data
        columns = [f["name"] for f in response["fields"]]

        # ensure that there is data before proceeding
        if "data" not in response or "_meta" not in response:
            raise NoDataFoundException(
                f"No data found for {endpoint} with params {api_params}",
            )

        data_results = response["data"]
        total_pages = response["_meta"]["totalPages"]
        pages_to_retrieve = total_pages

        # determine total number of pages to be retrieved
        if max_pages is not None:
            pages_to_retrieve = min(total_pages, max_pages)

            if pages_to_retrieve < total_pages:
                # User requested fewer pages than total
                print(
                    f"warning: only retrieving {max_pages} pages "
                    f"out of {total_pages} total",
                )

        with self._create_progress_bar(
            pages_to_retrieve,
            "Fetching data",
            verbose=verbose,
        ) as pbar:
            while current_page < total_pages:
                if max_pages is not None and current_page >= max_pages:
                    break

                current_page += 1
                parsed_api_params["page"] = current_page

                log(
                    f"Requesting url: {urlstring} with params {parsed_api_params}",
                    verbose,
                )

                response = self.make_api_call(
                    urlstring,
                    api_params=parsed_api_params,
                    verbose=verbose,
                )

                data_results.extend(response["data"])

                pbar.update(1)

        # Capitalize the first letter of each column name but leave the rest alone
        columns = [col[:1].upper() + col[1:] for col in columns]

        # Strip the extra whitespace from the data
        data = pd.DataFrame(data=data_results, columns=columns)
        data = data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        return data

    def _should_use_historical(self, date):
        return utils._handle_date(
            date,
            tz=self.default_timezone,
        ) < self._local_start_of_today() - pd.Timedelta(
            days=HISTORICAL_DAYS_THRESHOLD,
        )

    def list_all_endpoints(self) -> None:
        """Prints all available endpoints"""
        for endpoint, contents in sorted(self.endpoints_map.items()):
            print(endpoint)
            print(f"    {contents['summary']}")

    def describe_one_endpoint(self, endpoint: str) -> None:
        """Prints details about a given endpoint"""
        endpoint_contents = self.endpoints_map.get(endpoint, None)

        if endpoint_contents is None:
            print(f"{endpoint} is not a valid ERCOT API endpoint")
            return

        print(f"Endpoint: {endpoint}")
        print(f"Summary:  {endpoint_contents['summary']}")
        print("Parameters:")
        for param, details in sorted(endpoint_contents["parameters"].items()):
            print(f"    {param} - {details['value_type']}")

    def _parse_api_params(self, endpoint, page_size, api_params):
        # validate endpoint string
        endpoint_contents = self.endpoints_map.get(endpoint, None)

        if endpoint_contents is None:
            raise KeyError(f"{endpoint} is not a valid ERCOT API endpoint")

        # determine parameters and types for endpoint, validate and parse api_params
        parsed_api_params = {"size": page_size}

        for arg, value in api_params.items():
            parser = endpoint_contents["parameters"].get(arg, {}).get("parser_method")
            if parser is not None:
                parsed_api_params[arg] = parser(value)

        return parsed_api_params

    def _get_endpoints_map(self):
        endpoints = requests.get(ENDPOINTS_MAP_URL).json()
        endpoints = parse_all_endpoints(apijson=endpoints)

        return endpoints

    def _create_progress_bar(self, total_pages, desc, verbose):
        return tqdm(
            total=total_pages,
            desc=desc,
            ncols=80,
            disable=not verbose,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["list", "describe"])
    parser.add_argument("--endpoint", required=False)

    args = parser.parse_args()
    if args.action == "list":  # TODO avoid case match because lower python version
        ErcotAPI(sleep_seconds=2.0).list_all_endpoints()
    elif args.action == "describe":
        ErcotAPI(sleep_seconds=2.0).describe_one_endpoint(args.endpoint)
    else:
        print(f"{args.action} is not a valid action")
        print("Try 'list' or 'describe'")
