import argparse
import os
import time
from typing import Optional

import numpy as np
import pandas as pd
import requests
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

# This file is publicly available and contains the full list of endpoints and their parameters # noqa
ENDPOINTS_MAP_URL = (
    "https://raw.githubusercontent.com/ercot/api-specs/main/pubapi/pubapi-apim-api.json"  # noqa
)

# https://data.ercot.com/data-product-archive/NP4-188-CD
AS_PRICES_ENDPOINT = "/np4-188-cd/dam_clear_price_for_cap"

# We only use the historical API for AS REPORTS because those downloads are easier
# to parse (all the files are included in one zip file)
# https://data.ercot.com/data-product-archive/NP3-911-ER
AS_REPORTS_EMIL_ID = "np3-911-er"

# https://data.ercot.com/data-product-archive/NP4-745-CD
HOURLY_SOLAR_REPORT_ENDPOINT = "/np4-745-cd/spp_hrly_actual_fcast_geo"

#  https://data.ercot.com/data-product-archive/NP6-788-CD
LMP_BY_SETTLEMENT_POINT_ENDPOINT = "/np6-788-cd/lmp_node_zone_hub"

# https://data.ercot.com/data-product-archive/NP3-233-CD
HOURLY_RESOURCE_OUTAGE_CAPACITY_REPORTS_ENDPOINT = "/np3-233-cd/hourly_res_outage_cap"

# https://data.ercot.com/data-product-archive/NP4-183-CD
DAM_LMP_ENDPOINT = "/np4-183-cd/dam_hourly_lmp"

# https://data.ercot.com/data-product-archive/NP4-191-CD
SHADOW_PRICES_DAM_ENDPOINT = "/np4-191-cd/dam_shadow_prices"

# https://data.ercot.com/data-product-archive/NP6-86-CD
SHADOW_PRICES_SCED_ENDPOINT = "/np6-86-cd/shdw_prices_bnd_trns_const"


# How long a token lasts for before needing to be refreshed
TOKEN_EXPIRATION_SECONDS = 3600

# Number of historical links to fetch at once. The max is 1_000
DEFAULT_HISTORICAL_SIZE = 1_000

# Number of days in past when we should use the historical method
# NOTE: this seems to vary per dataset. If you get an error about no data available and
# your date is less than this many days in the past, try decreasing this number to
# search for historical data more recently.
HISTORICAL_DAYS_THRESHOLD = 90


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

    def _local_start_of_today(self):
        return pd.Timestamp("now", tz=self.default_timezone).floor("d")

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
        method="GET",
        api_params=None,
        data=None,
        parse_json=True,
        verbose=False,
    ):
        log(f"Requesting url: {url} with params: {api_params}", verbose)

        if method == "GET":
            response = requests.get(url, params=api_params, headers=self.headers())
        elif method == "POST":
            response = requests.post(
                url,
                params=api_params,
                headers=self.headers(),
                data=data,
            )
        else:
            raise ValueError("Unsupported method")

        if parse_json:
            return response.json()
        else:
            return response.content

    def get_public_reports(self, verbose=False):
        # General information about the public reports
        return self.make_api_call(BASE_URL, verbose=verbose)

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

        end = end or (date + pd.Timedelta(days=1))

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
                page_size=500_000,
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
        """Get Ancillary Services Reports

        Arguments:
            date (str): the date to fetch reports for.
            end (str, optional): the end date to fetch reports for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with ancillary services reports
        """
        if date in ["latest", "today"]:
            raise ValueError("Cannot get AS reports for 'latest' or 'today'")

        # Published with a 2-day delay
        report_date = date.normalize() + pd.DateOffset(days=2)
        end = end or (report_date + pd.Timedelta(days=1))

        urls = self._get_historical_data_links(
            emil_id=AS_REPORTS_EMIL_ID,
            start_date=report_date,
            end_date=end,
            verbose=verbose,
        )

        dfs = [
            self.ercot._handle_as_reports_file(
                url,
                verbose=verbose,
                headers=self.headers(),
            )
            for url in urls
        ]

        return pd.concat(dfs).reset_index(drop=True).drop(columns=["Time"])

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

        end = end or (date + pd.Timedelta(days=1))

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
                page_size=500_000,
                verbose=verbose,
                **api_params,
            )

        data = self.ercot._handle_lmp(
            docs=None,
            verbose=verbose,
            df=data.rename(columns={"RepeatHourFlag": "RepeatedHourFlag"}),
        )

        return data.sort_values(["Interval Start", "Location"]).reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_hourly_resource_outage_capacity(self, date, end=None, verbose=False):
        """Get Hourly Resource Outage Capacity Reports

        Arguments:
            date (str): the date to fetch reports for.
            end (str, optional): the end date to fetch reports for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with hourly resource outage capacity reports
        """
        if date == "latest":
            return self.get_hourly_resource_outage_capacity("today", verbose=verbose)

        end = end or (date + pd.Timedelta(days=1))

        if self._should_use_historical(date):
            data = self.get_historical_data(
                endpoint=HOURLY_RESOURCE_OUTAGE_CAPACITY_REPORTS_ENDPOINT,
                start_date=date,
                end_date=end,
                verbose=verbose,
            )
        else:
            api_params = {
                "operatingDateFrom": date,
                "operatingDateTo": end,
            }

            data = self.hit_ercot_api(
                endpoint=HOURLY_RESOURCE_OUTAGE_CAPACITY_REPORTS_ENDPOINT,
                page_size=500_000,
                verbose=verbose,
                **api_params,
            )

        self.ercot.parse_doc(data, verbose=verbose)

        return self.ercot._handle_hourly_resource_outage_capacity(
            doc=None, verbose=verbose, df=data
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
        end = end or (date + pd.Timedelta(days=1))

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
                page_size=500_000,
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
        end = end or (date + pd.Timedelta(days=1))

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
                page_size=50_000,
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
        end = end or (date + pd.Timedelta(days=1)).normalize()

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
                page_size=50_000,
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

    def get_historical_data(
        self,
        endpoint,
        start_date,
        end_date,
        read_as_csv=True,
        sleep_seconds=0.25,
        verbose=False,
    ):
        """Retrieves historical data from the given emil_id from start to end date.
        The historical data endpoint only allows filtering by the postDatetimeTo and
        postDatetimeFrom parameters. The retrieval process has two steps:

        1. Get the links to download the historical data
        2. Download the historical data from the links

        Arguments:
            endpoint [str]: a string representing a specific ERCOT API endpoint.
            start_date [date]: the start date for the historical data
            end_date [date]: the end date for the historical data
            sleep_seconds [float]: the number of seconds to sleep between requests.
                Increase if you are getting rate limited.
            verbose [bool]: if True, will print out status messages

        Returns:
            [pandas.DataFrame]: a dataframe of historical data
        """
        emil_id = endpoint.split("/")[1]
        links = self._get_historical_data_links(emil_id, start_date, end_date, verbose)

        if not links:
            raise NoDataFoundException(
                f"No historical data links found for {endpoint} with",
                f"time range {start_date} to {end_date}",
            )

        dfs = []

        for link in tqdm(
            links,
            desc="Fetching historical data",
            ncols=80,
            disable=not verbose,
        ):
            try:
                # Data comes back as a compressed zip file.
                response = self.make_api_call(link, verbose=verbose, parse_json=False)

                # Convert the bytes to a file-like object
                bytes = pd.io.common.BytesIO(response)

                if not read_as_csv:
                    dfs.append(bytes)
                else:
                    # Decompress the zip file and read the csv
                    dfs.append(pd.read_csv(bytes, compression="zip"))

                # Necessary to avoid rate limiting
                time.sleep(sleep_seconds)

            except Exception as e:
                print(f"Link: {link} failed with error: {e}")
                if response.status_code == 429:
                    # Rate limited, so sleep for a longer time
                    log(
                        f"Rate limited. Sleeping for {sleep_seconds * 10} seconds",
                        verbose,
                    )
                    time.sleep(sleep_seconds * 10)
                continue

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

        links = [
            archive.get("_links").get("endpoint").get("href") for archive in archives
        ]

        log(f"Found {len(links)} archives", verbose)

        return links

    def hit_ercot_api(
        self,
        endpoint: str,
        page_size: int = 100_000,
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
                to 100_000 because otherwise this will be very slow when fetching
                large datasets.
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

                status_code = response.get("statusCode")

                # status code only seems to be present for a failure
                if status_code and status_code != 200:
                    log(f"Error: {response.get('message')}", verbose)
                    break

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
        ErcotAPI().list_all_endpoints()
    elif args.action == "describe":
        ErcotAPI().describe_one_endpoint(args.endpoint)
    else:
        print(f"{args.action} is not a valid action")
        print("Try 'list' or 'describe'")
