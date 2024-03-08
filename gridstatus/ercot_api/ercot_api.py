import argparse
import os
import time
from typing import Optional

import pandas as pd
import requests
from tqdm import tqdm

from gridstatus import utils
from gridstatus.base import Markets, NoDataFoundException
from gridstatus.decorators import support_date_range
from gridstatus.ercot import ELECTRICAL_BUS_LOCATION_TYPE, Ercot
from gridstatus.ercot_api.api_parser import parse_all_endpoints
from gridstatus.gs_logging import log

TOKEN_URL = "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"  # noqa
BASE_URL = "https://api.ercot.com/api/public-reports"

# This file is publicly available and contains the full list of endpoints and their parameters # noqa
ENDPOINTS_MAP_URL = "https://raw.githubusercontent.com/ercot/api-specs/main/pubapi/pubapi-apim-api.json"  # noqa

# https://data.ercot.com/data-product-archive/NP4-183-CD
DAM_LMP_HOURLY_EMIL_ID = "NP4-183-CD"
DAM_LMP_ENDPOINT = "/np4-183-cd/dam_hourly_lmp"

SHADOW_PRICES_DAM_ENDPOINT = "/np4-191-cd/dam_shadow_prices"
SHADOW_PRICES_SCED_ENDPOINT = "/np6-86-cd/shdw_prices_bnd_trns_const"

# How long a token lasts for before needing to be refreshed
TOKEN_EXPIRATION_SECONDS = 3600


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

    def make_api_call(
        self,
        url,
        method="GET",
        api_params=None,
        data=None,
        parse_json=True,
        verbose=False,
    ):
        self.refresh_token_if_needed()

        # Both forms of authentication are required
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Ocp-Apim-Subscription-Key": self.subscription_key,
        }

        log(f"Requesting url: {url}", verbose)

        if method == "GET":
            response = requests.get(url, params=api_params, headers=headers)
        elif method == "POST":
            response = requests.post(url, params=api_params, headers=headers, data=data)
        else:
            raise ValueError("Unsupported method")

        if parse_json:
            return response.json()
        else:
            return response.content

    def get_public_reports(self, verbose=False):
        # General information about the public reports
        return self.make_api_call(BASE_URL, verbose=verbose)

    @support_date_range(frequency="DAY_START")
    def get_lmp_by_bus_dam(self, date, end=None, verbose=False):
        """
        Retrieves the hourly Day Ahead Market (DAM) Location Marginal Prices (LMPs)
        for the given date range.

        Data source: https://data.ercot.com/data-product-archive/NP4-183-CD
        (requires login)
        """
        # Since we are filtering on the deliveryDate, there's no need to subtract a day
        # even though this is a day ahead market
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).date() + pd.Timedelta(
                days=1,
            )

        # Assume if there's only a start date, fetch data for that day only
        end = end or date

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
        # Get data for today through tomorrow (because the data for tomorrow will not
        # always be available)
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).date()
            end = date + pd.Timedelta(days=1)

        # Assume if there's only a start date, fetch data for that day only
        end = end or date

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
        # Query for the past two hours because the data is published every hour
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).floor("H") - pd.Timedelta(
                hours=1,
            )
            end = date + pd.Timedelta(hours=2)

        # Assume if no end date is provided, we only want data for the given date
        end = end or date.normalize() + pd.Timedelta(days=1)

        api_params = {
            "SCEDTimestampFrom": date - pd.Timedelta(hours=1),
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
        data.loc[:, "Limiting Facility"] = (
            data["From Station"]
            + "_"
            + data["From Station kV"].astype(str)
            + "_"
            + data["To Station"]
            + "_"
            + data["To Station kV"].astype(str)
        )

        data.loc[data["Contingency Name"] == "BASE CASE", "Limiting Facility"] = pd.NA

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

        # make requests, paginating as needed
        current_page = 1
        total_pages = 1
        data_results = []
        columns = None

        with tqdm(desc="Paginating results", ncols=80) as progress_bar:
            while current_page <= total_pages:
                if max_pages is not None and current_page > max_pages:
                    break

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

                # this section runs on first request/page only
                if columns is None:
                    columns = [f["name"] for f in response["fields"]]
                    # ensure that there is data before proceeding
                    # note: this logic may be vulnerable to API changes!
                    if "data" not in response or "_meta" not in response:
                        break
                    total_pages = response["_meta"]["totalPages"]
                    # determine number-of-pages denominator for progress bar
                    if max_pages is None:
                        denominator = total_pages
                    else:
                        denominator = min(total_pages, max_pages)

                        if denominator < total_pages:
                            # User requested fewer pages than total
                            print(
                                f"warning: only retrieving {max_pages} pages "
                                f"out of {total_pages} total",
                            )

                    progress_bar.total = denominator
                    progress_bar.refresh()

                data_results.extend(response["data"])
                progress_bar.update(1)
                current_page += 1

        if not data_results:
            raise NoDataFoundException(
                f"No data found for {endpoint} with params {api_params}",
            )

        # Capitalize the first letter of each column name but leave the rest alone
        columns = [col[:1].upper() + col[1:] for col in columns]

        # Strip the extra whitespace from the data
        data = pd.DataFrame(data=data_results, columns=columns)
        data = data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        return data

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
