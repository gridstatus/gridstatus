import argparse
import io
import os
import time
import zipfile
from typing import Optional

import pandas as pd
import requests
from tqdm import tqdm

from gridstatus.decorators import support_date_range
from gridstatus.ercot import Ercot
from gridstatus.ercot_api.api_parser import get_endpoints_map
from gridstatus.gs_logging import log

TOKEN_URL = "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"  # noqa
BASE_URL = "https://api.ercot.com/api/public-reports"

# https://data.ercot.com/data-product-archive/NP4-183-CD
DAM_LMP_HOURLY_EMIL_ID = "NP4-183-CD"

TOKEN_EXPIRATION_SECONDS = 3600


class ErcotAPI:
    """
    Class to authenticate and make requests to the ERCOT Data API (api.ercot.com)

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
        self.token_url = TOKEN_URL
        self.token = None
        self.token_expiry = None

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
        params=None,
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
            response = requests.get(url, params=params, headers=headers)
        elif method == "POST":
            response = requests.post(url, params=params, headers=headers, data=data)
        else:
            raise ValueError("Unsupported method")

        if parse_json:
            return response.json()
        else:
            return response.content

    def get_zip_file(self, emil_id, document_id, verbose=False):
        """
        Retrieves a zip file specified by the emil_id and document_id.

        Args:
            emil_id (str): the EMIL ID of the data product
            document_id (int): the document ID of the specific file to download
        """

        url = f"{BASE_URL}/archive/{emil_id}?download={document_id}"

        content = self.make_api_call(url, parse_json=False, verbose=verbose)

        # Use BytesIO for the byte stream
        zip_file = io.BytesIO(content)

        with zipfile.ZipFile(zip_file, "r") as z:
            # Extract file names (assuming only one file in the zip)
            file_names = z.namelist()
            if len(file_names) == 0:
                raise Exception("No files found in the zip archive")

            # Read the first file as a pandas DataFrame
            with z.open(file_names[0]) as f:
                df = pd.read_csv(f)

        return df

    def get_public_reports(self, verbose=False):
        # General information about the public reports
        return self.make_api_call(BASE_URL, verbose=verbose)

    def get_document_list_for_data_product(
        self,
        emil_id,
        start_date=None,
        end_date=None,
        size=None,
        verbose=False,
    ):
        """
        Retrieves the list of all files available for a given data product. Documents
        are sorted by post date, with the most recent documents appearing first. Using
        a size=1 will return the most recent document.

        Args:
            emil_id (str): the EMIL ID of the data product
            size (int): the number of records to return (max 1000)

        Returns:
            list: a list of dictionaries, each containing metadata about a file
        """
        url = f"{BASE_URL}/archive/{emil_id}"

        params = {
            "size": size,
            "postDatetimeFrom": start_date.date() if start_date else None,
            "postDatetimeTo": end_date.date() if end_date else None,
        }

        params = {k: v for k, v in params.items() if v is not None}

        # TODO: handle pagination. The API returns a maximum of 1000 records at a time
        response = self.make_api_call(url, params=params, verbose=verbose)

        archives = response.get("archives")

        if not archives:
            raise ValueError(f"No archives found for {emil_id}")

        log(f"Found {len(archives)} archives for {emil_id}", verbose)

        return archives

    def get_document_ids_to_download(
        self,
        emil_id,
        start_date,
        size=None,
        end_date=None,
        verbose=False,
    ):
        """
        Finds the document IDs for the emil id files that match a given date range

        Args:
            emil_id (str): the EMIL ID of the data product
            start_date (str): the start date in the format "YYYY-MM-DD"
            end_date (str): the end date in the format "YYYY-MM-DD"
            verbose (bool): if True, will print out status messages

        Returns:
            list: a list of document IDs to download
        """
        archives = self.get_document_list_for_data_product(
            emil_id,
            start_date,
            end_date,
            size=size,
            verbose=verbose,
        )

        if start_date is not None:
            date_range = [str(pd.Timestamp(start_date).date())]
            found_dates = []

            if end_date:
                date_range = [
                    str(date.date()) for date in pd.date_range(start_date, end_date)
                ]

            document_ids = []

            for doc in archives:
                doc_posted_date = doc["postDatetime"].split("T")[0]
                if doc_posted_date in date_range:
                    found_dates.append(doc_posted_date)
                    document_ids.append(doc["docId"])

            log(f"Missing dates: {set(date_range) - set(found_dates)}", verbose)
        else:
            document_ids = [doc["docId"] for doc in archives]

        return document_ids

    @support_date_range(frequency=None)
    def get_dam_lmp_hourly_by_bus(self, date, end=None, verbose=False):
        """
        Retrieves the hourly Day Ahead Market (DAM) Location Marginal Prices (LMPs)
        for the given date range.

        Data source: https://data.ercot.com/data-product-archive/NP4-183-CD
        (requires login)
        """
        # Subtract one day since this is the day ahead market
        date = date if date == "latest" else date - pd.DateOffset(days=1)
        end = end if end is None else end - pd.DateOffset(days=1)

        size = None

        if date == "latest":
            size = 1
            date = None
            end = None

        document_ids = self.get_document_ids_to_download(
            emil_id=DAM_LMP_HOURLY_EMIL_ID,
            start_date=date,
            end_date=end,
            size=size,
            verbose=verbose,
        )

        if len(document_ids) == 1:
            data = self.get_zip_file(
                DAM_LMP_HOURLY_EMIL_ID,
                document_id=document_ids[0],
                verbose=verbose,
            )

        else:
            dfs = []
            for doc_id in tqdm(document_ids, desc="Downloading documents"):
                dfs.append(
                    self.get_zip_file(
                        DAM_LMP_HOURLY_EMIL_ID,
                        document_id=doc_id,
                        verbose=verbose,
                    ),
                )
            data = pd.concat(dfs)

        return self.parse_dam_doc(data)

    def parse_dam_doc(self, data):
        return (
            Ercot()
            .parse_doc(data)
            .rename(columns={"BusName": "Location"})
            .drop(columns=["Time"])
            .sort_values(["Interval Start"])
            .reset_index(drop=True)
        )


def hit_ercot_api(
    endpoint: str,
    page_size: int = 1000,
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
            to 1000 (the ERCOT API default).
        max_pages: if provided, will stop paginating after reaching this number.
            Useful in testing to avoid long-running queries, but may result in
            incomplete data.
        verbose: if True, will print out status messages
        api_params: any additional arguments and values to pass along to the endpoint

    Raises:
        KeyError if the given endpoint does not exist

    Returns:
        a dataframe of results
    """

    # validate endpoint string
    endpoint_contents = get_endpoints_map().get(endpoint, None)
    if endpoint_contents is None:
        raise KeyError(f"{endpoint} is not a valid ERCOT API endpoint")

    # prepare url string
    urlstring = f"{BASE_URL}{endpoint}"

    # determine parameters and types for endpoint, validate and parse api_params
    parsed_api_params = {"size": page_size}
    for arg, value in api_params.items():
        parser = endpoint_contents["parameters"].get(arg, {}).get("parser_method")
        if parser is not None:
            parsed_api_params[arg] = parser(value)

    # make requests, paginating as needed
    current_page = 1
    total_pages = 1
    data_results = []
    columns = None

    with tqdm(
        desc="Paginating results",
        ncols=80,
        total=1,
    ) as progress_bar:
        while current_page <= total_pages:
            if max_pages is not None and current_page > max_pages:
                break
            parsed_api_params["page"] = current_page
            response = requests.get(urlstring, params=parsed_api_params).json()

            log(f"Requesting url: {urlstring}", verbose)

            if response.get("statusCode") != 200:
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
        print("No data results returned, try different query params")

    # prepare and return dataframe of results
    return pd.DataFrame(
        data=data_results,
        columns=columns,
    )


def describe_one_endpoint(endpoint: str) -> None:
    """Prints details about a given endpoint"""
    endpoint_contents = get_endpoints_map().get(endpoint, None)
    if endpoint_contents is None:
        print(f"{endpoint} is not a valid ERCOT API endpoint")
        return

    print(f"Endpoint: {endpoint}")
    print(f"Summary:  {endpoint_contents['summary']}")
    print("Parameters:")
    for param, details in sorted(endpoint_contents["parameters"].items()):
        print(f"    {param} - {details['value_type']}")


def list_all_endpoints() -> None:
    """Prints all available endpoints"""
    endpoints = get_endpoints_map()
    for endpoint, contents in sorted(endpoints.items()):
        print(endpoint)
        print(f"    {contents['summary']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["list", "describe"])
    parser.add_argument("--endpoint", required=False)

    args = parser.parse_args()
    if args.action == "list":  # TODO avoid case match because lower python version
        list_all_endpoints()
    elif args.action == "describe":
        describe_one_endpoint(args.endpoint)
    else:
        print(f"{args.action} is not a valid action")
        print("Try 'list' or 'describe'")
