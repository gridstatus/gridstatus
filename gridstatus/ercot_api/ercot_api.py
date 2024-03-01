import argparse
import io
import os
import time
import zipfile
from typing import Optional

import pandas as pd
import requests
from tqdm import tqdm

from gridstatus.ercot_api.api_parser import get_endpoints_map
from gridstatus.gs_logging import log

TOKEN_URL = "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"  # noqa
BASE_URL = "https://api.ercot.com/api/public-reports"

DAM_EMIL_ID = "NP4-183-CD"

TOKEN_EXPIRATION_SECONDS = 3600


class AuthenticatedErcotApi:
    """
    Class to authenticate and make requests to the ERCOT API

    "https://developer.ercot.com/applications/pubapi/ERCOT%20Public%20API%20Registration%20and%20Authentication/"
    """  # noqa

    def __init__(
        self,
        username: str = None,
        password: str = None,
        subscription_key: str = None,
    ):
        self.username = username or os.getenv("ERCOT_USERNAME")
        self.password = password or os.getenv("ERCOT_PASSWORD")
        self.subscription_key = subscription_key or os.getenv("ERCOT_SUBSCRIPTION_KEY")

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
        data=None,
        parse_json=True,
        verbose=False,
    ):
        self.refresh_token_if_needed()
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Ocp-Apim-Subscription-Key": self.subscription_key,
        }

        log(f"Requesting url: {url}", verbose)

        if method == "GET":
            response = requests.get(url, headers=headers)
        elif method == "POST":
            response = requests.post(url, headers=headers, data=data)
        else:
            raise ValueError("Unsupported method")

        if parse_json:
            return response.json()
        else:
            return response.content

    def get_zip_file(self, emil_id, document_id, verbose=False):
        url = f"{BASE_URL}/archive/{emil_id}?download={document_id}"

        content = self.make_api_call(url, parse_json=False, verbose=verbose)

        # Use BytesIO for the byte stream
        zip_file = io.BytesIO(content)

        # Open the zip file
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
        return self.make_api_call(BASE_URL, verbose=verbose)

    def get_archive_for_data_product(self, emil_id, verbose=False):
        url = f"{BASE_URL}/archive/{emil_id}"

        response = self.make_api_call(url, verbose=verbose)

        archives = response.get("archives")

        if not archives:
            raise ValueError(f"No archives found for {emil_id}")

        log(f"Found {len(archives)} archives for {emil_id}", verbose)

        return archives

    def get_document_ids_to_download(
        self,
        emil_id,
        start_date,
        end_date=None,
        verbose=False,
    ):
        archives = self.get_archive_for_data_product(emil_id, verbose=verbose)

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

        return document_ids

    def get_dam_hourly(self, start_date, end_date=None, verbose=False):
        document_ids = self.get_document_ids_to_download(
            DAM_EMIL_ID,
            start_date,
            end_date,
            verbose=verbose,
        )

        dfs = []

        for doc_id in tqdm(document_ids, desc="Downloading documents"):
            df = self.get_zip_file(DAM_EMIL_ID, document_id=doc_id, verbose=verbose)
            dfs.append(df)

        data = pd.concat(dfs)

        return data


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
