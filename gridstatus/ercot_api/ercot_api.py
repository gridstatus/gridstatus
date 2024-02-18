import argparse
from typing import Optional

import pandas as pd
import requests
from tqdm import tqdm

from gridstatus.ercot_api.api_parser import get_endpoints_map
from gridstatus.gs_logging import log

BASE_URL = "https://api.ercot.com/api/public-reports"


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
