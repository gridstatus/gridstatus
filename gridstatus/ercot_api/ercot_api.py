import requests
from typing import Optional

import pandas as pd

from gridstatus.ercot_api.api_parser import get_endpoints_map


BASE_URL = "https://api.ercot.com/api/public-reports"


def hit_ercot_api(
    endpoint: str,
    page_size: Optional[int] = None,
    max_pages: Optional[int] = None,
    **api_params,
) -> pd.DataFrame:
    """Retrieves data from the given endpoint of the ERCOT API

    Arguments:
        endpoint: a string representing a specific ERCOT API endpoint.
            examples: "/np6-345-cd/act_sys_load_by_wzn", "/np6-787-cd/lmp_electrical_bus"
        page_size: if provided, specifies the number of results to return per page
        max_pages: if provided, will stop paginating after reaching this number.
            Useful in testing to avoid long-running queries, but may result in incomplete data
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
    
    # determine parameters and types for endpoint, validate and parse api_params
    parsed_api_params = []
    for arg, value in api_params.items():
        parser = endpoint_contents["parameters"].get(arg, {}).get("parser")
        if parser is not None:
            parsed_api_params.append((arg, parser(value)))

    # prepare url string
    querystring = "&".join(
        [f"{arg}={value}" for arg, value in parsed_api_params]
    )
    urlstring = f"{BASE_URL}{endpoint}?{querystring}"
    if page_size is not None:
        urlstring += f"&size={page_size}"

    # make requests, paginating as needed
    current_page = 1
    total_pages = 1
    data_results = []
    columns = None

    while current_page <= total_pages:
        if max_pages is not None and current_page > max_pages:
            break
        response = requests.get(f"{urlstring}&page={current_page}").json()
        data_results.extend(response["data"])
        if columns is None:
            # only on first request/iteration: populate columns and update total pages
            columns = [f["name"] for f in response["fields"]]
            total_pages = response["_meta"]["query"]["totalPages"]
        current_page += 1

    # prepare and return dataframe of results
    return pd.DataFrame(
        data=data_results,
        columns=columns,
    )


def endpoint_help(endpoint: str) -> None:
    """Prints details about a given endpoint"""
    endpoint_contents = get_endpoints_map().get(endpoint, None)
    if endpoint_contents is None:
        print(f"{endpoint} is not a valid ERCOT API endpoint")
        return
    
    print(f"Endpoint: {endpoint}")
    print(f"Summary:  {endpoint_contents['summary']}")
    print(f"Parameters:")
    for param, details in sorted(endpoint_contents["parameters"].items()):
        print(f"    {param} - {details['value_type']}")


def list_all_endpoints() -> None:
    """Prints all available endpoints"""
    endpoints = get_endpoints_map()
    for endpoint, contents in sorted(endpoints.items()):
        print(endpoint)
        print(f"    {contents['summary']}")


if __name__ == "__main__":
    pass