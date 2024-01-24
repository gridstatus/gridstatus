import datetime

import pytest

from gridstatus.ercot_api.api_parser import VALID_VALUE_TYPES, get_endpoints_map
from gridstatus.ercot_api.ercot_api import hit_ercot_api


def _endpoints_map_check(endpoint_dict: dict) -> list[str]:
    """Applies unit test checks to a single endpoint in the endpoints map.

    Ensures that top-level fields are present, and each parameter has a valid
    "payload" of value_type and parser_method

    Returns empty list if the given endpoint passes the check,
    otherwise returns a list of everything that's wrong, for ease of debugging
    """
    issues = []

    if "summary" not in endpoint_dict:
        issues.append("missing summary")

    parameters = endpoint_dict.get("parameters")
    if parameters is None:
        issues.append("missing parameters")
    else:
        for param, param_dict in parameters.items():
            value_type = param_dict.get("value_type")
            if value_type is None:
                issues.append(f"{param} is missing value_type")
            elif value_type not in VALID_VALUE_TYPES:
                issues.append(f"{param} has invalid value_type {value_type}")
            parser_method = param_dict.get("parser_method")
            if parser_method is None:
                issues.append(f"{param} is missing parser_method")
            elif not callable(parser_method):
                issues.append(f"{param} has an invalid parser_method")
    return issues


def test_get_endpoints_map():
    endpoints_map = get_endpoints_map()

    # update this count as needed, if ercot api evolves to add/remove endpoints
    assert len(endpoints_map) == 102

    # detailed check of all endpoints, fields, and values
    issues = []
    for endpoint, endpoint_dict in endpoints_map.items():
        for issue in _endpoints_map_check(endpoint_dict):
            issues.append([f"{endpoint} - {issue}"])
    assert len(issues) == 0


def test_hit_ercot_api():
    """
    First we test that entering a bad endpoint results in a keyerror
    """
    with pytest.raises(KeyError) as _:
        hit_ercot_api("just a real bad endpoint right here")

    """
    Now a happy path test, using "actual system load by weather zone" endpoint.
    Starting from two days ago should result in 48 hourly values, and there are
        12 columns in the resulting dataframe.
    We are also testing here that datetime objects are correctly parsed into
        the desired date string format that the operatingDayFrom parameter expects.
    """
    two_days_ago = datetime.datetime.now() - datetime.timedelta(days=2)
    actual_by_wzn_endpoint = "/np6-345-cd/act_sys_load_by_wzn"
    two_days_actual_by_wzn = hit_ercot_api(
        actual_by_wzn_endpoint, operatingDayFrom=two_days_ago
    )
    assert two_days_actual_by_wzn.shape == (48, 12)

    """
    Now let's apply a value filter and test it.
    We start by taking the midpoint value between min and max of total load over
        the last two days, then query with a filter of only values above that,
        using the totalFrom parameter. There should be fewer than 48 rows, and all
        values for total load should be greater than the threshold we put in.
    """
    min_load = two_days_actual_by_wzn["total"].min()
    max_load = two_days_actual_by_wzn["total"].max()
    in_between_load = (max_load + min_load) / 2
    higher_loads_result = hit_ercot_api(
        actual_by_wzn_endpoint,
        operatingDayFrom=two_days_ago,
        totalFrom=in_between_load,
    )
    assert len(higher_loads_result["total"]) < 48
    assert all(higher_loads_result["total"] > in_between_load)

    """
    Now we test the page_size and max_pages arguments. We know that our two days
        query returns 48 results, so if we lower page_size to 10 and max_pages to 3,
        we should only get 30 rows total. We can also use this opportunity to test
        that invalid parameter names are silently ignored.
    """
    small_pages_result = hit_ercot_api(
        actual_by_wzn_endpoint,
        page_size=10,
        max_pages=3,
        operatingDayFrom=two_days_ago,
        wowWhatAFakeParameter=True,
        thisOneIsAlsoFake=42,
    )
    assert small_pages_result.shape == (30, 12)
