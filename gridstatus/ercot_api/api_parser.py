import json
from datetime import date, datetime
from typing import Union


META_ENDPOINTS = {
    "/",
    "/version",
    "/{emilId}",
    "/archive/{emilId}",
}


UNIVERSAL_PARAM_NAMES = {"page", "size", "sort", "dir"}


SCHEMA_TYPE_MAP = {
    "string": str,
    "boolean": bool,
    "integer": int,
    "number": float,
}


# outer key: endpoint
# inner key: parameters valid for that endpoint
# inner value: expected type of parameter value and a function to parse it into expected format
EndpointsMap = dict[str, dict[str, tuple[type, callable]]]

# lazily evaluated upon first usage
_endpoints_map: EndpointsMap = None



def _select_schema_parser(format: str) -> callable:
    if format == "yyyy-MM-ddTH24:mm:ss":
        return _timestamp_parser
    elif format == "yyyy-MM-dd":
        return _date_parser
    elif format == "true | false":
        return lambda x: "true" if x else "false"
    else:
        return lambda x: x


def _timestamp_parser(timestamp: Union[str, datetime]) -> str:
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%S")


def _date_parser(datevalue: Union[str, datetime]) -> str:
    if isinstance(datevalue, str):
        datevalue = date.fromisoformat(datevalue)
    return datevalue.strftime("%Y-%m-%d")


def _parse_all_endpoints(apijson: dict) -> EndpointsMap:
    return {
        endpoint_string: _parse_endpoint_contents(contents)
        for endpoint_string, contents in apijson["paths"].items()
        if endpoint_string not in META_ENDPOINTS
    }


def _parse_endpoint_contents(contents: dict) -> dict[str, tuple[type, callable]]:
    """
    pull out unique parameters and their schemas/types
    from a single endpoint payload in the "paths" field of the json api docs
    """
    return {
        p["name"]: (SCHEMA_TYPE_MAP[p["schema"]["type"]], _select_schema_parser(p["schema"]["format"]))
        for p in contents["get"]["parameters"]
        if p["name"] not in UNIVERSAL_PARAM_NAMES
    }


def get_endpoints_map() -> EndpointsMap:
    """Provides access to a parsed map of all data endpoints and their parameters"""
    global _endpoints_map # enable us to edit it in here
    if _endpoints_map is None:
        with open("./pubapi-apim-api.json") as rf:
            apijson = json.load(rf)
        _endpoints_map = _parse_all_endpoints(apijson=apijson)
    return _endpoints_map
