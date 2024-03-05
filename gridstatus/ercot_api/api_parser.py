import types
from datetime import date, datetime
from typing import Union

META_ENDPOINTS = {
    "/",
    "/version",
    "/{emilId}",
    "/archive/{emilId}",
}


UNIVERSAL_PARAM_NAMES = {"page", "size", "sort", "dir"}


VALID_VALUE_TYPES = {
    "string",
    "timestamp",
    "date",
    "minute+second mm:ss",
    "boolean",
    "integer",
    "float",
}


datetime_formats = types.SimpleNamespace()
datetime_formats.DATE = "yyyy-MM-dd"
datetime_formats.MINUTE_SECOND = "mm:ss"
datetime_formats.TIMESTAMP = "yyyy-MM-ddTH24:mm:ss"


"""
_endpoints_map is lazily evaluated upon first usage
its structure is as follows:
{
    endpoint_string: {
        "summary": a summary description
        "parameters": {
            parameter_name: {
                "value_type": a human-readable type like 'timestamp' or 'boolean'
                "parser": a callable parser, used internally to ensure the correct
                    format is passed to the API
            }
        }
    }
}
"""
_endpoints_map: dict = None


def _timestamp_parser(timestamp: Union[str, datetime]) -> str:
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%S")


def _date_parser(datevalue: Union[str, datetime]) -> str:
    if isinstance(datevalue, str):
        datevalue = date.fromisoformat(datevalue)
    return datevalue.strftime("%Y-%m-%d")


def _minute_second_parser(timestamp: Union[str, datetime]) -> str:
    if isinstance(timestamp, str):
        # assumes string is in correct mm:ss format
        return timestamp
    return timestamp.strftime("%M:%S")


def _bool_parser(boolvalue: Union[str, bool]) -> str:
    if isinstance(boolvalue, bool):
        return "true" if boolvalue else "false"
    else:
        return boolvalue.lower()


def parse_all_endpoints(apijson: dict) -> dict:
    return {
        endpoint_string: _parse_endpoint_contents(contents)
        for endpoint_string, contents in apijson["paths"].items()
        if endpoint_string not in META_ENDPOINTS
    }


def _parse_endpoint_contents(contents: dict) -> dict:
    """Unpacks parameter info and a summary from the endpoint docs"""
    results = {"summary": contents["get"]["summary"], "parameters": {}}
    for p in contents["get"]["parameters"]:
        if p["name"] not in UNIVERSAL_PARAM_NAMES:
            value_type, parser_method = _parse_schema(p["schema"])
            results["parameters"][p["name"]] = {
                "value_type": value_type,
                "parser_method": parser_method,
            }
    return results


def _parse_schema(schema: dict) -> tuple[str, callable]:
    """Selects a parser method for a given parameter, using its schema dict

    This makes life easier on the caller by ensuring that diverse but valid
        user inputs are parsed correctly into the string format expected by
        the ERCOT API, i.e. "yyyy-MM-ddTH24:mm:ss" for timestamps
    """
    t = schema["type"]
    if t == "string":
        f = schema["format"]
        if f == datetime_formats.TIMESTAMP:
            return ("timestamp", _timestamp_parser)
        elif f == datetime_formats.DATE:
            return ("date", _date_parser)
        elif f == datetime_formats.MINUTE_SECOND:
            return ("minute+second mm:ss", _minute_second_parser)
        else:
            return ("string", lambda x: x)
    elif t == "boolean":
        return ("boolean", _bool_parser)
    elif t == "integer":
        return ("integer", lambda i: int(i))
    elif t == "number":
        return ("float", lambda f: float(f))
    else:
        raise TypeError(
            f"unexpected schema type {schema['type']} and format {schema['format']}",
        )
