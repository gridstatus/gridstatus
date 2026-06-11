import json
import os
import shutil
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import vcr

# NOTE(Kladar): Set VCR_RECORD_MODE to "all" to update the fixtures as an integration test,
# say on a weekly or monthly job.
RECORD_MODE = os.getenv("VCR_RECORD_MODE", "new_episodes")

# Map of ISO -> endpoint patterns that require date range handling
DATE_RANGE_METHODS = {
    "isone": [
        "genfuelmix",
        "realtimehourlydemand",
        "dayaheadhourlydemand",
        "hourlyloadforecast",
        "reliabilityregionloadforecast",
    ],
    "pjm": [
        "marginal_value_real_time_5_min",
        "marginal_value_day_ahead_hourly",
        "transmission_constraints_day_ahead_hourly",
    ],
}


def before_record_callback(
    request: vcr.request.Request,
    source: str,
) -> vcr.request.Request:
    parsed_url = urlparse(request.uri)
    path_parts = parsed_url.path.split("/")

    if any(endpoint in path_parts for endpoint in DATE_RANGE_METHODS.get(source, [])):
        query_params = parse_qs(parsed_url.query)
        if "date" in query_params and "end" in query_params:
            key = f"{query_params['date'][0]}_{query_params['end'][0]}"
            if not hasattr(before_record_callback, "requests"):
                before_record_callback.requests = {}
            if key not in before_record_callback.requests:
                before_record_callback.requests[key] = []
            before_record_callback.requests[key].append(request)
            combined_body = combine_requests(before_record_callback.requests[key])
            request.body = combined_body.encode("utf-8")
    return request


def combine_requests(requests: list[vcr.request.Request]) -> str:
    combined = []
    for request in requests:
        combined.append(
            {
                "method": request.method,
                "uri": request.uri,
                "body": request.body,
                "headers": dict(request.headers),
            },
        )
    return json.dumps(combined)


def clean_cassettes(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def _strip_cache_buster_params(uri: str) -> str:
    """Remove cache-buster query params (keys starting with ``_``) from a URI.

    ERCOT's MIS listing endpoint appends a ``&_<epoch>`` query param on every
    request, which breaks VCR URI matching during offline replay. Stripping
    keys that start with ``_`` normalizes both the recorded and replayed URIs.
    """
    parsed = urlparse(uri)
    if not parsed.query:
        return uri
    params = parse_qs(parsed.query, keep_blank_values=True)
    filtered = {k: v for k, v in params.items() if not k.startswith("_")}
    return urlunparse(parsed._replace(query=urlencode(filtered, doseq=True)))


def _match_uri_ignoring_cache_buster(
    r1: vcr.request.Request,
    r2: vcr.request.Request,
) -> bool:
    return _strip_cache_buster_params(r1.uri) == _strip_cache_buster_params(r2.uri)


def setup_vcr(
    source: str,
    record_mode: str,
) -> vcr.VCR:
    cassette_dir = f"{os.path.dirname(__file__)}/fixtures/{source}/vcr_cassettes"

    if record_mode == "all":
        clean_cassettes(cassette_dir)

    vcr_instance = vcr.VCR(
        cassette_library_dir=cassette_dir,
        record_mode=record_mode,
        match_on=["uri_no_cache_buster", "method"],
        before_record=lambda request: before_record_callback(request, source),
        filter_headers=[
            ("Authorization", "XXXXXX"),
            ("Ocp-Apim-Subscription-Key", "XXXXXX"),
            ("X-Api-Key", "XXXXXX"),
        ],
    )
    vcr_instance.register_matcher(
        "uri_no_cache_buster",
        _match_uri_ignoring_cache_buster,
    )
    return vcr_instance
