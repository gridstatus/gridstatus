import json
import os
import shutil
from urllib.parse import parse_qs, urlparse

import vcr

# NOTE(Kladar): Set VCR_RECORD_MODE to "all" to update the fixtures as an integration test,
# say on a weekly or monthly job.
RECORD_MODE = os.environ.get("VCR_RECORD_MODE", "all")

# NOTE(Kladar): Since these methodssupport date ranges and make multiple API calls in a single get
# request, vcr needs to combine them into a single cassette.
# TODO(Kladar): Figure out the best place to put these in a source-specific manner.
DATE_RANGE_METHODS = [
    "realtimehourlydemand",
    "dayaheadhourlydemand",
    "hourlyloadforecast",
    "reliabilityregionloadforecast",
]


def before_record_callback(
    request: vcr.request.Request,
) -> vcr.request.Request:
    parsed_url = urlparse(request.uri)
    path_parts = parsed_url.path.split("/")

    if any(endpoint in path_parts for endpoint in DATE_RANGE_METHODS):
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


def setup_vcr(
    source: str,
    record_mode: str,
) -> vcr.VCR:
    cassette_dir = f"{os.path.dirname(__file__)}/fixtures/{source}/vcr_cassettes"

    if record_mode == "all":
        clean_cassettes(cassette_dir)

    return vcr.VCR(
        cassette_library_dir=cassette_dir,
        record_mode=record_mode,
        match_on=["uri", "method"],
        before_record=before_record_callback,
    )
