import json
import os
from urllib.parse import parse_qs, urlparse

import vcr

# NOTE(Kladar): Set VCR_RECORD_MODE to "all" to update the fixtures as an integration test,
# say on a weekly or monthly job.
record_mode = os.environ.get("VCR_RECORD_MODE", "once")


def before_record_callback(
    request: vcr.request.Request,
    date_range_endpoints: list[str],
) -> vcr.request.Request:
    parsed_url = urlparse(request.uri)
    path_parts = parsed_url.path.split("/")

    if any(endpoint in path_parts for endpoint in date_range_endpoints):
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


def setup_vcr(
    source: str,
    record_mode: str,
    date_range_endpoints: list[str],
) -> vcr.VCR:
    return vcr.VCR(
        cassette_library_dir=f"{os.path.dirname(__file__)}/fixtures/{source}/vcr_cassettes",
        record_mode=record_mode,
        match_on=["uri", "method"],
        before_record=before_record_callback(date_range_endpoints=date_range_endpoints),
    )
