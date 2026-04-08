import functools
import json
import os
import shutil
from urllib.parse import parse_qs, urlparse

import pytest
import vcr

# NOTE(Kladar): Set VCR_RECORD_MODE to "all" to update the fixtures as an integration test,
# say on a weekly or monthly job.
# In CI, default to "none" (playback only) so tests fail fast if a cassette is missing.
# Locally, default to "new_episodes" so missing cassettes are recorded from live APIs.
_default_mode = "none" if os.getenv("CI") == "true" else "new_episodes"
RECORD_MODE = os.getenv("VCR_RECORD_MODE", _default_mode)

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


class _SkipOrCassette:
    """Wraps a VCR cassette to skip when the file is missing. Works as both
    context manager and decorator (like the original VCR cassette object)."""

    def __init__(self, vcr_instance, cassette_dir, record_mode, path, kwargs):
        self._vcr = vcr_instance
        self._cassette_dir = cassette_dir
        self._record_mode = record_mode
        self._path = path
        self._kwargs = kwargs
        self._cassette_ctx = None

    def _should_skip(self):
        if self._record_mode != "none":
            return False
        full_path = (
            self._path
            if os.path.isabs(self._path)
            else os.path.join(self._cassette_dir, self._path)
        )
        return not os.path.exists(full_path)

    # -- context manager --
    def __enter__(self):
        if self._should_skip():
            pytest.skip(f"VCR cassette not found: {os.path.basename(self._path)}")
        self._cassette_ctx = self._vcr.use_cassette(self._path, **self._kwargs)
        return self._cassette_ctx.__enter__()

    def __exit__(self, *exc_info):
        if self._cassette_ctx is not None:
            return self._cassette_ctx.__exit__(*exc_info)

    # -- decorator --
    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            with self:
                return func(*args, **kw)

        return wrapper


class SkipMissingCassetteVCR:
    """Wrapper around vcr.VCR that skips tests when cassettes are missing in none mode."""

    def __init__(self, vcr_instance: vcr.VCR, cassette_dir: str, record_mode: str):
        self._vcr = vcr_instance
        self._cassette_dir = cassette_dir
        self._record_mode = record_mode

    def __getattr__(self, name):
        return getattr(self._vcr, name)

    def use_cassette(self, path, **kwargs):
        return _SkipOrCassette(
            self._vcr, self._cassette_dir, self._record_mode, path, kwargs
        )


def setup_vcr(
    source: str,
    record_mode: str,
) -> SkipMissingCassetteVCR:
    cassette_dir = f"{os.path.dirname(__file__)}/fixtures/{source}/vcr_cassettes"

    if record_mode == "all":
        clean_cassettes(cassette_dir)

    vcr_config = {
        "cassette_library_dir": cassette_dir,
        "record_mode": record_mode,
        "match_on": ["uri", "method"],
        "before_record": lambda request: before_record_callback(request, source),
        "filter_headers": [
            ("Authorization", "XXXXXX"),
            ("Ocp-Apim-Subscription-Key", "XXXXXX"),
            ("X-Api-Key", "XXXXXX"),
        ],
    }

    return SkipMissingCassetteVCR(vcr.VCR(**vcr_config), cassette_dir, record_mode)
