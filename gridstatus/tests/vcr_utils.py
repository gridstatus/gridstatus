import functools
import json
import os
import shutil
from urllib.parse import parse_qs, urlencode, urlparse

import pytest
import vcr

# Default record mode:
#   - GitHub Actions (and most CI providers) set CI=true, so we default to
#     "none" — cassettes are pure playback and missing cassettes cause the
#     test to skip via SkipMissingCassetteVCR.
#   - Locally CI is unset, so we default to "new_episodes" — existing
#     cassettes replay and new interactions are recorded from the live API.
# Override with VCR_RECORD_MODE=<mode> to force a specific mode in either
# environment (e.g. VCR_RECORD_MODE=all on a scheduled refresh job to
# rewrite every cassette).
_default_mode = "none" if os.getenv("CI") == "true" else "new_episodes"
RECORD_MODE = os.getenv("VCR_RECORD_MODE", _default_mode)


def date_range_cassette(prefix: str, start, end) -> str:
    """Return ``{prefix}_{start:%Y-%m-%d}_{end:%Y-%m-%d}.yaml``.

    Shortens the otherwise-long inline f-strings used for date-range
    cassette names so they don't need ``# noqa: E501`` escapes. Accepts
    anything :func:`pandas.Timestamp` can parse.
    """
    import pandas as pd

    s = pd.Timestamp(start).strftime("%Y-%m-%d")
    e = pd.Timestamp(end).strftime("%Y-%m-%d")
    return f"{prefix}_{s}_{e}.yaml"


def dummy_credential(label: str) -> str:
    """Return a placeholder credential safe for VCR playback.

    VCR strips real auth headers before writing cassettes (see
    ``filter_headers`` in :func:`setup_vcr`), so in playback mode the
    client only needs a non-empty credential to construct without error.
    Tests that hit the live API must be marked ``@pytest.mark.integration``
    so they are filtered out of the ``record_mode=none`` CI matrix.
    """
    return f"DUMMY_{label}_FOR_VCR_PLAYBACK"


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


def _strip_ercot_cache_buster(uri: str) -> str:
    """Strip cache-busting query parameters that would otherwise break
    VCR URI matching.

    Several of the sources we record append a random number to requests to
    defeat caching. They show up as ``_=NNNNN`` (jQuery-style) or bare
    ``_NNNNN`` query keys. Strip any param whose name starts with ``_`` so
    requests across runs can match.
    """
    parsed = urlparse(uri)
    params = parse_qs(parsed.query, keep_blank_values=True)
    if any(k.startswith("_") for k in params):
        filtered = {k: v for k, v in params.items() if not k.startswith("_")}
        new_query = urlencode(filtered, doseq=True)
        return parsed._replace(query=new_query).geturl()
    return uri


def _ercot_uri_matcher(r1, r2):
    """Custom VCR URI matcher that normalises ERCOT cache-buster params."""
    return _strip_ercot_cache_buster(r1.uri) == _strip_ercot_cache_buster(r2.uri)


def setup_vcr(
    source: str,
    record_mode: str,
) -> SkipMissingCassetteVCR:
    cassette_dir = f"{os.path.dirname(__file__)}/fixtures/{source}/vcr_cassettes"

    if record_mode == "all":
        clean_cassettes(cassette_dir)

    # Several ISO endpoints (ERCOT's IceDocListJsonWS and public-reports, CAISO's
    # outlook/history CSVs) append a random ``_=NNN`` cache-buster query param
    # on every request. Use a custom URI matcher that strips any underscore-
    # prefixed query param so cassettes replay cleanly across runs.
    vcr_instance = vcr.VCR(
        cassette_library_dir=cassette_dir,
        record_mode=record_mode,
        match_on=["ercot_uri", "method"],
        before_record=lambda request: before_record_callback(request, source),
        filter_headers=[
            ("Authorization", "XXXXXX"),
            ("Ocp-Apim-Subscription-Key", "XXXXXX"),
            ("X-Api-Key", "XXXXXX"),
        ],
    )

    vcr_instance.register_matcher("ercot_uri", _ercot_uri_matcher)

    return SkipMissingCassetteVCR(vcr_instance, cassette_dir, record_mode)
