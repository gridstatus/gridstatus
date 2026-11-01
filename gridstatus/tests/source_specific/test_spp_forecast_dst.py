from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from gridstatus import SPP

FIXTURES = Path(__file__).parents[1] / "fixtures" / "spp_forecast_dst_2026"
SHORT_FEEDS = [
    ("get_load_forecast_short_term", "OP-STLF", "STLF", "STLF"),
    (
        "get_solar_and_wind_forecast_short_term",
        "OP-STRF",
        "Wind Forecast MW",
        "Wind Forecast MW",
    ),
]
MID_FEEDS = [
    ("get_load_forecast_mid_term", "OP-MTLF", "MTLF", "MTLF"),
    (
        "get_solar_and_wind_forecast_mid_term",
        "OP-MTRF",
        "Wind Forecast MW",
        "Wind Forecast MW",
    ),
    (
        "get_solar_and_wind_forecast_by_reserve_zone",
        "RF_RESERVE_ZONE",
        "WindForecastMW",
        "Wind Forecast",
    ),
]
SHORT_CASES = [
    ("01:00-05:00", "0100"),
    ("01:05-05:00", "0105"),
    ("01:55-05:00", "0155"),
    ("01:00-06:00", "0100d"),
    ("01:05-06:00", "0105d"),
    ("01:55-06:00", "0155d"),
    ("02:00-06:00", "0200"),
]
MID_CASES = [
    ("00:00-05:00", "0000"),
    ("01:00-05:00", "0100"),
    ("01:00-06:00", "0200"),
    ("02:00-06:00", "0200d"),
    ("03:00-06:00", "0300"),
]


@pytest.mark.parametrize(
    "feed,clock,file_time",
    [(feed, clock, name) for feed in SHORT_FEEDS for clock, name in SHORT_CASES]
    + [(feed, clock, name) for feed in MID_FEEDS for clock, name in MID_CASES],
)
def test_forecast_dst_source_files(feed, clock, file_time):
    method, prefix, source_value, output_value = feed
    iso = SPP()
    source = pd.read_csv(FIXTURES / f"{prefix}.csv")
    filename = f"{prefix}-20261101{file_time}.csv"
    expected = source[source.SourceFile == filename].drop(columns="SourceFile")
    assert not expected.empty
    expected.columns = expected.columns.str.strip()
    requested = pd.Timestamp(f"2026-11-01T{clock}")

    def read_source(url):
        assert url.endswith("/" + filename)
        if prefix in ("OP-STLF", "OP-STRF"):
            assert f"/2026/11/01/{int(file_time[:2]) + 1:02d}/" in url
        return expected.copy()

    with patch("gridstatus.spp.pd.read_csv", side_effect=read_source):
        result = getattr(iso, method)(requested)

    assert result["Publish Time"].eq(requested).all()
    gmt_column = "GMTInterval" if "GMTInterval" in expected else "GMTIntervalEnd"
    assert sorted(result["Interval End"].dt.tz_convert("UTC")) == sorted(
        pd.to_datetime(expected[gmt_column], utc=True),
    )
    assert sorted(result[output_value]) == sorted(expected[source_value])
    assert len(result) == len(expected)


@pytest.mark.parametrize("feed", MID_FEEDS)
def test_hourly_forecast_range_keeps_both_publications(feed):
    method, prefix, _, _ = feed
    iso = SPP()
    source = pd.read_csv(FIXTURES / f"{prefix}.csv")
    calls = []

    def read_source(url):
        filename = url.rsplit("/", 1)[1]
        calls.append(filename)
        frame = source[source.SourceFile == filename].drop(columns="SourceFile")
        assert not frame.empty
        return frame.copy()

    with patch("gridstatus.spp.pd.read_csv", side_effect=read_source):
        result = getattr(iso, method)(
            "2026-11-01T00:00-05:00",
            "2026-11-01T03:00-06:00",
        )
    expected = pd.date_range("2026-11-01T05:00Z", periods=4, freq="h")
    assert sorted(result["Publish Time"].dt.tz_convert("UTC").unique()) == list(
        expected,
    )
    assert calls == [
        f"{prefix}-20261101{suffix}.csv" for suffix in ("0000", "0100", "0200", "0200d")
    ]


@pytest.mark.parametrize("forecast_type", ["SHORT_TERM", "MID_TERM"])
@pytest.mark.parametrize(
    "day,hour",
    [("2026-10-31", "02"), ("2026-11-02", "02"), ("2026-03-08", "03")],
)
def test_non_fallback_publication_is_unchanged(forecast_type, day, hour):
    iso = SPP()
    result = iso._forecast_publish_time(
        f"https://example.com/OP-FORECAST-{day.replace('-', '')}{hour}00.csv",
        forecast_type,
    )
    assert result == pd.Timestamp(f"{day} {hour}:00", tz=iso.default_timezone)


@pytest.mark.parametrize("forecast_type", ["SHORT_TERM", "MID_TERM"])
@pytest.mark.parametrize("clock", ["01:00-05:00", "01:00-06:00", "02:00-06:00"])
def test_latest_forecast_uses_buffered_publication(forecast_type, clock):
    iso = SPP()
    requested = pd.Timestamp(f"2026-11-01T{clock}").tz_convert(iso.default_timezone)
    short = forecast_type == "SHORT_TERM"
    buffer = 2 if short else 10
    method = (
        iso._get_short_term_forecast_data if short else iso._get_mid_term_forecast_data
    )
    with (
        patch.object(iso, "now", return_value=requested + pd.Timedelta(minutes=buffer)),
        patch("gridstatus.spp.pd.read_csv", return_value=pd.DataFrame()),
    ):
        _, url = method("latest", "https://example.com", "OP-FORECAST")
    assert iso._forecast_publish_time(url, forecast_type) == requested
