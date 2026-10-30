"""Offline regressions for archive publication folds, independent of delivery folds."""

from unittest.mock import patch

import pandas as pd
import pytest

from gridstatus import ercot_constants
from gridstatus.ercot_api.ercot_api import ErcotAPI


@pytest.mark.parametrize(
    "method,columns_name",
    [
        ("get_wind_actual_and_forecast_hourly", "WIND_ACTUAL_AND_FORECAST_COLUMNS"),
        (
            "get_wind_actual_and_forecast_by_geographical_region_hourly",
            "WIND_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS",
        ),
        ("get_solar_actual_and_forecast_hourly", "SOLAR_ACTUAL_AND_FORECAST_COLUMNS"),
        (
            "get_solar_actual_and_forecast_by_geographical_region_hourly",
            "SOLAR_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS",
        ),
    ],
)
@pytest.mark.parametrize("date", ["2025-11-02", "2026-11-01", "2027-11-07"])
def test_same_publication_clock_time_preserves_both_files(method, columns_name, date):
    api = object.__new__(ErcotAPI)
    columns = getattr(ercot_constants, columns_name)
    values = [10.0, 11.0, 20.0, 21.0]
    raw = pd.DataFrame(
        {
            "DeliveryDate": [date] * 4,
            # Deliberately unsorted delivery times and repeated CSV row indexes.
            "HourEnding": [4, 3, 4, 3],
            "DSTFlag": ["N"] * 4,
            "postDatetime": [f"{date}T01:55:10"] * 4,
            "_source_filename": ["report.csv"] * 2 + ["reportxhr.csv"] * 2,
            **{column: values for column in columns[3:]},
        },
        index=[0, 1, 0, 1],
    )
    with patch.object(api, "get_historical_data", return_value=raw) as fetch:
        result = getattr(api, method)(date)

    assert fetch.call_args.kwargs["include_source_filename"] is True
    assert result.columns.tolist() == columns
    assert len(result) == 4
    assert not result.duplicated(["Interval Start", "Publish Time"]).any()
    for value in values:
        row = result.loc[result[columns[3]] == value].iloc[0]
        expected = pd.Timestamp(f"{date} 01:55:10").tz_localize(
            api.default_timezone,
            ambiguous=value < 20,
        )
        assert row["Publish Time"] == expected


@pytest.mark.parametrize("filename", [None, "report.csv", "reportXHR.csv"])
def test_filename_fold_and_normal_day(filename):
    api = object.__new__(ErcotAPI)
    dates = pd.Series(["2026-11-01T01:55:10", "2026-11-02T01:55:10"])
    raw = pd.DataFrame({"postDatetime": dates, "_source_filename": filename})
    mask = api._determine_ambiguous_timezone_for_publish_time(dates, raw)
    result = pd.to_datetime(dates).dt.tz_localize(api.default_timezone, ambiguous=mask)
    assert result.iloc[0].utcoffset() == pd.Timedelta(
        hours=-6 if filename == "reportXHR.csv" else -5,
    )
    assert result.iloc[1].utcoffset() == pd.Timedelta(hours=-6)


def test_missing_filename_column_preserves_default():
    api = object.__new__(ErcotAPI)
    dates = pd.Series(["2026-11-01T01:55:10"])
    assert api._determine_ambiguous_timezone_for_publish_time(dates).tolist() == [True]
