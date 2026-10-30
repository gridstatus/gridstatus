from unittest.mock import Mock

import pandas as pd
import pytest
import requests

from gridstatus import EPA
from gridstatus.base import NoDataFoundException
from gridstatus.epa.epa_constants import POWER_PLANT_EMISSIONS_GENERATION_COLUMNS


@pytest.fixture
def epa():
    client = EPA(api_key="test-key")
    client.session.get = Mock()
    return client


def response(payload):
    return Mock(json=lambda: payload, raise_for_status=Mock())


def attributes(facility=591, latitude=39.7302, longitude=-75.5371):
    return {
        "facilityId": facility,
        "unitId": "01",
        "latitude": latitude,
        "longitude": longitude,
    }


def hourly(day="2023-07-01", hour=0, facility=591):
    return {
        "facilityId": facility,
        "unitId": "01",
        "date": day,
        "hour": hour,
        "grossLoad": 22.5,
    }


@pytest.mark.parametrize("day", ["2023-01-01", "2023-07-01", "2023-11-05"])
def test_utc_window_and_facility_timezones(epa, day):
    # Two Florida facilities straddle the Eastern/Central boundary.
    prior = str((pd.Timestamp(day) - pd.Timedelta(days=1)).date())
    rows = [hourly(d, h, f) for d in [prior, day] for h in range(24) for f in [1, 2]]
    attrs = [attributes(1, 30.4, -87.2), attributes(2, 25.8, -80.2)]
    epa.session.get.side_effect = [response(rows)] + [response({"items": attrs})] * 2
    df = epa.get_power_plant_emissions_generation(day)
    start = pd.Timestamp(day, tz="UTC")
    assert len(df) == 48
    assert df.columns.tolist() == POWER_PLANT_EMISSIONS_GENERATION_COLUMNS
    assert df["Interval Start"].min() == start
    assert df["Interval Start"].max() == start + pd.Timedelta(hours=23)
    assert (df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)).all()
    assert not df.duplicated(["Interval Start", "Facility ID", "Unit ID"]).any()
    assert df["Gross Load"].eq(22.5).all()
    assert df["Unit ID"].eq("01").all()
    # Local midnight is 06 UTC in Pensacola and 05 UTC in Miami, even in July.
    for facility, utc_hour in [(1, 6), (2, 5)]:
        rows = [hourly(day, 0, facility)]
        epa.session.get.side_effect = [response(rows)]
        result = epa.get_power_plant_emissions_generation(day)
        assert result["Interval Start"].iloc[0] == start + pd.Timedelta(hours=utc_hour)


def test_pagination_year_join_and_range(epa):
    epa.FACILITIES_ATTRIBUTES_PAGE_SIZE = 1
    epa.session.get.side_effect = [
        response([hourly("2023-12-31", 23), hourly("2024-01-01")]),
        response({"items": [{**attributes(), "ownerOperator": "old"}]}),
        response({"items": []}),
        response({"items": [{**attributes(), "ownerOperator": "new"}]}),
        response({"items": []}),
        response([hourly("2024-01-02")]),
    ]
    df = epa.get_power_plant_emissions_generation(
        ("2024-01-01", "2024-01-03"),
        state_codes=["DE"],
        error="raise",
    )
    assert df["Owner Operator"].tolist() == ["old", "new", "new"]
    assert epa.session.get.call_count == 6  # Annual attributes reused on day two.
    assert epa.session.get.call_args_list[1].kwargs["params"]["stateCode"] == "DE"


@pytest.mark.parametrize("attrs", [[], [attributes(latitude=float("nan"))]])
def test_missing_coordinates_fail(epa, attrs):
    epa.session.get.side_effect = [response([hourly()]), response({"items": attrs})]
    with pytest.raises(ValueError, match="coordinates"):
        epa.get_power_plant_emissions_generation("2023-07-01")


def test_no_data_and_http_failure(epa):
    epa.session.get.return_value = response([])
    with pytest.raises(NoDataFoundException):
        epa.get_power_plant_emissions_generation("2023-07-01")
    epa.session.get.return_value.raise_for_status.side_effect = requests.HTTPError(
        "429",
    )
    with pytest.raises(requests.HTTPError):
        epa.get_power_plant_emissions_generation("2023-07-01")


def test_requires_epa_key(monkeypatch):
    monkeypatch.delenv("EPA_API_KEY", raising=False)
    monkeypatch.setenv("EIA_API_KEY", "unrelated-key")
    with pytest.raises(ValueError, match="EPA_API_KEY"):
        EPA()
