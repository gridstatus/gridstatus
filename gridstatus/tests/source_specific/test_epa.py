import os
from io import BytesIO
from unittest.mock import Mock

import pandas as pd
import pytest
import requests
from urllib3 import HTTPResponse
from urllib3.connectionpool import HTTPSConnectionPool

from gridstatus import EPACAM
from gridstatus.base import NoDataFoundException, NotSupported
from gridstatus.epa.epa_constants import POWER_PLANT_EMISSIONS_GENERATION_COLUMNS


@pytest.fixture
def epa():
    client = EPACAM(api_key="test-key")
    client.session.get = Mock()
    return client


def response(payload):
    return Mock(status_code=200, json=lambda: payload, raise_for_status=Mock())


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
        "unit_id": 123,
        "date": day,
        "hour": hour,
        "grossLoad": 22.5,
        "so2MassMeasureFlg": "Measured",
        "so2RateMeasureFlg": "Calculated",
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
    assert "Unit ID 2" not in df.columns
    assert df["SO2 Mass Measure Flag"].eq("Measured").all()
    assert df["SO2 Rate Measure Flag"].eq("Calculated").all()
    assert df.loc[df["Facility ID"].eq(1), "Latitude"].eq(30.4).all()
    assert df.loc[df["Facility ID"].eq(1), "Longitude"].eq(-87.2).all()
    # Local midnight is 06 UTC in Pensacola and 05 UTC in Miami, even in July.
    for facility, utc_hour in [(1, 6), (2, 5)]:
        rows = [hourly(day, 0, facility)]
        epa.session.get.side_effect = [response(rows)]
        result = epa.get_power_plant_emissions_generation(day)
        assert result["Interval Start"].iloc[0] == start + pd.Timedelta(hours=utc_hour)


def test_pagination_year_join_and_range(epa):
    # Leland Olds changed from Mountain to Central time during 2010.
    attrs = attributes(latitude=47.2819, longitude=-101.3194)
    epa.FACILITIES_ATTRIBUTES_PAGE_SIZE = 1
    epa.session.get.side_effect = [
        response(
            [hourly("2010-12-31", 23), hourly("2011-01-01"), hourly("2011-01-02")],
        ),
        response({"items": [{**attrs, "ownerOperator": "old"}]}),
        response({"items": []}),
        response({"items": [{**attrs, "ownerOperator": "new"}]}),
        response({"items": []}),
        response([hourly("2011-01-02")]),
    ]
    df = epa.get_power_plant_emissions_generation(
        ("2011-01-01", "2011-01-03"),
        state_codes=["ND"],
        error="raise",
    )
    assert df["Owner Operator"].tolist() == ["old", "new", "new"]
    assert df["Interval Start"].tolist() == list(
        pd.to_datetime(
            ["2011-01-01 05:00", "2011-01-01 06:00", "2011-01-02 06:00"],
            utc=True,
        ),
    )
    assert epa.session.get.call_count == 5  # One hourly request, four attribute pages.
    cached = epa.get_power_plant_emissions_generation("2011-01-02", state_codes=["ND"])
    assert cached["Owner Operator"].tolist() == ["new"]
    assert epa.session.get.call_count == 6  # Annual attributes reused across calls.
    assert epa.session.get.call_args_list[1].kwargs["params"]["stateCode"] == "ND"


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("EPA_API_KEY"), reason="EPA_API_KEY is required")
def test_power_plant_emissions_generation_live():
    df = EPACAM().get_power_plant_emissions_generation("2011-01-01", state_codes=["ND"])
    assert not df.empty
    assert df.columns.tolist() == POWER_PLANT_EMISSIONS_GENERATION_COLUMNS
    assert df["Interval Start"].dtype == "datetime64[ns, UTC]"
    assert df["Interval End"].dtype == "datetime64[ns, UTC]"
    assert df["Facility ID"].dtype == "Int64"
    assert df["Unit ID"].dtype == "string"
    assert df["State"].eq("ND").all()
    assert df[["Latitude", "Longitude"]].notna().all().all()
    assert not df.duplicated(["Interval Start", "Facility ID", "Unit ID"]).any()
    assert (df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)).all()
    assert df["Interval Start"].min() == pd.Timestamp("2011-01-01", tz="UTC")
    assert df["Interval End"].max() == pd.Timestamp("2011-01-02", tz="UTC")
    leland_olds = df.loc[df["Facility ID"].eq(2817) & df["Unit ID"].eq("1")]
    assert len(leland_olds) == 24
    assert leland_olds["Gross Load"].gt(0).all()


def test_partial_range_normalizes_timezone_and_preserves_end(epa):
    rows = [
        hourly(day, hour)
        for day in ["2023-06-30", "2023-07-01", "2023-07-02"]
        for hour in range(24)
    ]
    epa.session.get.side_effect = [
        response(rows),
        response({"items": [attributes()]}),
    ]
    df = epa.get_power_plant_emissions_generation(
        "2023-07-01T03:00:00-04:00",
        end="2023-07-02T05:00:00-04:00",
        error="raise",
    )
    assert epa.session.get.call_count == 2  # One hourly request and one attribute page.
    assert (
        df["Interval Start"].tolist()
        == pd.date_range(
            "2023-07-01 07:00",
            "2023-07-02 09:00",
            freq="h",
            tz="UTC",
            inclusive="left",
        ).tolist()
    )


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
        EPACAM()


def test_latest_requires_explicit_date(epa):
    with pytest.raises(NotSupported, match="explicit date"):
        epa.get_power_plant_emissions_generation("latest")
    epa.session.get.assert_not_called()


def test_first_available_day_and_earlier_dates(epa):
    with pytest.raises(ValueError, match="1995-01-01"):
        epa.get_power_plant_emissions_generation("1994-12-31")
    epa.session.get.assert_not_called()
    epa.session.get.side_effect = [
        response([hourly("1995-01-01")]),
        response({"items": [attributes()]}),
    ]
    result = epa.get_power_plant_emissions_generation("1995-01-01")
    assert (
        epa.session.get.call_args_list[0].kwargs["params"]["beginDate"] == "1995-01-01"
    )
    assert result["Interval Start"].tolist() == [pd.Timestamp("1995-01-01 05:00Z")]


@pytest.mark.parametrize(
    "statuses, retry_after, delays",
    [
        ([429, 429, 200], "3", [3, 2]),
        ([503, 200], None, []),
        ([429] * 4, None, [2, 4]),
        ([400], None, []),
    ],
)
def test_http_retries_and_validation(monkeypatch, statuses, retry_after, delays):
    # Exercise the real requests/urllib3 retry path without network calls or sleeps.
    message = b'{"message":"endDate must be on or before 2026-09-30"}'
    send = Mock(
        side_effect=[
            HTTPResponse(
                status=status,
                body=BytesIO(b"[]" if status == 200 else message),
                headers={"Retry-After": retry_after} if i == 0 and retry_after else {},
                preload_content=False,
            )
            for i, status in enumerate(statuses)
        ],
    )
    sleep = Mock()
    monkeypatch.setattr(HTTPSConnectionPool, "_make_request", send)
    monkeypatch.setattr("urllib3.util.retry.time.sleep", sleep)
    client = EPACAM(api_key="test-key")
    client.session.trust_env = False
    try:
        if statuses[-1] == 400:
            with pytest.raises(ValueError, match="2026-09-30"):
                client.get_power_plant_emissions_generation(
                    "2026-09-25",
                    end="2026-10-02",
                    error="raise",
                )
            assert "endDate=2026-10-01" in send.call_args.args[2]
        elif statuses[-1] == 429:
            with pytest.raises(requests.HTTPError, match="429"):
                client.get_power_plant_emissions_generation("2023-07-01")
        else:
            with pytest.raises(NoDataFoundException):
                client.get_power_plant_emissions_generation("2023-07-01")
        assert send.call_count == len(statuses)
        assert [call.args[0] for call in sleep.call_args_list] == delays
    finally:
        client.session.close()
