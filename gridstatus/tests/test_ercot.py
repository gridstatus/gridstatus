import pandas as pd
import pytest

import gridstatus
from gridstatus import Markets


@pytest.mark.skip(reason="takes too long to run")
def test_ercot_get_historical_rtm_spp():
    rtm = gridstatus.Ercot().get_historical_rtm_spp(2020)
    assert isinstance(rtm, pd.DataFrame)
    assert len(rtm) > 0


def test_ercot_get_as_prices():
    as_cols = [
        "Time",
        "Market",
        "Non-Spinning Reserves",
        "Regulation Down",
        "Regulation Up",
        "Responsive Reserves",
    ]

    # today
    iso = gridstatus.Ercot()
    today = pd.Timestamp.now(tz=iso.default_timezone).date()
    df = iso.get_as_prices(today)
    assert df.shape[0] >= 0
    assert df.columns.tolist() == as_cols
    assert df["Time"].unique()[0].date() == today

    date = today - pd.Timedelta(days=3)
    df = iso.get_as_prices(date)
    assert df.shape[0] >= 0
    assert df.columns.tolist() == as_cols
    assert df["Time"].unique()[0].date() == date


def test_ercot_get_load_today():
    cols = [
        "Time",
        "Load",
    ]
    iso = gridstatus.Ercot()
    today = pd.Timestamp.now(tz=iso.default_timezone).date()
    df = iso.get_load(today)
    assert df.shape[0] >= 0
    assert df.columns.tolist() == cols
    assert df["Time"].unique()[0].date() == today


def test_ercot_get_load_3_days_ago():
    cols = [
        "Time",
        "Load",
    ]
    iso = gridstatus.Ercot()
    today = pd.Timestamp.now(tz=iso.default_timezone).date()
    three_days_ago = today - pd.Timedelta(days=3)
    df = iso.get_load(three_days_ago)
    assert df.shape[0] >= 0
    assert df.columns.tolist() == cols
    assert df["Time"].unique()[0].date() == three_days_ago


def test_ercot_get_fuel_mix():

    # today
    iso = gridstatus.Ercot()
    cols = [
        "Time",
        "Coal and Lignite",
        "Hydro",
        "Nuclear",
        "Power Storage",
        "Solar",
        "Wind",
        "Natural Gas",
        "Other",
    ]
    df = iso.get_fuel_mix("today")
    assert df.shape[0] >= 0
    assert df.columns.tolist() == cols

    # latest
    df = iso.get_fuel_mix("latest").mix
    assert df.shape[0] >= 0
    assert df.columns.tolist() == cols


@pytest.mark.slow
def test_ercot_get_spp_today_real_time_15_minutes_zone():
    iso = gridstatus.Ercot()
    cols = [
        "Location",
        "Time",
        "Market",
        "Location Type",
        "SPP",
    ]
    df = iso.get_spp(
        date="today",
        market=Markets.REAL_TIME_15_MIN,
        location_type="zone",
    )
    assert df.shape[0] >= 0
    assert df.columns.tolist() == cols

    markets = df["Market"].unique()
    assert len(markets) == 1
    assert markets[0] == Markets.REAL_TIME_15_MIN.value

    location_types = df["Location Type"].unique()
    assert len(location_types) == 1
    assert location_types[0] == "Zone"


def test_ercot_get_spp_latest_day_ahead_hourly_zone():
    iso = gridstatus.Ercot()
    cols = [
        "Location",
        "Time",
        "Market",
        "Location Type",
        "SPP",
    ]
    df = iso.get_spp(
        date="latest",
        market=Markets.DAY_AHEAD_HOURLY,
        location_type="zone",
    )
    assert df.shape[0] >= 0
    assert df.columns.tolist() == cols

    markets = df["Market"].unique()
    assert len(markets) == 1
    assert markets[0] == Markets.DAY_AHEAD_HOURLY.value

    location_types = df["Location Type"].unique()
    assert len(location_types) == 1
    assert location_types[0] == "Zone"


def test_ercot_get_spp_latest_day_ahead_hourly_hub():
    iso = gridstatus.Ercot()
    cols = [
        "Location",
        "Time",
        "Market",
        "Location Type",
        "SPP",
    ]
    df = iso.get_spp(
        date="latest",
        market=Markets.DAY_AHEAD_HOURLY,
        location_type="hub",
    )
    assert df.shape[0] >= 0
    assert df.columns.tolist() == cols
    markets = df["Market"].unique()
    assert len(markets) == 1
    assert markets[0] == Markets.DAY_AHEAD_HOURLY.value

    location_types = df["Location Type"].unique()
    assert len(location_types) == 1
    assert location_types[0] == "Hub"


def test_ercot_get_spp_latest_day_ahead_hourly_node():
    iso = gridstatus.Ercot()
    cols = [
        "Location",
        "Time",
        "Market",
        "Location Type",
        "SPP",
    ]
    df = iso.get_spp(
        date="latest",
        market=Markets.DAY_AHEAD_HOURLY,
        location_type="node",
    )
    assert df.shape[0] >= 0
    assert df.columns.tolist() == cols

    markets = df["Market"].unique()
    assert len(markets) == 1
    assert markets[0] == Markets.DAY_AHEAD_HOURLY.value

    location_types = df["Location Type"].unique()
    assert len(location_types) == 1
    assert location_types[0] == "Node"
