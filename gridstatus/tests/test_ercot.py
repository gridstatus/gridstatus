import pandas as pd
import pytest

import gridstatus


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
