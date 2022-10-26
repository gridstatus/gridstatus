import pandas as pd
import pytest

import isodata
from isodata.base import Markets
from isodata.tests.test_isos import check_status


def test_nyiso_date_range():
    iso = isodata.NYISO()
    df = iso.get_historical_fuel_mix(start="Aug 1, 2022", end="Oct 22, 2022")
    assert df.shape[0] >= 0


def test_get_nyiso_historical_status():
    iso = isodata.NYISO()
    date = "20220609"
    status = iso.get_historical_status(date)
    check_status(status)

    start = "2022-05-01"
    end = "2022-10-02"
    status = iso.get_historical_status(start=start, end=end)
    check_status(status)


def test_nyiso_get_historical_lmp_with_range():
    iso = isodata.NYISO()
    start = "2021-12-01"
    end = "2022-2-02"
    df = iso.get_historical_lmp(
        start=start,
        end=end,
        market=Markets.REAL_TIME_5_MIN,
    )
    assert df.shape[0] >= 0


def test_nyiso_edt_to_est():
    # number of rows hardcoded based on when this test was written. should stay same
    iso = isodata.NYISO()

    date = "Nov 7, 2021"

    df = iso.get_historical_status(date=date)
    assert df.shape[0] >= 1

    df = iso.get_historical_fuel_mix(date=date)
    assert df.shape[0] >= 307

    df = iso.get_historical_forecast(date=date)
    assert df.shape[0] >= 145
    df = iso.get_historical_lmp(date=date, market=Markets.REAL_TIME_5_MIN)
    assert df.shape[0] >= 4605
    df = iso.get_historical_lmp(date=date, market=Markets.DAY_AHEAD_HOURLY)
    assert df.shape[0] >= 375

    df = iso.get_historical_load(date=date)
    assert df.shape[0] >= 307


def test_nyiso_est_to_edt():
    # number of rows hardcoded based on when this test was written. should stay same
    iso = isodata.NYISO()

    date = "March 14, 2021"

    df = iso.get_historical_status(date=date)
    assert df.shape[0] >= 5

    df = iso.get_historical_lmp(date=date, market=Markets.REAL_TIME_5_MIN)
    assert df.shape[0] >= 4215

    df = iso.get_historical_lmp(date=date, market=Markets.DAY_AHEAD_HOURLY)
    assert df.shape[0] >= 345

    df = iso.get_historical_forecast(date=date)
    assert df.shape[0] >= 143

    df = iso.get_historical_fuel_mix(date=date)
    assert df.shape[0] >= 281

    df = iso.get_historical_load(date=date)
    assert df.shape[0] >= 281


def test_location_type_parameter():
    iso = isodata.NYISO()

    date = "2022-06-09"

    df_zone = iso.get_historical_lmp(
        date=date,
        market=Markets.DAY_AHEAD_HOURLY,
        location_type="zone",
    )
    assert (df_zone["Location Type"] == "Zone").all()
    df_gen = iso.get_historical_lmp(
        date=date,
        market=Markets.DAY_AHEAD_HOURLY,
        location_type="generator",
    )
    assert (df_gen["Location Type"] == "Generator").all()

    df_zone = iso.get_lmp_today(market=Markets.DAY_AHEAD_HOURLY, location_type="zone")
    assert (df_zone["Location Type"] == "Zone").all()
    df_gen = iso.get_lmp_today(
        market=Markets.DAY_AHEAD_HOURLY,
        location_type="generator",
    )
    assert (df_gen["Location Type"] == "Generator").all()

    df_zone = iso.get_latest_lmp(market=Markets.DAY_AHEAD_HOURLY, location_type="zone")
    assert (df_zone["Location Type"] == "Zone").all()
    df_gen = iso.get_latest_lmp(
        market=Markets.DAY_AHEAD_HOURLY,
        location_type="generator",
    )
    assert (df_gen["Location Type"] == "Generator").all()

    with pytest.raises(ValueError):
        df = iso.get_latest_lmp(market=Markets.DAY_AHEAD_HOURLY, location_type="dummy")
