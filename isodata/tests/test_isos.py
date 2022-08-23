import pandas as pd
import pytest
from pandas.api.types import is_numeric_dtype

import isodata
from isodata import *
from isodata.base import FuelMix, GridStatus, ISOBase

all_isos = [MISO(), CAISO(), PJM(), Ercot(), SPP(), NYISO(), ISONE()]


def check_lmp_columns(df):
    assert set(df.columns) == set(
        [
            "Time",
            "Market",
            "Location",
            "Location Type",
            "LMP",
            "Energy",
            "Congestion",
            "Loss",
        ],
    )

    # todo check if market is valid enum


def check_forecast(df):
    assert set(df.columns) == set(
        ["Forecast Time", "Time", "Load Forecast"],
    )


def check_battery(df):
    assert set(df.columns) == set(
        ["Time", "Battery Supply"],
    )


def test_make_lmp_availability_df():
    isodata.utils.make_lmp_availability_table()


@pytest.mark.parametrize("iso", all_isos)
def test_get_latest_fuel_mix(iso):
    print(iso)
    mix = iso.get_latest_fuel_mix()
    assert isinstance(mix, FuelMix)
    assert isinstance(mix.time, pd.Timestamp)
    assert isinstance(mix.mix, pd.DataFrame)
    assert len(mix.mix) > 0
    assert mix.iso == iso.name
    assert isinstance(repr(mix), str)


@pytest.mark.parametrize("iso", [ISONE(), NYISO(), CAISO(), PJM()])
def test_get_fuel_mix(iso):
    df = iso.get_fuel_mix_today()
    assert isinstance(df, pd.DataFrame)


def test_list_isos():
    assert len(isodata.list_isos()) == 7


def test_get_iso():
    for iso in isodata.list_isos()["Id"].values:
        assert issubclass(isodata.get_iso(iso), ISOBase)


def test_get_iso_invalid():
    with pytest.raises(Exception) as e_info:
        isodata.get_iso("ISO DOESNT EXIST")


@pytest.mark.parametrize("iso", [SPP(), NYISO(), ISONE(), CAISO(), Ercot()])
def test_get_latest_status(iso):
    status = iso.get_latest_status()
    assert isinstance(status, GridStatus)


@pytest.mark.parametrize("iso", [NYISO()])
def test_get_historical_status(iso):
    date = "20220609"
    status = iso.get_historical_status(date)
    assert isinstance(status, pd.DataFrame)


@pytest.mark.parametrize("iso", [ISONE(), NYISO(), PJM(), CAISO()])
def test_get_historical_fuel_mix(iso):
    # date string works
    date_str = "04/03/2022"
    df = iso.get_historical_fuel_mix(date_str)
    assert isinstance(df, pd.DataFrame)
    assert df.loc[0]["Time"].strftime("%m/%d/%Y") == date_str
    assert df.loc[0]["Time"].tz is not None

    # timestamp object works
    date_obj = pd.to_datetime("2019/11/19")
    df = iso.get_historical_fuel_mix(date_obj)
    assert isinstance(df, pd.DataFrame)
    assert df.loc[0]["Time"].strftime("%Y%m%d") == date_obj.strftime("%Y%m%d")
    assert df.loc[0]["Time"].tz is not None

    # datetime object works
    date_obj = pd.to_datetime("2021/05/09").date()
    df = iso.get_historical_fuel_mix(date_obj)
    assert isinstance(df, pd.DataFrame)
    assert df.loc[0]["Time"].strftime("%Y%m%d") == date_obj.strftime("%Y%m%d")
    assert df.loc[0]["Time"].tz is not None


@pytest.mark.parametrize("iso", all_isos)
def test_get_latest_supply(iso):
    supply = iso.get_latest_supply()
    set(["time", "supply"]) == supply.keys()
    assert is_numeric_dtype(type(supply["supply"]))


@pytest.mark.parametrize("iso", [ISONE(), Ercot(), NYISO(), PJM(), CAISO()])
def test_get_supply_today(iso):
    # todo check that the date is right
    df = iso.get_supply_today()
    assert isinstance(df, pd.DataFrame)
    set(["Time", "Supply"]) == set(df.columns)
    assert is_numeric_dtype(df["Supply"])
    assert df.loc[0]["Time"].tz is not None


@pytest.mark.parametrize("iso", [ISONE(), NYISO(), PJM(), CAISO()])
def test_get_supply(iso):
    date_str = "March 3, 2022"
    df = iso.get_historical_supply(date_str)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Supply"]) == set(df.columns)
    assert df.loc[0]["Time"].date() == isodata.utils._handle_date(date_str).date()
    assert is_numeric_dtype(df["Supply"])
    assert df.loc[0]["Time"].tz is not None


@pytest.mark.parametrize("iso", all_isos)
def test_get_demand_today(iso):
    df = iso.get_demand_today()
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Demand"]) == set(df.columns)
    assert is_numeric_dtype(df["Demand"])
    assert isinstance(df.loc[0]["Time"], pd.Timestamp)
    assert df.loc[0]["Time"].tz is not None


@pytest.mark.parametrize("iso", all_isos)
def test_get_latest_demand(iso):
    demand = iso.get_latest_demand()
    set(["time", "demand"]) == demand.keys()
    assert is_numeric_dtype(type(demand["demand"]))


@pytest.mark.parametrize("iso", [PJM(), NYISO(), ISONE(), CAISO()])
def test_get_historical_demand(iso):
    # pick a test date 2 weeks back
    test_date = (pd.Timestamp.now() - pd.Timedelta(days=14)).date()

    # date string works
    date_str = test_date.strftime("%Y%m%d")
    df = iso.get_historical_demand(date_str)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Demand"]) == set(df.columns)
    assert df.loc[0]["Time"].strftime("%Y%m%d") == date_str
    assert is_numeric_dtype(df["Demand"])

    # timestamp object works
    df = iso.get_historical_demand(test_date)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Demand"]) == set(df.columns)
    assert df.loc[0]["Time"].strftime("%Y%m%d") == test_date.strftime("%Y%m%d")
    assert is_numeric_dtype(df["Demand"])

    # datetime object works
    df = iso.get_historical_demand(test_date)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Demand"]) == set(df.columns)
    assert df.loc[0]["Time"].strftime("%Y%m%d") == test_date.strftime("%Y%m%d")
    assert is_numeric_dtype(df["Demand"])


@pytest.mark.parametrize(
    "test",
    [
        {
            CAISO(): {
                "markets": [
                    Markets.REAL_TIME_HOURLY,
                    Markets.DAY_AHEAD_HOURLY,
                    Markets.REAL_TIME_15_MIN,
                ],
            },
        },
        {
            ISONE(): {
                # , Markets.REAL_TIME_5_MIN
                "markets": [Markets.DAY_AHEAD_HOURLY, Markets.REAL_TIME_HOURLY],
            },
        },
        {
            NYISO(): {
                "markets": [Markets.DAY_AHEAD_5_MIN, Markets.REAL_TIME_5_MIN],
            },
        },
    ],
)
def test_get_historical_lmp(test):
    iso = list(test)[0]
    markets = test[iso]["markets"]

    date_str = "20220722"
    for m in markets:
        print(iso.iso_id, m)
        hist = iso.get_historical_lmp(date_str, m)
        assert isinstance(hist, pd.DataFrame)
        check_lmp_columns(hist)


@pytest.mark.parametrize(
    "test",
    [
        {
            CAISO(): {
                "markets": [
                    Markets.REAL_TIME_HOURLY,
                    Markets.DAY_AHEAD_HOURLY,
                    Markets.REAL_TIME_15_MIN,
                ],
            },
        },
        {
            ISONE(): {
                "markets": [Markets.REAL_TIME_5_MIN, Markets.REAL_TIME_HOURLY],
            },
        },
        {
            MISO(): {
                "markets": [Markets.REAL_TIME_5_MIN, Markets.DAY_AHEAD_HOURLY],
            },
        },
        {
            NYISO(): {
                "markets": [Markets.DAY_AHEAD_5_MIN, Markets.REAL_TIME_5_MIN],
            },
        },
    ],
)
def test_get_latest_lmp(test):
    iso = list(test)[0]
    markets = test[iso]["markets"]
    locations = test[iso]

    date_str = "20220722"
    for m in markets:
        print(iso.iso_id, m)
        latest = iso.get_latest_lmp(m)
        assert isinstance(latest, pd.DataFrame)
        check_lmp_columns(latest)


@pytest.mark.parametrize(
    "test",
    [
        {
            CAISO(): {
                "markets": [
                    Markets.REAL_TIME_HOURLY,
                    Markets.REAL_TIME_15_MIN,
                    Markets.DAY_AHEAD_HOURLY,
                ],
            },
        },
        {
            ISONE(): {
                "markets": [Markets.DAY_AHEAD_HOURLY, Markets.REAL_TIME_5_MIN],
            },
        },
        {
            NYISO(): {
                "markets": [Markets.DAY_AHEAD_5_MIN, Markets.REAL_TIME_5_MIN],
            },
        },
    ],
)
def test_get_lmp_today(test):
    iso = list(test)[0]
    markets = test[iso]["markets"]

    for m in markets:
        today = iso.get_lmp_today(m)
        assert isinstance(today, pd.DataFrame)
        check_lmp_columns(today)


@pytest.mark.parametrize("iso", [ISONE(), CAISO(), NYISO()])
def test_get_historical_forecast(iso):
    test_date = (pd.Timestamp.now() - pd.Timedelta(days=14)).date()
    forecast = iso.get_historical_forecast(test_date)
    check_forecast(forecast)


@pytest.mark.parametrize(
    "iso",
    [MISO(), SPP(), Ercot(), ISONE(), CAISO(), PJM(), NYISO()],
)
def test_get_forecast_today(iso):
    forecast = iso.get_forecast_today()
    check_forecast(forecast)


@pytest.mark.parametrize(
    "iso",
    [CAISO()],
)
def test_get_battery_today(iso):
    battery = iso.get_battery_today()
    check_battery(battery)


@pytest.mark.parametrize(
    "iso",
    [CAISO()],
)
def test_get_historical_battery(iso):
    test_date = (pd.Timestamp.now() - pd.Timedelta(days=14)).date()
    battery = iso.get_historical_battery(test_date)
    check_battery(battery)
