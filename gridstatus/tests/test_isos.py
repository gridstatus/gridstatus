import pandas as pd
import pytest
from pandas.api.types import is_numeric_dtype

import gridstatus
from gridstatus import *
from gridstatus.base import FuelMix, GridStatus, ISOBase

all_isos = [MISO(), CAISO(), PJM(), Ercot(), SPP(), NYISO(), ISONE()]


def check_lmp_columns(df, market):
    assert set(
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
    ).issubset(df.columns)

    assert df["Market"].unique()[0] == market.value


def check_forecast(df):
    assert set(df.columns) == set(
        ["Forecast Time", "Time", "Load Forecast"],
    )

    assert check_is_datetime_type(df["Forecast Time"])
    assert check_is_datetime_type(df["Time"])


def check_storage(df):
    assert set(df.columns) == set(
        ["Time", "Supply", "Type"],
    )


def check_status(df):
    assert set(df.columns) == set(
        ["Time", "Status", "Notes"],
    )


def check_is_datetime_type(series):
    return pd.core.dtypes.common.is_datetime64_ns_dtype(
        series,
    ) | pd.core.dtypes.common.is_timedelta64_ns_dtype(series)


def test_make_lmp_availability_df():
    gridstatus.utils.make_lmp_availability_table()


@pytest.mark.parametrize("iso", all_isos)
def test_get_latest_fuel_mix(iso):
    mix = iso.get_fuel_mix("latest")
    assert isinstance(mix, FuelMix)
    assert isinstance(mix.time, pd.Timestamp)
    assert isinstance(mix.mix, pd.DataFrame)
    assert repr(mix)
    assert len(mix.mix) > 0
    assert mix.iso == iso.name
    assert isinstance(repr(mix), str)


@pytest.mark.parametrize("iso", [Ercot(), ISONE(), NYISO(), CAISO(), PJM()])
def test_get_fuel_mix_today(iso):
    df = iso.get_fuel_mix("today")
    assert isinstance(df, pd.DataFrame)


def test_list_isos():
    assert len(gridstatus.list_isos()) == 7


def test_get_iso():
    for iso in gridstatus.list_isos()["Id"].values:
        assert issubclass(gridstatus.get_iso(iso), ISOBase)


def test_get_iso_invalid():
    with pytest.raises(Exception) as e_info:
        gridstatus.get_iso("ISO DOESNT EXIST")


@pytest.mark.parametrize("iso", [SPP(), NYISO(), ISONE(), CAISO(), Ercot()])
def test_get_latest_status(iso):
    status = iso.get_status("latest")
    assert isinstance(status, GridStatus)

    # ensure there is a homepage if gridstatus can retrieve a status
    assert isinstance(iso.status_homepage, str)


def test_gridstatus_to_dict():
    time = pd.Timestamp.now()
    notes = ["note1", "note2"]
    gs = GridStatus(
        time=time,
        status="Test",
        reserves=None,
        notes=notes,
        iso=NYISO,
    )

    assert gs.to_dict() == {
        "notes": notes,
        "status": "Test",
        "time": time,
    }


@pytest.mark.parametrize("iso", [ISONE(), NYISO(), PJM(), CAISO()])
def test_get_historical_fuel_mix(iso):
    # date string works
    date_str = "04/03/2022"
    df = iso.get_fuel_mix(date_str)
    assert isinstance(df, pd.DataFrame)
    assert df.loc[0]["Time"].strftime("%m/%d/%Y") == date_str
    assert df.loc[0]["Time"].tz is not None

    # timestamp object works
    date_obj = pd.to_datetime("2019/11/19")
    df = iso.get_fuel_mix(date_obj)
    assert isinstance(df, pd.DataFrame)
    assert df.loc[0]["Time"].strftime("%Y%m%d") == date_obj.strftime("%Y%m%d")
    assert df.loc[0]["Time"].tz is not None

    # datetime object works
    date_obj = pd.to_datetime("2021/05/09").date()
    df = iso.get_fuel_mix(date_obj)
    assert isinstance(df, pd.DataFrame)
    assert df.loc[0]["Time"].strftime("%Y%m%d") == date_obj.strftime("%Y%m%d")
    assert df.loc[0]["Time"].tz is not None


@pytest.mark.parametrize("iso", all_isos)
def test_get_load_today(iso):
    df = iso.get_load("today")
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Load"]) == set(df.columns)
    assert is_numeric_dtype(df["Load"])
    assert isinstance(df.loc[0]["Time"], pd.Timestamp)
    assert df.loc[0]["Time"].tz is not None


@pytest.mark.parametrize("iso", all_isos)
def test_get_latest_load(iso):
    load = iso.get_load("latest")
    set(["time", "load"]) == load.keys()
    assert is_numeric_dtype(type(load["load"]))


@pytest.mark.parametrize("iso", [PJM(), NYISO(), ISONE(), CAISO()])
def test_get_historical_load(iso):
    # pick a test date 2 weeks back
    test_date = (pd.Timestamp.now() - pd.Timedelta(days=14)).date()

    # date string works
    date_str = test_date.strftime("%Y%m%d")
    df = iso.get_load(date_str)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Load"]) == set(df.columns)
    assert df.loc[0]["Time"].strftime("%Y%m%d") == date_str
    assert is_numeric_dtype(df["Load"])

    # timestamp object works
    df = iso.get_load(test_date)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Load"]) == set(df.columns)
    assert df.loc[0]["Time"].strftime("%Y%m%d") == test_date.strftime("%Y%m%d")
    assert is_numeric_dtype(df["Load"])

    # datetime object works
    df = iso.get_load(test_date)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Load"]) == set(df.columns)
    assert df.loc[0]["Time"].strftime("%Y%m%d") == test_date.strftime("%Y%m%d")
    assert is_numeric_dtype(df["Load"])


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
                "markets": [Markets.DAY_AHEAD_HOURLY, Markets.REAL_TIME_5_MIN],
            },
        },
        {
            PJM(): {
                "markets": [
                    # Markets.REAL_TIME_5_MIN, TODO renable, but too slow
                    Markets.REAL_TIME_HOURLY,
                    Markets.DAY_AHEAD_HOURLY,
                ],
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
        hist = iso.get_lmp(date_str, market=m)
        assert isinstance(hist, pd.DataFrame)
        check_lmp_columns(hist, m)


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
                "markets": [Markets.DAY_AHEAD_HOURLY, Markets.REAL_TIME_5_MIN],
            },
        },
        {
            PJM(): {
                "markets": [
                    Markets.DAY_AHEAD_HOURLY,
                ],
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
        latest = iso.get_lmp(date="latest", market=m)
        assert isinstance(latest, pd.DataFrame)
        check_lmp_columns(latest, m)


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
                "markets": [Markets.DAY_AHEAD_HOURLY, Markets.REAL_TIME_5_MIN],
            },
        },
        {
            PJM(): {
                "markets": [
                    Markets.DAY_AHEAD_HOURLY,
                ],
            },
        },
    ],
)
def test_get_lmp_today(test):
    iso = list(test)[0]
    markets = test[iso]["markets"]

    for m in markets:
        today = iso.get_lmp(date="today", market=m)
        assert isinstance(today, pd.DataFrame)
        check_lmp_columns(today, m)


@pytest.mark.parametrize("iso", [ISONE(), CAISO(), NYISO()])
def test_get_historical_load_forecast(iso):
    test_date = (pd.Timestamp.now() - pd.Timedelta(days=14)).date()
    forecast = iso.get_load_forecast(date=test_date)
    check_forecast(forecast)


@pytest.mark.parametrize("iso", [NYISO(), ISONE(), CAISO()])
def test_get_historical_forecast_with_date_range(iso):
    end = pd.Timestamp.now().normalize() - pd.Timedelta(days=14)
    start = (end - pd.Timedelta(days=7)).date()

    forecast = forecast = iso.get_load_forecast(
        start=start,
        end=end,
    )
    check_forecast(forecast)


@pytest.mark.parametrize(
    "iso",
    [PJM(), MISO(), SPP(), Ercot(), ISONE(), CAISO(), NYISO()],
)
def test_get_load_forecast_today(iso):
    forecast = iso.get_load_forecast("today")
    check_forecast(forecast)


@pytest.mark.parametrize(
    "iso",
    [CAISO()],
)
def test_get_storage_today(iso):
    storage = iso.get_storage("today")
    check_storage(storage)


@pytest.mark.parametrize(
    "iso",
    [CAISO()],
)
def test_get_historical_storage(iso):
    test_date = (pd.Timestamp.now() - pd.Timedelta(days=14)).date()
    storage = iso.get_storage(date=test_date)
    check_storage(storage)


# only testing with caiso, assume works with others


def test_end_is_today():
    iso = CAISO()

    num_days = 7
    end = pd.Timestamp.now(tz=iso.default_timezone) + pd.Timedelta(days=1)
    start = end - pd.Timedelta(days=num_days)
    data = iso.get_fuel_mix(date=start.date(), end="today")
    # make sure right number of days are returned
    assert data["Time"].dt.day.nunique() == num_days


@pytest.mark.parametrize("iso", [ISONE(), NYISO(), PJM(), CAISO()])
def test_get_fuel_mix_with_date_range(iso):
    # range not inclusive, add one to include today
    num_days = 7
    end = pd.Timestamp.now(tz=iso.default_timezone) + pd.Timedelta(days=1)
    start = end - pd.Timedelta(days=num_days)

    data = iso.get_fuel_mix(date=start.date(), end=end.date())
    # make sure right number of days are returned
    assert data["Time"].dt.day.nunique() == num_days


@pytest.mark.parametrize("iso", [ISONE(), NYISO(), PJM(), CAISO()])
def test_get_historical_load_with_date_range(iso):
    num_days = 7
    end = pd.Timestamp.now(tz=iso.default_timezone) + pd.Timedelta(days=1)
    start = end - pd.Timedelta(days=num_days)
    data = iso.get_load(date=start.date(), end=end.date())
    # make sure right number of days are returned
    assert data["Time"].dt.day.nunique() == num_days


@pytest.mark.parametrize("iso", [ISONE(), NYISO(), PJM(), CAISO()])
def test_date_or_start(iso):
    num_days = 2
    end = pd.Timestamp.now(tz=iso.default_timezone)
    start = end - pd.Timedelta(days=num_days)

    data_date = iso.get_fuel_mix(date=start.date(), end=end.date())
    data_start = iso.get_fuel_mix(
        start=start.date(),
        end=end.date(),
    )
    data_date = iso.get_fuel_mix(date=start.date())
    data_start = iso.get_fuel_mix(start=start.date())

    with pytest.raises(ValueError):
        iso.get_fuel_mix(start=start.date(), date=start.date())


def test_end_before_start_raises_error():
    iso = CAISO()
    with pytest.raises(AssertionError):
        iso.get_fuel_mix(start="Jan 2, 2021", end="Jan 1, 2021")
