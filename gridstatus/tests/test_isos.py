import pandas as pd
import pytest

import gridstatus
from gridstatus import CAISO, ISONE, MISO, NYISO, PJM, SPP, Ercot
from gridstatus.base import GridStatus, ISOBase

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


def test_list_isos():
    assert len(gridstatus.list_isos()) == 7


def test_get_iso():
    for iso in gridstatus.list_isos()["Id"].values:
        assert issubclass(gridstatus.get_iso(iso), ISOBase)


def test_get_iso_invalid():
    with pytest.raises(Exception):
        gridstatus.get_iso("ISO DOESNT EXIST")


def test_handle_date_today_tz():
    # make sure it returns a stamp
    # with the correct timezone
    tz = "US/Eastern"
    date = gridstatus.utils._handle_date(
        "today",
        tz=tz,
    )
    assert date.tzinfo.zone == tz


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

    iso.get_fuel_mix(date=start.date(), end=end.date())
    iso.get_fuel_mix(
        start=start.date(),
        end=end.date(),
    )
    iso.get_fuel_mix(date=start.date())
    iso.get_fuel_mix(start=start.date())

    with pytest.raises(ValueError):
        iso.get_fuel_mix(start=start.date(), date=start.date())


def test_end_before_start_raises_error():
    iso = CAISO()
    with pytest.raises(AssertionError):
        iso.get_fuel_mix(start="Jan 2, 2021", end="Jan 1, 2021")
