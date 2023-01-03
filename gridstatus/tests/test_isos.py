import pandas as pd
import pytest

import gridstatus
from gridstatus import CAISO, ISONE, MISO, NYISO, PJM, SPP, Ercot
from gridstatus.base import GridStatus, ISOBase

all_isos = [MISO(), CAISO(), PJM(), Ercot(), SPP(), NYISO(), ISONE()]


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


# only testing with caiso, assume works with others


def test_end_is_today():
    iso = CAISO()

    num_days = 7
    end = pd.Timestamp.now(tz=iso.default_timezone) + pd.Timedelta(days=1)
    start = end - pd.Timedelta(days=num_days)
    data = iso.get_fuel_mix(date=start.date(), end="today")
    # make sure right number of days are returned
    assert data["Time"].dt.day.nunique() == num_days


def test_end_before_start_raises_error():
    iso = CAISO()
    with pytest.raises(AssertionError):
        iso.get_fuel_mix(start="Jan 2, 2021", end="Jan 1, 2021")
