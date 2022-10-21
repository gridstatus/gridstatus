from turtle import st

import pandas as pd
import pytest

import isodata
from isodata.base import Markets
from isodata.tests.test_isos import check_lmp_columns


def test_pjm_handle_error():
    iso = isodata.PJM()

    # TODO this should stop raising erros in the future when archived data is supported
    with pytest.raises(RuntimeError):
        iso.get_historical_lmp(
            date="4/15/2022",
            market="REAL_TIME_5_MIN",
            locations=None,
        )


def test_pjm_pnode():
    iso = isodata.PJM()
    df = iso.get_pnode_ids()
    assert len(df) > 0


def test_no_data():
    date = "2000-01-14"
    iso = isodata.PJM()
    with pytest.raises(RuntimeError):
        df = iso.get_historical_fuel_mix(start=date)


def test_dst_shift_back():
    date = "2021-11-07"
    iso = isodata.PJM()
    df = iso.get_historical_fuel_mix(start=date)

    assert len(df["Time"]) == 25  # 25 hours due to shift backwards in time
    assert (df["Time"].dt.strftime("%Y-%m-%d") == date).all()


def test_dst_shift_forward():
    date = "2021-03-14"
    iso = isodata.PJM()
    df = iso.get_historical_fuel_mix(start=date)

    assert len(df["Time"]) == 23  # 23 hours due to shift forwards in time
    assert (df["Time"].dt.strftime("%Y-%m-%d") == date).all()


def test_pjm_get_historical_lmp():
    iso = isodata.PJM()
    markets = [
        Markets.REAL_TIME_HOURLY,
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
    ]

    for m in markets:
        print(iso.iso_id, m)

        # all archive
        # use location_type hub because it has the fewest results
        hist = iso.get_historical_lmp(
            start="2019-07-15",
            end="2019-07-16",
            location_type="hub",
            market=m,
        )
        assert isinstance(hist, pd.DataFrame)
        check_lmp_columns(hist, m)

        # all standard
        # use location_type hub because it has the fewest results
        end = pd.Timestamp.now()
        start = end - pd.Timedelta(days=1)

        hist = iso.get_historical_lmp(
            start=start,
            end=end,
            location_type="hub",
            market=m,
        )
        assert isinstance(hist, pd.DataFrame)
        check_lmp_columns(hist, m)
