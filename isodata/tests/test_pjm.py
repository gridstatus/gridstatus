from turtle import st

import pandas as pd
import pytest

import isodata
from isodata.base import Markets
from isodata.tests.test_isos import check_lmp_columns

# def test_pjm_handle_error():
#     iso = isodata.PJM()

#     # TODO this should stop raising erros in the future when archived data is supported
#     with pytest.raises(RuntimeError):
#         iso.get_historical_lmp(
#             date="4/15/2022",
#             market="REAL_TIME_5_MIN",
#             locations=None,
#         )


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


def _lmp_tests(iso, m):
    # uses location_type hub because it has the fewest results, so runs faster

    # span calendar year
    end = pd.Timestamp.now()
    start = end - pd.Timedelta(days=1)

    hist = iso.get_historical_lmp(
        start="2018-12-31",
        end="2019-01-02",
        location_type="hub",
        market=m,
    )
    assert isinstance(hist, pd.DataFrame)
    check_lmp_columns(hist, m)
    # 2 days worth of data for each location
    assert (hist.groupby("Location")["Time"].count() == 48).all()

    # all archive
    hist = iso.get_historical_lmp(
        start="2019-07-15",
        end="2019-07-16",
        location_type="hub",
        market=m,
    )
    assert isinstance(hist, pd.DataFrame)
    check_lmp_columns(hist, m)

    # all standard
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


def test_pjm_get_historical_lmp_hourly():
    iso = isodata.PJM()
    markets = [
        Markets.REAL_TIME_HOURLY,
        Markets.DAY_AHEAD_HOURLY,
    ]

    for m in markets:
        print(iso.iso_id, m)
        _lmp_tests(iso, m)


@pytest.mark.slow
def test_pjm_get_historical_lmp_5_min():
    iso = isodata.PJM()
    _lmp_tests(iso, Markets.REAL_TIME_5_MIN)


def test_pjm_update_dates():
    args_dict = {
        "self": isodata.PJM(),
        "market": Markets.REAL_TIME_5_MIN,
    }
    dates = [
        pd.Timestamp("2018-12-31 00:00:00-0500", tz="US/Eastern"),
        pd.Timestamp("2019-01-01 00:00:00-0500", tz="US/Eastern"),
    ]
    new_dates = isodata.pjm.pjm_update_dates(dates, args_dict)
    assert new_dates == [
        pd.Timestamp("2018-12-31 00:00:00-0500", tz="US/Eastern"),
        pd.Timestamp("2018-12-31 23:59:00-0500", tz="US/Eastern"),
    ]

    dates = [
        pd.Timestamp("2018-12-01 00:00:00-0500", tz="US/Eastern"),
        pd.Timestamp("2019-01-01 00:00:00-0500", tz="US/Eastern"),
        pd.Timestamp("2019-02-01 00:00:00-0500", tz="US/Eastern"),
    ]
    new_dates = isodata.pjm.pjm_update_dates(dates, args_dict)
    assert new_dates == [
        pd.Timestamp("2018-12-01 00:00:00-0500", tz="US/Eastern"),
        pd.Timestamp(
            "2018-12-31 23:59:00-0500",
            tz="US/Eastern",
        ),
        None,
        pd.Timestamp(
            "2019-01-01 00:00:00-0500",
            tz="US/Eastern",
        ),
        pd.Timestamp("2019-02-01 00:00:00-0500", tz="US/Eastern"),
    ]

    dates = [
        pd.Timestamp("2017-12-01 00:00:00-0500", tz="US/Eastern"),
        pd.Timestamp("2020-02-01 00:00:00-0500", tz="US/Eastern"),
    ]
    new_dates = isodata.pjm.pjm_update_dates(dates, args_dict)
    assert new_dates == [
        pd.Timestamp("2017-12-01 00:00:00-0500", tz="US/Eastern"),
        pd.Timestamp(
            "2017-12-31 23:59:00-0500",
            tz="US/Eastern",
        ),
        None,
        pd.Timestamp(
            "2018-01-01 00:00:00-0500",
            tz="US/Eastern",
        ),
        pd.Timestamp(
            "2018-12-31 23:59:00-0500",
            tz="US/Eastern",
        ),
        None,
        pd.Timestamp(
            "2019-01-01 00:00:00-0500",
            tz="US/Eastern",
        ),
        pd.Timestamp(
            "2019-12-31 23:59:00-0500",
            tz="US/Eastern",
        ),
        None,
        pd.Timestamp(
            "2020-01-01 00:00:00-0500",
            tz="US/Eastern",
        ),
        pd.Timestamp(
            "2020-02-01 00:00:00-0500",
            tz="US/Eastern",
        ),
    ]
