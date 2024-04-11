import pandas as pd

import gridstatus
from gridstatus.utils import is_dst_end, is_today, is_yesterday


def test_is_dst_end():
    date = pd.Timestamp("Nov 6, 2022", tz=gridstatus.NYISO.default_timezone)

    assert is_dst_end(date)
    assert not is_dst_end(date - pd.Timedelta("1 day"))
    assert not is_dst_end(date + pd.Timedelta("1 day"))

    # test start
    dst_start = pd.Timestamp(
        "Mar 13, 2022",
        tz=gridstatus.NYISO.default_timezone,
    )
    assert not is_dst_end(dst_start)


def test_is_today():
    utc_start_of_today = pd.Timestamp.utcnow().normalize()
    assert not is_today(utc_start_of_today, tz="America/New_York")
    assert not is_today(
        utc_start_of_today + pd.Timedelta(hours=2),
        tz="America/New_York",
    )
    assert is_today(utc_start_of_today + pd.Timedelta(hours=6), tz="America/New_York")
    assert not is_today(
        utc_start_of_today - pd.Timedelta(hours=6),
        tz="America/New_York",
    )

    assert not is_today(utc_start_of_today, tz="America/Chicago")
    assert not is_today(
        utc_start_of_today + pd.Timedelta(hours=3),
        tz="America/Chicago",
    )
    assert is_today(utc_start_of_today + pd.Timedelta(hours=6), tz="America/Chicago")
    assert not is_today(
        utc_start_of_today - pd.Timedelta(hours=5),
        tz="America/Chicago",
    )


def test_is_yesterday():
    utc_start_of_yesterday = pd.Timestamp.utcnow().normalize() - pd.Timedelta("1 day")

    assert not is_yesterday(utc_start_of_yesterday, tz="America/New_York")
    assert not is_yesterday(
        utc_start_of_yesterday + pd.Timedelta(hours=2),
        tz="America/New_York",
    )
    assert is_yesterday(
        utc_start_of_yesterday + pd.Timedelta(hours=6),
        tz="America/New_York",
    )
    assert not is_yesterday(
        utc_start_of_yesterday - pd.Timedelta(hours=6),
        tz="America/New_York",
    )

    assert not is_yesterday(utc_start_of_yesterday, tz="America/Chicago")
    assert not is_yesterday(
        utc_start_of_yesterday + pd.Timedelta(hours=3),
        tz="America/Chicago",
    )

    assert is_yesterday(
        utc_start_of_yesterday + pd.Timedelta(hours=6),
        tz="America/Chicago",
    )
    assert not is_yesterday(
        utc_start_of_yesterday - pd.Timedelta(hours=5),
        tz="America/Chicago",
    )
