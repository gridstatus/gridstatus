import pandas as pd
import time_machine

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


eastern_timezone = "America/New_York"
central_timezone = "America/Chicago"


def test_is_today():
    # Mock a time where the EST date is different from the UTC date
    with time_machine.travel("2024-01-01T02:00:00Z", tick=False):
        utc_start_of_day = pd.Timestamp.utcnow().normalize()

        assert (
            pd.Timestamp.utcnow().date() != pd.Timestamp.now(tz=eastern_timezone).date()
        )

        # Because is_today converts the timestamp into the timezone provided,
        # this should be true
        assert is_today(utc_start_of_day, tz=eastern_timezone)

        # EST offset is 5 hours so this timestamp is tomorrow in EST
        assert not is_today(
            utc_start_of_day + pd.Timedelta(hours=5),
            tz=eastern_timezone,
        )

        # Yesterday in EST
        assert not is_today(
            utc_start_of_day - pd.Timedelta(hours=19, seconds=1),
            tz=eastern_timezone,
        )

        # CST offset is 6 hours so this timestamp is today in CST
        assert is_today(utc_start_of_day + pd.Timedelta(hours=5), tz=central_timezone)

        # Yesterday in CST
        assert not is_today(
            utc_start_of_day - pd.Timedelta(hours=18, seconds=1),
            tz=central_timezone,
        )

        # Tomorrow in CST
        assert not is_today(
            utc_start_of_day + pd.Timedelta(hours=6),
            tz=central_timezone,
        )

    # Mock a time where the EST date is the same as the UTC date
    with time_machine.travel("2024-01-01T12:00:00Z", tick=False):
        utc_start_of_day = pd.Timestamp.utcnow().normalize()

        assert (
            pd.Timestamp.utcnow().date() == pd.Timestamp.now(tz=eastern_timezone).date()
        )

        # Yesterday in EST
        assert not is_today(utc_start_of_day, tz=eastern_timezone)

        assert is_today(utc_start_of_day + pd.Timedelta(hours=5), tz=eastern_timezone)
        assert is_today(utc_start_of_day + pd.DateOffset(days=1), tz=eastern_timezone)

        # Tomorrow in EST
        assert not is_today(
            utc_start_of_day + pd.DateOffset(days=1) + pd.Timedelta(hours=5),
            tz=eastern_timezone,
        )

        # Yesterday in CST
        assert not is_today(utc_start_of_day, tz=central_timezone)

        assert is_today(utc_start_of_day + pd.Timedelta(hours=6), tz=central_timezone)

        # Tomorrow in CST
        assert not is_today(
            utc_start_of_day + pd.DateOffset(days=1) + pd.Timedelta(hours=6),
            tz=central_timezone,
        )


def test_is_yesterday():
    with time_machine.travel("2024-01-01T02:00:00Z", tick=False):
        start_of_utc_yesterday = pd.Timestamp.utcnow().normalize() - pd.DateOffset(
            days=1,
        )

        # Because is_yesterday converts the timestamp into the given timezone,
        # this should be true
        assert is_yesterday(start_of_utc_yesterday, tz=eastern_timezone)

        # EST offset is 5 hours so this timestamp is today in EST
        assert not is_yesterday(
            start_of_utc_yesterday + pd.Timedelta(hours=5),
            tz=eastern_timezone,
        )

        assert is_yesterday(start_of_utc_yesterday, tz=central_timezone)
        assert is_yesterday(
            start_of_utc_yesterday + pd.Timedelta(hours=5),
            tz=central_timezone,
        )

        # CST offset is 6 hours so this timestamp is today in CST
        assert not is_yesterday(
            start_of_utc_yesterday + pd.Timedelta(hours=6),
            tz=central_timezone,
        )

    with time_machine.travel("2024-01-01T12:00:00Z", tick=False):
        start_of_utc_yesterday = pd.Timestamp.utcnow().normalize() - pd.DateOffset(
            days=1,
        )

        # This is the day before yesterday in EST
        assert not is_yesterday(start_of_utc_yesterday, tz=eastern_timezone)
        assert is_yesterday(
            start_of_utc_yesterday + pd.Timedelta(hours=5),
            tz=eastern_timezone,
        )

        # This is today in EST
        assert not is_yesterday(
            start_of_utc_yesterday + pd.DateOffset(days=1) + pd.Timedelta(hours=6),
            tz=eastern_timezone,
        )

        # This is the day before yesterday in CST
        assert not is_yesterday(start_of_utc_yesterday, tz=central_timezone)
        assert is_yesterday(
            start_of_utc_yesterday + pd.Timedelta(hours=6),
            tz=central_timezone,
        )

        # This is today in CST
        assert not is_yesterday(
            start_of_utc_yesterday + pd.DateOffset(days=1) + pd.Timedelta(hours=6),
            tz=central_timezone,
        )
