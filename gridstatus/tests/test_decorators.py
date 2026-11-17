import pandas as pd
import time_machine

from gridstatus.decorators import FiveMinOffset, support_date_range

# todo test other offsets


def test_five_min_offset():
    before_dst = pd.Timestamp("2024-03-10 01:55:00", tz="US/Central")
    after = before_dst + FiveMinOffset()
    assert after == pd.Timestamp("2024-03-10 03:00:00", tz="US/Central")

    # also test other direction

    before_std = pd.Timestamp("2024-11-03 00:55:00", tz="US/Central") + pd.Timedelta(
        hours=1,
    )
    after = before_std + FiveMinOffset()
    assert after == pd.Timestamp("2024-11-03 00:55:00", tz="US/Central") + pd.Timedelta(
        hours=1,
        minutes=5,
    )


class HourlySource:
    default_timezone = "US/Central"

    @support_date_range("HOUR_START")
    def get_hours(self, date, end=None, verbose=False):
        return pd.DataFrame({"Interval Start": [date], "Interval End": [end]})


def test_today_spans_the_fall_back_day():
    # 8:00 CST on November 1, 2026, a 25 hour day
    with time_machine.travel("2026-11-01T14:00:00Z", tick=False):
        df = HourlySource().get_hours("today")

    assert len(df) == 25
    assert df["Interval Start"].iloc[0] == pd.Timestamp(
        "2026-11-01 00:00:00-05:00",
    ).tz_convert("US/Central")
    assert df["Interval End"].iloc[-1] == pd.Timestamp(
        "2026-11-02 00:00:00-06:00",
    ).tz_convert("US/Central")
