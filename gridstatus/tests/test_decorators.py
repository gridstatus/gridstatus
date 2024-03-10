import pandas as pd

from gridstatus.decorators import FiveMinOffset

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
