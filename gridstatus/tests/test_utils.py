import pandas as pd

import gridstatus
from gridstatus.utils import is_dst_end


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
