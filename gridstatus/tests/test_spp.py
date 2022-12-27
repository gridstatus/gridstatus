import pandas as pd
import pytest

import gridstatus
from gridstatus import Markets, NotSupported


@pytest.mark.parametrize(
    "market,location_type",
    [
        (Markets.REAL_TIME_5_MIN, "Hub"),
        (Markets.REAL_TIME_5_MIN, "Interface"),
        (Markets.DAY_AHEAD_HOURLY, "Hub"),
        (Markets.DAY_AHEAD_HOURLY, "Interface"),
    ],
)
def test_get_lmp_latest(market, location_type):
    iso = gridstatus.SPP()
    df = iso.get_lmp(
        date="latest",
        market=market,
        location_type=location_type,
    )
    cols = [
        "Time",
        "Market",
        "Location",
        "Location Type",
        "LMP",
        "Energy",
        "Congestion",
        "Loss",
    ]
    assert df.shape[0] >= 0
    assert df.columns.tolist() == cols
    markets = df["Market"].unique()
    assert len(markets) == 1
    assert markets[0] == market.value

    location_types = df["Location Type"].unique()
    assert len(location_types) == 1
    assert location_types[0] == location_type


@pytest.mark.parametrize(
    "date,market,location_type",
    [
        ("today", Markets.REAL_TIME_5_MIN, "Hub"),
        ("latest", Markets.REAL_TIME_15_MIN, "Hub"),
        (pd.Timestamp("2020-01-01T00:00:00"), Markets.REAL_TIME_5_MIN, "Hub"),
    ],
)
def test_get_lmp_unsupported_raises_not_supported(date, market, location_type):
    iso = gridstatus.SPP()
    with pytest.raises(NotSupported):
        iso.get_lmp(
            date=date,
            market=market,
            location_type=location_type,
        )


def test_parse_gmt_interval_end():
    df = pd.DataFrame(
        [
            {
                "ExpectedTime": pd.Timestamp(
                    "2022-12-26 18:45:00-0600",
                    tz="US/Central",
                ),
                "GMTINTERVALEND": 1672102200000,
            },
        ],
    )

    df["ActualTime"] = gridstatus.SPP._parse_gmt_interval_end(
        df,
        interval_duration=pd.Timedelta(minutes=5),
        timezone="US/Central",
    )
    assert df["ActualTime"].tolist() == df["ExpectedTime"].tolist()


def test_parse_gmt_interval_end_daylight_savings_time():
    df = pd.DataFrame(
        [
            {
                "ExpectedTime": pd.Timestamp(
                    "2022-03-15 13:00:00-0500",
                    tz="US/Central",
                ),
                # 2022-03-15 13:05:00 CDT
                "GMTINTERVALEND": 1647367500000,
            },
        ],
    )

    df["ActualTime"] = gridstatus.SPP._parse_gmt_interval_end(
        df,
        interval_duration=pd.Timedelta(minutes=5),
        timezone="US/Central",
    )
    assert df["ActualTime"].tolist() == df["ExpectedTime"].tolist()


def test_parse_day_ahead_hour_end():
    df = pd.DataFrame(
        [
            {
                "ExpectedTime": pd.Timestamp(
                    "2022-12-26 08:00:00-0600",
                    tz="US/Central",
                ),
                "DA_HOUREND": "12/26/2022 9:00:00 AM",
            },
        ],
    )

    df["ActualTime"] = gridstatus.SPP._parse_day_ahead_hour_end(
        df,
        timezone="US/Central",
    )
    assert df["ActualTime"].tolist() == df["ExpectedTime"].tolist()


def test_parse_day_ahead_hour_end_daylight_savings_time():
    df = pd.DataFrame(
        [
            {
                "ExpectedTime": pd.Timestamp(
                    "2022-03-15 13:00:00-0500",
                    tz="US/Central",
                ),
                "DA_HOUREND": "03/15/2022 2:00:00 PM",
            },
        ],
    )

    df["ActualTime"] = gridstatus.SPP._parse_day_ahead_hour_end(
        df,
        timezone="US/Central",
    )
    assert df["ActualTime"].tolist() == df["ExpectedTime"].tolist()
