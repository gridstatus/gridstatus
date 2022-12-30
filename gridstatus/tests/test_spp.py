import pandas as pd
import pytest

import gridstatus
from gridstatus import Markets, NotSupported


def test_get_fuel_mix_central_time():
    iso = gridstatus.SPP()
    fm = iso.get_fuel_mix(date="latest")
    assert fm.time.tz.zone == iso.default_timezone


@pytest.mark.parametrize(
    "market,location_type",
    [
        (Markets.REAL_TIME_5_MIN, "Hub"),
        (Markets.REAL_TIME_5_MIN, "Interface"),
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


def test_get_lmp_latest_settlement_type_returns_three_location_types():
    iso = gridstatus.SPP()
    df = iso.get_lmp(
        date="latest",
        market=Markets.REAL_TIME_5_MIN,
        location_type="SETTLEMENT_LOCATION",
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
    assert markets[0] == Markets.REAL_TIME_5_MIN.value

    assert set(df["Location Type"]) == {
        "Interface",
        "Hub",
        "Settlement Location",
    }


@pytest.mark.slow
@pytest.mark.parametrize(
    "market,location_type",
    [
        (Markets.DAY_AHEAD_HOURLY, "Hub"),
        (Markets.REAL_TIME_5_MIN, "Hub"),
    ],
)
def test_get_lmp_today(market, location_type):
    iso = gridstatus.SPP()
    df = iso.get_lmp(
        date="today",
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
        ("latest", Markets.REAL_TIME_15_MIN, "Interface"),
        (
            pd.Timestamp.now() - pd.Timedelta(days=2),
            Markets.REAL_TIME_15_MIN,
            "Interface",
        ),
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


@pytest.mark.parametrize(
    "date,market,location_type",
    [
        ("latest", Markets.DAY_AHEAD_HOURLY, "Hub"),
        ("latest", Markets.DAY_AHEAD_HOURLY, "Interface"),
    ],
)
def test_get_lmp_day_ahead_cannot_have_latest(date, market, location_type):
    iso = gridstatus.SPP()
    with pytest.raises(ValueError):
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
                "GMTIntervalEnd": 1672102200000,
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
                "GMTIntervalEnd": 1647367500000,
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
