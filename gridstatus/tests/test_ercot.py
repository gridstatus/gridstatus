import pandas as pd
import pytest

import gridstatus
from gridstatus import Markets


def check_ercot_spp(df, market, location_type):
    """Common checks for SPP data:
    - Columns
    - One Market
    - One Location Type
    """
    cols = [
        "Location",
        "Time",
        "Market",
        "Location Type",
        "SPP",
    ]
    assert df.shape[0] >= 0
    assert df.columns.tolist() == cols
    markets = df["Market"].unique()
    assert len(markets) == 1
    assert markets[0] == market.value

    location_types = df["Location Type"].unique()
    assert len(location_types) == 1
    assert location_types[0] == location_type


@pytest.mark.skip(reason="takes too long to run")
def test_ercot_get_historical_rtm_spp():
    rtm = gridstatus.Ercot().get_rtm_spp(2020)
    assert isinstance(rtm, pd.DataFrame)
    assert len(rtm) > 0


def test_ercot_get_as_prices():
    as_cols = [
        "Time",
        "Market",
        "Non-Spinning Reserves",
        "Regulation Down",
        "Regulation Up",
        "Responsive Reserves",
    ]

    # today
    iso = gridstatus.Ercot()
    today = pd.Timestamp.now(tz=iso.default_timezone).date()
    df = iso.get_as_prices(today)
    assert df.shape[0] >= 0
    assert df.columns.tolist() == as_cols
    assert df["Time"].unique()[0].date() == today

    date = today - pd.Timedelta(days=3)
    df = iso.get_as_prices(date)
    assert df.shape[0] >= 0
    assert df.columns.tolist() == as_cols
    assert df["Time"].unique()[0].date() == date


def test_ercot_get_load_today():
    cols = [
        "Time",
        "Load",
    ]
    iso = gridstatus.Ercot()
    today = pd.Timestamp.now(tz=iso.default_timezone).date()
    df = iso.get_load(today)
    assert df.shape[0] >= 0
    assert df.columns.tolist() == cols
    assert df["Time"].unique()[0].date() == today


def test_ercot_get_load_latest():
    expected_keys = {
        "time",
        "load",
    }
    iso = gridstatus.Ercot()
    today = pd.Timestamp.now(tz=iso.default_timezone).date()
    load = iso.get_load("latest")
    assert load.keys() == expected_keys
    assert load["time"].date() == today


def test_ercot_get_load_3_days_ago():
    cols = [
        "Time",
        "Load",
    ]
    iso = gridstatus.Ercot()
    today = pd.Timestamp.now(tz=iso.default_timezone).date()
    three_days_ago = today - pd.Timedelta(days=3)
    df = iso.get_load(three_days_ago)
    assert df.shape[0] >= 0
    assert df.columns.tolist() == cols
    assert df["Time"].unique()[0].date() == three_days_ago


def test_ercot_get_fuel_mix():

    # today
    iso = gridstatus.Ercot()
    cols = [
        "Time",
        "Coal and Lignite",
        "Hydro",
        "Nuclear",
        "Power Storage",
        "Solar",
        "Wind",
        "Natural Gas",
        "Other",
    ]
    df = iso.get_fuel_mix("today")
    assert df.shape[0] >= 0
    assert df.columns.tolist() == cols

    # latest
    df = iso.get_fuel_mix("latest").mix
    assert df.shape[0] >= 0
    assert df.columns.tolist() == cols


@pytest.mark.slow
def test_ercot_get_spp_today_real_time_15_minutes_zone():
    iso = gridstatus.Ercot()
    df = iso.get_spp(
        date="today",
        market=Markets.REAL_TIME_15_MIN,
        location_type="zone",
    )
    check_ercot_spp(df, Markets.REAL_TIME_15_MIN, "Zone")


@pytest.mark.slow
def test_ercot_get_two_days_ago_real_time_15_minutes_zone():
    iso = gridstatus.Ercot()
    two_days_ago = pd.Timestamp.now(tz=iso.default_timezone).date() - pd.Timedelta(
        days=2,
    )
    df = iso.get_spp(
        date=two_days_ago,
        market=Markets.REAL_TIME_15_MIN,
        location_type="zone",
    )
    check_ercot_spp(df, Markets.REAL_TIME_15_MIN, "Zone")


def test_ercot_get_two_days_ago_day_ahead_hourly_zone():
    iso = gridstatus.Ercot()
    two_days_ago = pd.Timestamp.now(tz=iso.default_timezone).date() - pd.Timedelta(
        days=2,
    )
    df = iso.get_spp(
        date=two_days_ago,
        market=Markets.DAY_AHEAD_HOURLY,
        location_type="zone",
    )
    check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Zone")


def test_ercot_get_dam_latest_day_ahead_hourly_zone_should_raise_exception():
    iso = gridstatus.Ercot()
    with pytest.raises(ValueError):
        df = iso.get_spp(
            date="latest",
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="zone",
        )


def test_ercot_get_dam_today_day_ahead_hourly_hub():
    iso = gridstatus.Ercot()
    df = iso.get_spp(
        date="today",
        market=Markets.DAY_AHEAD_HOURLY,
        location_type="hub",
    )
    check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Hub")


def test_ercot_get_dam_today_day_ahead_hourly_node():
    iso = gridstatus.Ercot()
    df = iso.get_spp(
        date="today",
        market=Markets.DAY_AHEAD_HOURLY,
        location_type="node",
    )
    check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Node")


def test_ercot_get_dam_today_day_ahead_hourly_zone():
    iso = gridstatus.Ercot()
    df = iso.get_spp(
        date="today",
        market=Markets.DAY_AHEAD_HOURLY,
        location_type="zone",
    )
    check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Zone")


def test_ercot_parse_delivery_date_hour_interval():
    df = pd.DataFrame(
        [
            {
                "ExpectedTime": pd.Timestamp(
                    "2022-01-01 00:00:00-06:00",
                    tz="US/Central",
                ),
                "DeliveryDate": "01/01/2022",
                "DeliveryHour": "1",
                "DeliveryInterval": "1",
            },
            {
                "ExpectedTime": pd.Timestamp(
                    "2022-01-02 23:45:00-06:00",
                    tz="US/Central",
                ),
                "DeliveryDate": "01/02/2022",
                "DeliveryHour": "24",
                "DeliveryInterval": "4",
            },
        ],
    )
    df["ActualTime"] = gridstatus.Ercot._parse_delivery_date_hour_interval(
        df,
        "US/Central",
    )
    assert df["ActualTime"].tolist() == df["ExpectedTime"].tolist()


def test_ercot_parse_delivery_date_hour_ending():
    df = pd.DataFrame(
        [
            {
                "ExpectedTime": pd.Timestamp(
                    "2022-01-01 00:00:00-06:00",
                    tz="US/Central",
                ),
                "DeliveryDate": "01/01/2022",
                "HourEnding": "01:00",
            },
            {
                "ExpectedTime": pd.Timestamp(
                    "2022-01-01 23:00:00-06:00",
                    tz="US/Central",
                ),
                "DeliveryDate": "01/01/2022",
                "HourEnding": "24:00",
            },
        ],
    )
    df["ActualTime"] = gridstatus.Ercot._parse_delivery_date_hour_ending(
        df,
        "US/Central",
    )
    assert df["ActualTime"].tolist() == df["ExpectedTime"].tolist()


def test_ercot_parse_oper_day_hour_ending():
    df = pd.DataFrame(
        [
            {
                "ExpectedTime": pd.Timestamp(
                    "2022-01-01 00:00:00-06:00",
                    tz="US/Central",
                ),
                "Oper Day": "01/01/2022",
                "Hour Ending": "100",
            },
            {
                "ExpectedTime": pd.Timestamp(
                    "2022-01-01 23:00:00-06:00",
                    tz="US/Central",
                ),
                "Oper Day": "01/01/2022",
                "Hour Ending": "2400",
            },
        ],
    )
    df["ActualTime"] = gridstatus.Ercot._parse_oper_day_hour_ending(df, "US/Central")
    assert df["ActualTime"].tolist() == df["ExpectedTime"].tolist()
