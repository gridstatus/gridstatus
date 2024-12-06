import pandas as pd
import pytest

from gridstatus.base import ISOBase, Markets, NotSupported
from gridstatus.lmp_config import lmp_config

# TODO(kladar): Dive into this more to understand what's going on and whether VCR applies


def days_ago(days):
    return pd.Timestamp.now(
        tz=ISOTodayHistoricalDayAheadHourly.default_timezone,
    ) - pd.Timedelta(days=days)


class ISOZeroSupport(ISOBase):
    default_timezone = "US/Central"

    @lmp_config(supports={})
    def get_lmp(self, date, market):
        print(f"get_lmp({date}, {market})")


class ISOTodayHistoricalDayAheadHourly(ISOBase):
    default_timezone = "US/Central"

    @lmp_config(supports={Markets.DAY_AHEAD_HOURLY: ["today", "historical"]})
    def get_lmp(self, date, market):
        print(f"get_lmp({date}, {market})")


class ISOLatestRealTime15Minutes(ISOBase):
    default_timezone = "US/Central"

    @lmp_config(supports={Markets.REAL_TIME_15_MIN: ["latest"]})
    def get_lmp(self, date, market):
        print(f"get_lmp({date}, {market})")


def test_lmp_config_support_check_matches():
    iso = ISOLatestRealTime15Minutes()
    iso.get_lmp("latest", Markets.REAL_TIME_15_MIN)
    iso.get_lmp("latest", "REAL_TIME_15_MIN")

    iso = ISOTodayHistoricalDayAheadHourly()
    iso.get_lmp("today", Markets.DAY_AHEAD_HOURLY)
    iso.get_lmp("today", "DAY_AHEAD_HOURLY")
    iso.get_lmp(days_ago(2), Markets.DAY_AHEAD_HOURLY)
    iso.get_lmp(days_ago(2), "DAY_AHEAD_HOURLY")


def test_lmp_config_signature_combos_success():
    iso = ISOLatestRealTime15Minutes()
    date = "latest"
    market = "REAL_TIME_15_MIN"
    iso.get_lmp(date, market)
    iso.get_lmp(date, market=market)
    iso.get_lmp(date=date, market=market)


def test_lmp_config_signature_combos_failure_propagates_type_errors():
    iso = ISOLatestRealTime15Minutes()
    date = "latest"
    market = "REAL_TIME_15_MIN"

    with pytest.raises(TypeError):
        iso.get_lmp(date)
    with pytest.raises(TypeError):
        iso.get_lmp(market)
    with pytest.raises(TypeError):
        iso.get_lmp(date=date)
    with pytest.raises(TypeError):
        iso.get_lmp(market=market)


def test_lmp_config_support_check_does_not_match():
    with pytest.raises(NotSupported):
        ISOTodayHistoricalDayAheadHourly().get_lmp("latest", Markets.DAY_AHEAD_HOURLY)
    with pytest.raises(NotSupported):
        ISOZeroSupport().get_lmp("today", Markets.DAY_AHEAD_HOURLY)
