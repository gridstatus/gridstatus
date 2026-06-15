import warnings

import pandas as pd
import pytest

import gridstatus
from gridstatus.base import Markets

STUB_LMP_DF = pd.DataFrame(
    {
        "Time": pd.to_datetime(["2024-01-02 00:00:00"]).tz_localize("US/Eastern"),
        "Interval Start": pd.to_datetime(["2024-01-02 00:00:00"]).tz_localize(
            "US/Eastern",
        ),
        "Interval End": pd.to_datetime(["2024-01-02 01:00:00"]).tz_localize(
            "US/Eastern",
        ),
        "Market": ["DAY_AHEAD_HOURLY"],
        "Location": ["A"],
        "Location Type": ["Zone"],
        "LMP": [1.0],
        "Energy": [1.0],
        "Congestion": [0.0],
        "Loss": [0.0],
    },
)


def _stub_get_lmp(*args, **kwargs):
    return STUB_LMP_DF.copy()


DEPRECATED_GET_LMP_CALLS = [
    (gridstatus.ISONE, dict(date="2024-01-02", market=Markets.DAY_AHEAD_HOURLY)),
    (gridstatus.Ercot, dict(date="2024-01-02")),
    (gridstatus.CAISO, dict(date="2024-01-02", market=Markets.DAY_AHEAD_HOURLY)),
    (gridstatus.PJM, dict(date="2024-01-02", market=Markets.DAY_AHEAD_HOURLY)),
    (gridstatus.MISO, dict(date="2024-01-02", market=Markets.DAY_AHEAD_HOURLY)),
    (gridstatus.NYISO, dict(date="2024-01-02", market=Markets.DAY_AHEAD_HOURLY)),
]

NEW_LMP_METHOD_CALLS = [
    (gridstatus.ISONE, "get_lmp_day_ahead_hourly"),
    (gridstatus.ISONE, "get_lmp_real_time_hourly"),
    (gridstatus.ISONE, "get_lmp_real_time_5_min"),
    (gridstatus.Ercot, "get_lmp_by_settlement_point"),
    (gridstatus.Ercot, "get_lmp_by_bus"),
    (gridstatus.CAISO, "get_lmp_real_time_5_min"),
    (gridstatus.CAISO, "get_lmp_real_time_15_min"),
    (gridstatus.CAISO, "get_lmp_day_ahead_hourly"),
    (gridstatus.PJM, "get_lmp_real_time_5_min"),
    (gridstatus.PJM, "get_lmp_real_time_hourly"),
    (gridstatus.PJM, "get_lmp_day_ahead_hourly"),
    (gridstatus.MISO, "get_lmp_real_time_5_min"),
    (gridstatus.MISO, "get_lmp_day_ahead_hourly"),
    (gridstatus.MISO, "get_lmp_real_time_hourly_prelim"),
    (gridstatus.MISO, "get_lmp_real_time_hourly_final"),
    (gridstatus.NYISO, "get_lmp_real_time_5_min"),
    (gridstatus.NYISO, "get_lmp_real_time_15_min"),
    (gridstatus.NYISO, "get_lmp_real_time_hourly"),
    (gridstatus.NYISO, "get_lmp_day_ahead_hourly"),
]


@pytest.mark.parametrize("iso_class,call_kwargs", DEPRECATED_GET_LMP_CALLS)
def test_get_lmp_emits_deprecation_warning(iso_class, call_kwargs, monkeypatch):
    iso = iso_class()
    monkeypatch.setattr(iso, "_get_lmp", _stub_get_lmp)

    with pytest.warns(DeprecationWarning):
        iso.get_lmp(**call_kwargs)


@pytest.mark.parametrize("iso_class,method_name", NEW_LMP_METHOD_CALLS)
def test_new_lmp_methods_do_not_warn(iso_class, method_name, monkeypatch):
    iso = iso_class()
    monkeypatch.setattr(iso, "_get_lmp", _stub_get_lmp)

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        getattr(iso, method_name)(date="2024-01-02")
