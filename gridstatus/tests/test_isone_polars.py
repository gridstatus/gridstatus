"""Offline tests for ISONE methods that return polars frames."""

import pandas as pd
import polars as pl
import pytest
from pandas.testing import assert_frame_equal

import gridstatus.isone as isone_module
from gridstatus import utils
from gridstatus.isone import ISONE

TZ = ISONE.default_timezone


def _to_pandas(df: object) -> pd.DataFrame:
    if utils.is_polars(df):
        return df.to_pandas()
    return df.reset_index(drop=True)


class TestUtilsDispatch:
    def test_is_polars(self):
        assert utils.is_polars(pl.DataFrame({"a": [1]}))
        assert not utils.is_polars(pd.DataFrame({"a": [1]}))

    def test_concat_dataframes_dispatch(self):
        pl_out = utils.concat_dataframes(
            [pl.DataFrame({"a": [1]}), pl.DataFrame({"a": [2]})],
        )
        assert utils.is_polars(pl_out)
        assert pl_out.shape == (2, 1)

        pd_out = utils.concat_dataframes(
            [pd.DataFrame({"a": [1]}), pd.DataFrame({"a": [2]})],
        )
        assert isinstance(pd_out, pd.DataFrame)
        assert pd_out.shape == (2, 1)

    def test_move_cols_to_front_polars(self):
        df = pl.DataFrame({"a": [1], "Time": [2], "b": [3]})
        assert utils.move_cols_to_front(df, ["Time"]).columns == ["Time", "a", "b"]

    def test_filter_lmp_locations_polars(self):
        df = pl.DataFrame(
            {"Location": ["A", "B"], "Location Type": ["Z", "Y"], "v": [1, 2]},
        )
        out = utils.filter_lmp_locations(df, locations=["A"], location_type="ALL")
        assert out.to_dicts() == [{"Location": "A", "Location Type": "Z", "v": 1}]

    def test_localize_ambiguous_infer_polars_dst_fall_back(self):
        naive = pd.to_datetime(
            pd.Series(
                [
                    "2023-11-05 00:55:00",
                    "2023-11-05 01:00:00",
                    "2023-11-05 01:00:00",
                    "2023-11-05 02:00:00",
                ],
            ),
        )
        df = pl.DataFrame({"Time": naive.to_numpy(), "v": [1, 2, 3, 4]})
        out = utils.localize_ambiguous_infer_polars(df, "Time", TZ)
        utc = (
            out.select(pl.col("Time").dt.convert_time_zone("UTC"))
            .to_series()
            .dt.strftime("%Y-%m-%d %H:%M")
            .to_list()
        )
        assert utc == [
            "2023-11-05 04:55",
            "2023-11-05 05:00",
            "2023-11-05 06:00",
            "2023-11-05 07:00",
        ]

    def test_create_interval_start_from_hour_start_polars(self):
        df = pl.DataFrame(
            {
                "Date": ["2024-01-01", "2024-01-01"],
                "Hour Ending": ["1", "02X"],
            },
        )
        out = utils.create_interval_start_from_hour_start_polars(df)
        starts = out["Interval Start"].dt.strftime("%Y-%m-%d %H:%M").to_list()
        assert starts == ["2024-01-01 00:00", "2024-01-01 01:00"]


def _fuel_mix_raw():
    return pd.DataFrame(
        {
            "Date": ["2024-01-01"] * 4,
            "Time": ["00:00:00", "00:00:00", "00:05:00", "00:05:00"],
            "Fuel Category": ["Solar", "Wind", "Solar", "Wind"],
            "Gen Mw": [10.0, 20.0, 11.0, 21.0],
        },
    )


def _load_raw():
    return pd.DataFrame(
        {
            "Date/Time": ["2024-01-01 00:00:00", "2024-01-01 00:05:00"],
            "Native Load": [100.0, 110.0],
        },
    )


def _load_dst_raw():
    times = [
        "2023-11-05 00:55:00",
        "2023-11-05 01:00:00",
        "2023-11-05 01:00:00",
        "2023-11-05 02:00:00",
    ]
    return pd.DataFrame({"Date/Time": times, "Native Load": [1.0, 2.0, 3.0, 4.0]})


def _system_load_records(series):
    base = [
        {
            "BeginDate": "2024-01-01T00:00:00.000-05:00",
            "Mw": 50.0,
            "NativeLoadBtmPv": 60.0,
        },
        {
            "BeginDate": "2024-01-01T01:00:00.000-05:00",
            "Mw": 55.0,
            "NativeLoadBtmPv": 65.0,
        },
    ]
    if series == "forecast":
        for r in base:
            r["CreationDate"] = "2023-12-31T10:00:00.000-05:00"
    return [{"data": {series: base}}]


class TestISONEPolarsMethods:
    @pytest.mark.parametrize("raw_factory", [_fuel_mix_raw])
    def test_get_fuel_mix_returns_polars(self, monkeypatch, raw_factory):
        monkeypatch.setattr(
            isone_module,
            "_make_request",
            lambda url, skiprows, verbose: raw_factory().copy(),
        )
        df = ISONE().get_fuel_mix(date="2024-01-01")
        assert utils.is_polars(df)
        assert df.columns == ["Time", "Solar", "Wind"]
        assert df.height == 2

    @pytest.mark.parametrize("raw_factory", [_load_raw, _load_dst_raw])
    def test_get_load_returns_polars(self, monkeypatch, raw_factory):
        monkeypatch.setattr(
            isone_module,
            "_make_request",
            lambda url, skiprows, verbose: raw_factory().copy(),
        )
        df = ISONE().get_load(date="2023-11-05")
        assert utils.is_polars(df)
        assert list(df.columns) == [
            "Time",
            "Interval Start",
            "Interval End",
            "Load",
        ]
        assert df.height == len(raw_factory())

    def test_get_load_forecast_returns_polars(self, monkeypatch):
        monkeypatch.setattr(
            isone_module,
            "_make_wsclient_request",
            lambda url, data, verbose=False: _system_load_records("forecast"),
        )
        df = ISONE().get_load_forecast(date="2024-01-01")
        assert utils.is_polars(df)
        assert set(df.columns) == {
            "Time",
            "Interval Start",
            "Interval End",
            "Forecast Time",
            "Load Forecast",
        }

    def test_get_btm_solar_returns_polars(self, monkeypatch):
        monkeypatch.setattr(
            isone_module,
            "_make_wsclient_request",
            lambda url, data, verbose=False: _system_load_records("actual"),
        )
        df = ISONE().get_btm_solar(date="2024-01-01")
        assert utils.is_polars(df)
        out = _to_pandas(df)
        assert list(out.columns) == [
            "Time",
            "Interval Start",
            "Interval End",
            "BTM Solar",
        ]
        assert_frame_equal(
            out[["BTM Solar"]],
            pd.DataFrame({"BTM Solar": [10.0, 10.0]}),
            check_dtype=False,
        )
