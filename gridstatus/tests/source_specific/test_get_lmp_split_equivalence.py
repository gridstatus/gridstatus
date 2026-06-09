"""Equivalence tests for the per-dataset LMP methods introduced in ENG-3978.

Each new ``get_lmp_*`` method should return the same data as the (now
deprecated) ``get_lmp(market=..., locations="ALL")`` call it replaces.

These tests hit external sources and must be recorded with VCR before they
will pass offline:

    VCR_RECORD_MODE=all uv run pytest -vvv \
        gridstatus/tests/source_specific/test_get_lmp_split_equivalence.py

Cassettes are gitignored, so recording them does not add repo bloat.
"""

import warnings

import pandas as pd
import pytest

import gridstatus
from gridstatus.base import Markets
from gridstatus.nyiso import NYISOLocationType
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

VERBOSE = False


def _old_get_lmp(iso, **kwargs) -> pd.DataFrame:
    """Call the deprecated get_lmp without surfacing the DeprecationWarning."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return iso.get_lmp(**kwargs)


def _assert_equivalent(
    new_df: pd.DataFrame,
    old_df: pd.DataFrame,
    sort_cols: list[str],
) -> None:
    assert list(new_df.columns) == list(old_df.columns)

    new_sorted = new_df.sort_values(sort_cols).reset_index(drop=True)
    old_sorted = old_df.sort_values(sort_cols).reset_index(drop=True)

    pd.testing.assert_frame_equal(new_sorted, old_sorted, check_dtype=False)


caiso_vcr = setup_vcr(source="caiso", record_mode=RECORD_MODE)
ercot_vcr = setup_vcr(source="ercot", record_mode=RECORD_MODE)
pjm_vcr = setup_vcr(source="pjm", record_mode=RECORD_MODE)
miso_vcr = setup_vcr(source="miso", record_mode=RECORD_MODE)
nyiso_vcr = setup_vcr(source="nyiso", record_mode=RECORD_MODE)
isone_vcr = setup_vcr(source="isone", record_mode=RECORD_MODE)


class TestCAISOLmpSplit:
    iso = gridstatus.CAISO()
    date = "2024-01-02"

    @pytest.mark.parametrize(
        "method,market",
        [
            ("get_lmp_real_time_5_min", Markets.REAL_TIME_5_MIN),
            ("get_lmp_real_time_15_min", Markets.REAL_TIME_15_MIN),
            ("get_lmp_day_ahead_hourly", Markets.DAY_AHEAD_HOURLY),
        ],
    )
    def test_split_matches_deprecated_get_lmp(self, method, market):
        cassette_name = f"test_lmp_split_{method}_{self.date}"
        with caiso_vcr.use_cassette(cassette_name):
            new_df = getattr(self.iso, method)(date=self.date, verbose=VERBOSE)
            old_df = _old_get_lmp(
                self.iso,
                date=self.date,
                market=market,
                locations="ALL",
                verbose=VERBOSE,
            )

        _assert_equivalent(new_df, old_df, ["Interval Start", "Location"])


class TestErcotLmpSplit:
    iso = gridstatus.Ercot()
    date = "2024-01-02"

    @pytest.mark.parametrize(
        "method,location_type",
        [
            ("get_lmp_by_settlement_point", "Settlement Point"),
            ("get_lmp_by_bus", "Electrical Bus"),
        ],
    )
    def test_split_matches_deprecated_get_lmp(self, method, location_type):
        cassette_name = f"test_lmp_split_{method}_{self.date}"
        with ercot_vcr.use_cassette(cassette_name):
            new_df = getattr(self.iso, method)(date=self.date, verbose=VERBOSE)
            old_df = _old_get_lmp(
                self.iso,
                date=self.date,
                location_type=location_type,
                verbose=VERBOSE,
            )

        _assert_equivalent(new_df, old_df, ["SCED Timestamp", "Location"])


class TestPJMLmpSplit:
    iso = gridstatus.PJM()
    date = "2024-01-02"

    @pytest.mark.parametrize(
        "method,market",
        [
            ("get_lmp_real_time_5_min", Markets.REAL_TIME_5_MIN),
            ("get_lmp_real_time_hourly", Markets.REAL_TIME_HOURLY),
            ("get_lmp_day_ahead_hourly", Markets.DAY_AHEAD_HOURLY),
        ],
    )
    def test_split_matches_deprecated_get_lmp(self, method, market):
        cassette_name = f"test_lmp_split_{method}_{self.date}"
        with pjm_vcr.use_cassette(cassette_name):
            new_df = getattr(self.iso, method)(date=self.date, verbose=VERBOSE)
            old_df = _old_get_lmp(
                self.iso,
                date=self.date,
                market=market,
                locations="ALL",
                verbose=VERBOSE,
            )

        _assert_equivalent(new_df, old_df, ["Interval Start", "Location Id"])


class TestMISOLmpSplit:
    iso = gridstatus.MISO()

    def test_real_time_5_min_matches_deprecated_get_lmp(self):
        cassette_name = "test_lmp_split_miso_real_time_5_min"
        with miso_vcr.use_cassette(cassette_name):
            new_df = self.iso.get_lmp_real_time_5_min(date="latest", verbose=VERBOSE)
            old_df = _old_get_lmp(
                self.iso,
                date="latest",
                market=Markets.REAL_TIME_5_MIN,
                locations="ALL",
                verbose=VERBOSE,
            )

        _assert_equivalent(new_df, old_df, ["Interval Start", "Location"])

    def test_day_ahead_hourly_matches_deprecated_get_lmp(self):
        date = "2024-01-02"
        cassette_name = f"test_lmp_split_miso_day_ahead_hourly_{date}"
        with miso_vcr.use_cassette(cassette_name):
            new_df = self.iso.get_lmp_day_ahead_hourly(date=date, verbose=VERBOSE)
            old_df = _old_get_lmp(
                self.iso,
                date=date,
                market=Markets.DAY_AHEAD_HOURLY,
                locations="ALL",
                verbose=VERBOSE,
            )

        _assert_equivalent(new_df, old_df, ["Interval Start", "Location"])

    def test_real_time_hourly_prelim_matches_deprecated_get_lmp(self):
        # Prelim data is only available for ~4 days, so use a recent date.
        date = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(days=2)
        cassette_name = (
            f"test_lmp_split_miso_real_time_hourly_prelim_{date.strftime('%Y-%m-%d')}"
        )
        with miso_vcr.use_cassette(cassette_name):
            new_df = self.iso.get_lmp_real_time_hourly_prelim(
                date=date,
                verbose=VERBOSE,
            )
            old_df = _old_get_lmp(
                self.iso,
                date=date,
                market=Markets.REAL_TIME_HOURLY_PRELIM,
                locations="ALL",
                verbose=VERBOSE,
            )

        _assert_equivalent(new_df, old_df, ["Interval Start", "Location"])

    def test_real_time_hourly_final_matches_deprecated_get_lmp(self):
        date = "2024-01-02"
        cassette_name = f"test_lmp_split_miso_real_time_hourly_final_{date}"
        with miso_vcr.use_cassette(cassette_name):
            new_df = self.iso.get_lmp_real_time_hourly_final(date=date, verbose=VERBOSE)
            old_df = _old_get_lmp(
                self.iso,
                date=date,
                market=Markets.REAL_TIME_HOURLY_FINAL,
                locations="ALL",
                verbose=VERBOSE,
            )

        _assert_equivalent(new_df, old_df, ["Interval Start", "Location"])


class TestNYISOLmpSplit:
    iso = gridstatus.NYISO()
    date = "2024-01-02"

    def _expected_zone_and_generator(self, market: Markets) -> pd.DataFrame:
        zone = _old_get_lmp(
            self.iso,
            date=self.date,
            market=market,
            location_type=NYISOLocationType.ZONE,
            verbose=VERBOSE,
        )
        generator = _old_get_lmp(
            self.iso,
            date=self.date,
            market=market,
            location_type=NYISOLocationType.GENERATOR,
            verbose=VERBOSE,
        )

        df = pd.concat([zone, generator], axis=0)

        if market == Markets.REAL_TIME_5_MIN:
            maximum_timestamp = min(
                zone["Interval Start"].max(),
                generator["Interval Start"].max(),
            )
            df = df[df["Interval Start"] <= maximum_timestamp]

        return df.sort_values(["Interval Start", "Location"]).reset_index(drop=True)

    @pytest.mark.parametrize(
        "method,market",
        [
            ("get_lmp_real_time_hourly", Markets.REAL_TIME_HOURLY),
            ("get_lmp_day_ahead_hourly", Markets.DAY_AHEAD_HOURLY),
        ],
    )
    def test_split_matches_zone_and_generator_concat(self, method, market):
        cassette_name = f"test_lmp_split_{method}_{self.date}"
        with nyiso_vcr.use_cassette(cassette_name):
            new_df = getattr(self.iso, method)(date=self.date, verbose=VERBOSE)
            expected = self._expected_zone_and_generator(market)

        _assert_equivalent(new_df, expected, ["Interval Start", "Location"])


class TestISONELmpSplit:
    iso = gridstatus.ISONE()
    date = "2024-01-02"

    @pytest.mark.parametrize(
        "method,market",
        [
            ("get_lmp_real_time_5_min", Markets.REAL_TIME_5_MIN),
            ("get_lmp_real_time_hourly", Markets.REAL_TIME_HOURLY),
            ("get_lmp_day_ahead_hourly", Markets.DAY_AHEAD_HOURLY),
        ],
    )
    def test_split_matches_deprecated_get_lmp(self, method, market):
        cassette_name = f"test_lmp_split_isone_{method}_{self.date}"
        with isone_vcr.use_cassette(cassette_name):
            new_df = getattr(self.iso, method)(date=self.date, verbose=VERBOSE)
            old_df = _old_get_lmp(
                self.iso,
                date=self.date,
                market=market,
                locations="ALL",
                verbose=VERBOSE,
            )

        _assert_equivalent(new_df, old_df, ["Interval Start", "Location"])
