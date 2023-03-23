import os.path
import re
import sys

import pandas as pd
import pytest

from gridstatus import SPP, Markets, NotSupported
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets


class TestSPP(BaseTestISO):
    iso = SPP()

    """get_fuel_mix"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_date_or_start(self):
        pass

    def test_get_fuel_mix_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_historical_with_date_range(self):
        pass

    def test_get_fuel_mix_central_time(self):
        fm = self.iso.get_fuel_mix(date="latest")
        assert fm.Time.iloc[0].tz.zone == self.iso.default_timezone

    """get_lmp"""

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_historical(self, market):
        super().test_get_lmp_historical(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_today(self, market):
        super().test_get_lmp_today(market=market)

    @with_markets(
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_latest(self, market):
        super().test_get_lmp_latest(market=market)

    @pytest.mark.parametrize(
        "market,location_type",
        [
            (Markets.REAL_TIME_5_MIN, "Hub"),
            (Markets.REAL_TIME_5_MIN, "Interface"),
        ],
    )
    def test_get_lmp_latest_with_locations(self, market, location_type):
        df = self.iso.get_lmp(
            date="latest",
            market=market,
            location_type=location_type,
        )
        self._check_lmp_columns(df, market)

        location_types = df["Location Type"].unique()
        assert len(location_types) == 1
        assert location_types[0] == location_type

    def test_get_lmp_latest_settlement_type_returns_three_location_types(self):
        market = Markets.REAL_TIME_5_MIN
        df = self.iso.get_lmp(
            date="latest",
            market=market,
            location_type="SETTLEMENT_LOCATION",
        )
        self._check_lmp_columns(df, market)

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
    def test_get_lmp_today_with_location(self, market, location_type):
        df = self.iso.get_lmp(
            date="today",
            market=market,
            location_type=location_type,
        )
        self._check_lmp_columns(df, market=market)
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
    def test_get_lmp_unsupported_raises_not_supported(
        self,
        date,
        market,
        location_type,
    ):
        with pytest.raises(NotSupported):
            self.iso.get_lmp(
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
    def test_get_lmp_day_ahead_cannot_have_latest(self, date, market, location_type):
        with pytest.raises(ValueError):
            self.iso.get_lmp(
                date=date,
                market=market,
                location_type=location_type,
            )

    """get_load"""

    def test_get_load_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_load_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_historical_with_date_range(self):
        pass

    """get_load_forecast"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical_with_date_range(self):
        pass

    """get_status"""

    def test__get_status_from_fixtures(self):
        iso = SPP()

        self._assert_grid_status_fixture(
            "fixtures/spp/grid-conditions-20210215.html",
            {
                "iso.name": iso.name,
                "notes.contains": (
                    "SPP declared an Energy Emergency Alert (EEA) Level 3"
                ),
                "reserves": None,
                "status": "Energy Emergency Alert Level 3",
                "time": pd.Timestamp("2021-02-15 11:08:00-06:00"),
                "unit": "MW",
            },
        )

        self._assert_grid_status_fixture(
            "fixtures/spp/grid-conditions-20210217.html",
            {
                "iso.name": iso.name,
                "notes.contains": (
                    "SPP declared an Energy Emergency Alert (EEA) Level 2"
                ),
                "reserves": None,
                "status": "Energy Emergency Alert Level 2",
                "time": pd.Timestamp("2021-02-17 18:42:00-06:00"),
                "unit": "MW",
            },
        )

        self._assert_grid_status_fixture(
            "fixtures/spp/grid-conditions-20221010.html",
            {
                "iso.name": iso.name,
                "notes.contains": "normal operations",
                "reserves": None,
                "status": "Normal",
                "time": pd.Timestamp("2022-10-10 14:57:00-05:00"),
                "unit": "MW",
            },
        )

        self._assert_grid_status_fixture(
            "fixtures/spp/grid-conditions-20230101.html",
            {
                "iso.name": iso.name,
                "notes.contains": "normal operations",
                "reserves": None,
                "status": "Normal",
                "time": pd.Timestamp("2022-12-26 10:00:00-06:00"),
                "unit": "MW",
            },
        )

    def test__get_status_timestamp(self):
        self._assert_get_status_timestamp(
            "2022-12-26 10:00",
            "US/Central",
            "SPP is in normal operations as of Dec. 26, 2022 at 10:00 a.m. CT.",
        )
        self._assert_get_status_timestamp(
            "2021-02-15 11:08",
            "US/Central",
            (
                "Current Grid Conditions "
                "(last updated Feb. 15 at 11:08 a.m. Central time):"
            ),
            year_hint=2021,
        )
        self._assert_get_status_timestamp(
            "2022-10-10 14:57",
            "US/Central",
            "(Last updated October 10, 2022, at 2:57 p.m. Central Time)",
        )

    """get_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

    @staticmethod
    def _assert_get_status_timestamp(expected, expected_tz, *actuals, year_hint=None):
        for actual in actuals:
            assert SPP()._get_status_timestamp(
                [actual],
                year_hint=year_hint,
            ) == pd.Timestamp(expected, tz=expected_tz)

    def _assert_grid_status_fixture(self, filename, expected):
        actual = self._get_status_from_fixture(filename)
        print(f"actual = {actual}", file=sys.stderr)
        try:
            assert actual.iso.name == expected["iso.name"]
            assert actual.status == expected["status"]
            assert actual.time == expected["time"]
            assert actual.reserves == expected["reserves"]
            assert actual.unit == expected["unit"]

            notes_contains_match = any(
                expected["notes.contains"].lower() in note.lower()
                for note in actual.notes
            )
            if not notes_contains_match:
                raise AssertionError(
                    f"Could not find {repr(expected['notes.contains'])} "
                    f"in {repr(actual.notes)}",
                )
        except AssertionError as e:
            raise AssertionError(f"{filename}: {e}") from e

    def _get_status_from_fixture(self, filename, year_hint=None):
        """Load fixture, deriving year_hint from filename if not provided"""
        if year_hint is None:
            year_hint_group = re.search(r"-([0-9]{4})", filename)
            if year_hint_group:
                year_hint = year_hint_group.group(1)
        filename_path = os.path.dirname(__file__) + "/" + filename
        with open(filename_path, "r") as f:
            contents = f.read()
        try:
            status = self.iso._get_status_from_html(
                contents,
                year_hint=year_hint,
            )
        except Exception as e:
            raise Exception(f"Error parsing {filename}: {e}")
        return status
