from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from gridstatus import SPP, Markets, NoDataFoundException, NotSupported
from gridstatus.spp import (
    BAA_LOAD_THRESHOLD_MW,
    LOCATION_TYPE_BUS,
    LOCATION_TYPE_HUB,
    LOCATION_TYPE_INTERFACE,
    LOCATION_TYPE_SETTLEMENT_LOCATION,
    BAAEnum,
    fill_baa_column,
)
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

api_vcr = setup_vcr(
    source="spp",
    record_mode=RECORD_MODE,
)


class TestSPP(BaseTestISO):
    iso = SPP()

    # -- Base class tests using today/latest/relative dates → mark integration --

    @pytest.mark.skip(
        reason="SPP BAA column added to outputs - https://www.notion.so/33de835f42aa81f0a147cbf7490f4f85"
    )
    @pytest.mark.integration
    def test_get_load_latest(self):
        super().test_get_load_latest()

    @pytest.mark.skip(
        reason="SPP BAA column added to outputs - https://www.notion.so/33de835f42aa81f0a147cbf7490f4f85"
    )
    @pytest.mark.integration
    def test_get_load_today(self):
        super().test_get_load_today()

    """get_fuel_mix"""

    FUEL_MIX_COLS = [
        "Interval Start",
        "Interval End",
        "Coal",
        "Diesel Fuel Oil",
        "Hydro",
        "Natural Gas",
        "Nuclear",
        "Solar",
        "Waste Disposal Services",
        "Wind",
        "Waste Heat",
        "Other",
    ]

    FUEL_MIX_DETAILED_COLS = [
        "Interval Start",
        "Interval End",
        "Coal Market",
        "Coal Self",
        "Diesel Fuel Oil Market",
        "Diesel Fuel Oil Self",
        "Hydro Market",
        "Hydro Self",
        "Natural Gas Market",
        "Natural Gas Self",
        "Nuclear Market",
        "Nuclear Self",
        "Solar Market",
        "Solar Self",
        "Waste Disposal Services Market",
        "Waste Disposal Services Self",
        "Wind Market",
        "Wind Self",
        "Waste Heat Market",
        "Waste Heat Self",
        "Other Market",
        "Other Self",
    ]

    FUEL_MIX_BAA_COLS = [
        "Interval Start",
        "Interval End",
        "BAA",
        "Coal",
        "Diesel Fuel Oil",
        "Hydro",
        "Natural Gas",
        "Nuclear",
        "Solar",
        "Waste Disposal Services",
        "Wind",
        "Waste Heat",
        "Other",
    ]

    FUEL_MIX_DETAILED_BAA_COLS = [
        "Interval Start",
        "Interval End",
        "BAA",
        "Coal Market",
        "Coal Self",
        "Diesel Fuel Oil Market",
        "Diesel Fuel Oil Self",
        "Hydro Market",
        "Hydro Self",
        "Natural Gas Market",
        "Natural Gas Self",
        "Nuclear Market",
        "Nuclear Self",
        "Solar Market",
        "Solar Self",
        "Waste Disposal Services Market",
        "Waste Disposal Services Self",
        "Wind Market",
        "Wind Self",
        "Waste Heat Market",
        "Waste Heat Self",
        "Other Market",
        "Other Self",
    ]

    def _check_fuel_mix(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.name is None
        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

    # Base test uses dates >365 days old, which raises NotSupported
    @pytest.mark.integration
    def test_get_fuel_mix_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_historical(time_column="Interval Start")

    @pytest.mark.integration
    def test_get_fuel_mix_date_or_start(self):
        super().test_get_fuel_mix_date_or_start()

    @pytest.mark.integration
    def test_get_fuel_mix_historical_with_date_range(self):
        super().test_get_fuel_mix_historical_with_date_range(
            time_column="Interval Start",
        )

    @pytest.mark.integration
    def test_get_fuel_mix_range_two_days_with_day_start_endpoint(self):
        super().test_get_fuel_mix_range_two_days_with_day_start_endpoint(
            time_column="Interval Start",
        )

    @pytest.mark.integration
    def test_get_fuel_mix_start_end_same_day(self):
        super().test_get_fuel_mix_start_end_same_day(
            time_column="Interval Start",
        )

    @pytest.mark.integration
    def test_get_fuel_mix_latest(self):
        fm = self.iso.get_fuel_mix(date="latest")

        assert len(fm) > 0
        assert fm.columns.tolist() == self.FUEL_MIX_COLS
        assert fm["Interval Start"].iloc[0].tz.zone == self.iso.default_timezone
        assert "BAA" not in fm.columns

    @pytest.mark.integration
    def test_get_fuel_mix_today(self):
        fm = self.iso.get_fuel_mix(date="today")

        assert len(fm) > 0
        assert fm.columns.tolist() == self.FUEL_MIX_COLS
        assert "BAA" not in fm.columns

    @pytest.mark.integration
    def test_get_fuel_mix_detailed_latest(self):
        fm = self.iso.get_fuel_mix_detailed(date="latest")

        assert len(fm) > 0
        assert fm.columns.tolist() == self.FUEL_MIX_DETAILED_COLS
        assert "BAA" not in fm.columns

    def test_get_fuel_mix_too_old_raises(self):
        old_date = pd.Timestamp("2024-09-01", tz=self.iso.default_timezone)
        with pytest.raises(NotSupported):
            self.iso.get_fuel_mix(date=old_date)

    def test_get_fuel_mix_historical_recent(self):
        date = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        with api_vcr.use_cassette("test_get_fuel_mix_historical_recent.yaml"):
            fm = self.iso.get_fuel_mix(date=date)

        assert len(fm) > 0
        assert fm.columns.tolist() == self.FUEL_MIX_COLS
        assert "BAA" not in fm.columns
        assert fm["Interval Start"].min() >= date

    """get_fuel_mix_by_baa"""

    @pytest.mark.integration
    def test_get_fuel_mix_by_baa_latest(self):
        fm = self.iso.get_fuel_mix_by_baa(date="latest")

        assert len(fm) > 0
        assert fm.columns.tolist() == self.FUEL_MIX_BAA_COLS
        assert fm["Interval Start"].iloc[0].tz.zone == self.iso.default_timezone
        assert set(fm["BAA"].unique()) == {"SPP", "SWPW"}

    @pytest.mark.integration
    def test_get_fuel_mix_by_baa_today(self):
        fm = self.iso.get_fuel_mix_by_baa(date="today")

        assert len(fm) > 0
        assert fm.columns.tolist() == self.FUEL_MIX_BAA_COLS
        assert set(fm["BAA"].unique()) == {"SPP", "SWPW"}

    def test_get_fuel_mix_by_baa_historical_recent(self):
        date = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        with api_vcr.use_cassette(
            "test_get_fuel_mix_by_baa_historical_recent.yaml",
        ):
            fm = self.iso.get_fuel_mix_by_baa(date=date)

        assert len(fm) > 0
        assert fm.columns.tolist() == self.FUEL_MIX_BAA_COLS
        assert set(fm["BAA"].unique()).issubset({"SPP", "SWPW"})
        assert "SPP" in fm["BAA"].values
        assert fm["Interval Start"].min() >= date

    """get_fuel_mix_by_baa_detailed"""

    @pytest.mark.integration
    def test_get_fuel_mix_by_baa_detailed_latest(self):
        fm = self.iso.get_fuel_mix_by_baa_detailed(date="latest")

        assert len(fm) > 0
        assert fm.columns.tolist() == self.FUEL_MIX_DETAILED_BAA_COLS
        assert set(fm["BAA"].unique()) == {"SPP", "SWPW"}

    """get_lmp_real_time_5_min_by_location"""

    def _check_lmp_real_time_5_min_by_location(
        self,
        df,
        location_types=[
            LOCATION_TYPE_HUB,
            LOCATION_TYPE_INTERFACE,
            LOCATION_TYPE_SETTLEMENT_LOCATION,
        ],
    ):
        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "Market",
            "Location",
            "Location Type",
            "PNode",
            "LMP",
            "Energy",
            "Congestion",
            "Loss",
        ]

        assert set(df["Location Type"]) == set(location_types)

        assert df["Market"].unique() == [Markets.REAL_TIME_5_MIN.value]
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()

        assert np.allclose(df["LMP"], df["Energy"] + df["Congestion"] + df["Loss"])

    @pytest.mark.skip(
        reason="SPP BAA column added to outputs - https://www.notion.so/33de835f42aa81f0a147cbf7490f4f85"
    )
    @pytest.mark.integration
    def test_get_lmp_real_time_5_min_by_location_latest(self):
        df = self.iso.get_lmp_real_time_5_min_by_location(date="latest")

        self._check_lmp_real_time_5_min_by_location(df)

        # Latest data should have one interval
        assert df["Interval Start"].nunique() == 1
        # Check that the max interval is relatively recent (within last 24 hours)
        max_interval = df["Interval Start"].max()
        assert max_interval >= self.local_start_of_today() - pd.Timedelta(days=1)
        assert max_interval <= self.local_now() + pd.Timedelta(hours=1)

    @pytest.mark.integration
    @pytest.mark.slow
    def test_get_lmp_real_time_5_min_by_location_today(self):
        df = self.iso.get_lmp_real_time_5_min_by_location(
            date="today",
            verbose=True,
        )

        self._check_lmp_real_time_5_min_by_location(df)

        assert df["Interval Start"].min() >= self.local_start_of_today()
        assert df["Interval End"].max() >= self.local_now().floor(
            "5min",
        ) - pd.DateOffset(minutes=10)

    def test_get_lmp_real_time_5_min_by_location_date_range(self):
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        end = start + pd.DateOffset(hours=2, minutes=15)

        with api_vcr.use_cassette(
            "test_get_lmp_real_time_5_min_by_location_date_range.yaml",
        ):
            df = self.iso.get_lmp_real_time_5_min_by_location(
                date=start,
                end=end,
                verbose=True,
            )

        self._check_lmp_real_time_5_min_by_location(df)

        assert df["Interval Start"].min() == start
        assert df["Interval End"].max() == end

    def test_get_lmp_real_time_5_min_by_location_historical_date(self):
        # For a historical date, the decorator only retrieves a single interval
        date = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)

        with api_vcr.use_cassette(
            "test_get_lmp_real_time_5_min_by_location_historical_date.yaml",
        ):
            df = self.iso.get_lmp_real_time_5_min_by_location(
                date=date,
                verbose=True,
            )

        self._check_lmp_real_time_5_min_by_location(df)

        assert df["Interval Start"].min() == date
        assert df["Interval End"].max() == date + pd.DateOffset(minutes=5)

    def test_get_lmp_real_time_5_min_by_location_last_interval_of_day(self):
        day = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        last_interval_start = day + pd.DateOffset(hours=23, minutes=55)

        with api_vcr.use_cassette(
            "test_get_lmp_real_time_5_min_by_location_last_interval_of_day.yaml",
        ):
            df = self.iso.get_lmp_real_time_5_min_by_location(
                (last_interval_start, last_interval_start + pd.DateOffset(minutes=5)),
            )

        self._check_lmp_real_time_5_min_by_location(df)

        assert df["Interval Start"].min() == last_interval_start
        assert df["Interval End"].max() == last_interval_start + pd.DateOffset(
            minutes=5
        )

    @pytest.mark.skip(
        reason="SPP BAA column added to outputs - https://www.notion.so/33de835f42aa81f0a147cbf7490f4f85"
    )
    @pytest.mark.integration
    @pytest.mark.parametrize(
        "location_type",
        [
            LOCATION_TYPE_HUB,
            LOCATION_TYPE_INTERFACE,
            LOCATION_TYPE_SETTLEMENT_LOCATION,
        ],
    )
    def test_get_lmp_real_time_5_min_by_location_filters_location(self, location_type):
        df = self.iso.get_lmp_real_time_5_min_by_location(
            date="latest",
            location_type=location_type,
        )

        self._check_lmp_real_time_5_min_by_location(df, location_types=[location_type])

    @pytest.mark.integration
    def test_get_lmp_real_time_5_min_by_location_dst_end(self):
        with api_vcr.use_cassette(
            "test_get_lmp_real_time_5_min_by_location_dst_end.yaml",
        ):
            df = self.iso.get_lmp_real_time_5_min_by_location(
                pd.Timestamp("2024-11-03 00:55:00-05:00"),
                pd.Timestamp("2024-11-03 01:25:00-06:00"),
            )

        # Note the missing files between '2024-11-03 01:05:00-05:00' and
        # '2024-11-03 01:10:00-06:00'
        assert all(
            df["Interval End"].unique()
            == pd.to_datetime(
                [
                    "2024-11-03 01:00:00-05:00",
                    "2024-11-03 01:05:00-05:00",
                    "2024-11-03 01:10:00-06:00",
                    "2024-11-03 01:15:00-06:00",
                    "2024-11-03 01:20:00-06:00",
                    "2024-11-03 01:25:00-06:00",
                ],
            ),
        )

    def test_get_lmp_real_time_5_min_by_location_with_daily_files(self):
        """Test that we can get LMP data using daily files."""
        date = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        with api_vcr.use_cassette(
            f"test_get_lmp_real_time_5_min_by_location_with_daily_files_{date.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_lmp_real_time_5_min_by_location(
                date=date,
                use_daily_files=True,
            )

        self._check_lmp_real_time_5_min_by_location(df)
        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(hours=23, minutes=55)

    """get_lmp_real_time_5_min_by_bus"""

    def _check_lmp_real_time_5_min_by_bus(self, df):
        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "Market",
            "Location",
            "Location Type",
            "LMP",
            "Energy",
            "Congestion",
            "Loss",
        ]

        assert df["Market"].unique() == [Markets.REAL_TIME_5_MIN.value]

        assert df["Location Type"].unique() == [LOCATION_TYPE_BUS]
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()

        assert np.allclose(df["LMP"], df["Energy"] + df["Congestion"] + df["Loss"])

    @pytest.mark.skip(
        reason="SPP BAA column added to outputs - https://www.notion.so/33de835f42aa81f0a147cbf7490f4f85"
    )
    @pytest.mark.integration
    def test_get_lmp_real_time_5_min_by_bus_latest(self):
        df = self.iso.get_lmp_real_time_5_min_by_bus(date="latest")

        self._check_lmp_real_time_5_min_by_bus(df)
        # Latest data should have one interval
        assert df["Interval Start"].nunique() == 1
        # Check that the max interval is relatively recent (within last 24 hours)
        max_interval = df["Interval Start"].max()
        assert max_interval >= self.local_start_of_today() - pd.Timedelta(days=1)
        assert max_interval <= self.local_now() + pd.Timedelta(hours=1)

    @pytest.mark.integration
    @pytest.mark.slow
    def test_get_lmp_real_time_5_min_by_bus_today(self):
        df = self.iso.get_lmp_real_time_5_min_by_bus(date="today", verbose=True)

        self._check_lmp_real_time_5_min_by_bus(df)

        assert df["Interval Start"].min() >= self.local_start_of_today()
        assert df["Interval End"].max() >= self.local_now().floor(
            "5min",
        ) - pd.DateOffset(minutes=10)

    def test_get_lmp_real_time_5_min_by_bus_date_range(self):
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        end = start + pd.DateOffset(hours=2, minutes=15)

        with api_vcr.use_cassette(
            "test_get_lmp_real_time_5_min_by_bus_date_range.yaml",
        ):
            df = self.iso.get_lmp_real_time_5_min_by_bus(
                date=start,
                end=end,
            )

        self._check_lmp_real_time_5_min_by_bus(df)

        assert df["Interval Start"].min() == start
        assert df["Interval End"].max() == end

    def test_get_lmp_real_time_5_min_by_bus_historical_date(self):
        # For a historical date, the decorator only retrieves a single interval
        date = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)

        with api_vcr.use_cassette(
            "test_get_lmp_real_time_5_min_by_bus_historical_date.yaml",
        ):
            df = self.iso.get_lmp_real_time_5_min_by_bus(date=date)

        self._check_lmp_real_time_5_min_by_bus(df)

        assert df["Interval Start"].min() == date
        assert df["Interval End"].max() == date + pd.DateOffset(minutes=5)

    def test_get_lmp_real_time_5_min_by_bus_last_interval_of_day(self):
        day = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        last_interval_start = day + pd.DateOffset(hours=23, minutes=55)

        with api_vcr.use_cassette(
            "test_get_lmp_real_time_5_min_by_bus_last_interval_of_day.yaml",
        ):
            df = self.iso.get_lmp_real_time_5_min_by_bus(
                (last_interval_start, last_interval_start + pd.DateOffset(minutes=5)),
            )

        self._check_lmp_real_time_5_min_by_bus(df)

        assert df["Interval Start"].min() == last_interval_start
        assert df["Interval End"].max() == last_interval_start + pd.DateOffset(
            minutes=5
        )

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "date,end",
        [
            (
                pd.Timestamp("2025-11-02 00:55:00-05:00"),
                pd.Timestamp("2025-11-02 01:25:00-06:00"),
            ),
        ],
    )
    def test_get_lmp_real_time_5_min_by_bus_dst_end(self, date, end):
        with api_vcr.use_cassette(
            f"test_get_lmp_real_time_5_min_by_bus_dst_end_{date}_{end}.yaml",
        ):
            df = self.iso.get_lmp_real_time_5_min_by_bus(
                date,
                end,
            )

        # Note the missing files between '2024-11-03 01:05:00-05:00' and
        # '2024-11-03 01:10:00-06:00'
        assert all(
            df["Interval End"].unique()
            == pd.to_datetime(
                [
                    "2024-11-03 01:00:00-05:00",
                    "2024-11-03 01:05:00-05:00",
                    "2024-11-03 01:10:00-06:00",
                    "2024-11-03 01:15:00-06:00",
                    "2024-11-03 01:20:00-06:00",
                    "2024-11-03 01:25:00-06:00",
                ],
            ),
        )

    """get_lmp_day_ahead_hourly"""

    def _check_lmp_day_ahead_hourly(
        self,
        df,
        location_types=[
            LOCATION_TYPE_HUB,
            LOCATION_TYPE_INTERFACE,
            LOCATION_TYPE_SETTLEMENT_LOCATION,
        ],
    ):
        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "Market",
            "BAA",
            "Location",
            "Location Type",
            "PNode",
            "LMP",
            "Energy",
            "Congestion",
            "Loss",
        ]

        assert set(df["Location Type"]) == set(location_types)
        assert df["Market"].unique() == [Markets.DAY_AHEAD_HOURLY.value]
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

        assert np.allclose(df["LMP"], df["Energy"] + df["Congestion"] + df["Loss"])

    @pytest.mark.integration
    def test_get_lmp_day_ahead_hourly_latest_not_supported(self):
        with pytest.raises(NotSupported):
            self.iso.get_lmp_day_ahead_hourly(date="latest")

    @pytest.mark.integration
    def test_get_lmp_day_ahead_hourly_today(self):
        df = self.iso.get_lmp_day_ahead_hourly(date="today")

        self._check_lmp_day_ahead_hourly(df)
        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.DateOffset(
            days=1,
        )

    def test_get_lmp_day_ahead_hourly_date_range(self):
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        end = start + pd.DateOffset(days=2)

        with api_vcr.use_cassette(
            f"test_get_lmp_day_ahead_hourly_date_range_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_lmp_day_ahead_hourly(
                start=start,
                end=end,
            )

        self._check_lmp_day_ahead_hourly(df)

        assert df["Interval Start"].min() == start
        # Not end day inclusive
        assert df["Interval End"].max() == end

    def test_get_lmp_day_ahead_hourly_historical_date(self):
        date = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)

        with api_vcr.use_cassette(
            f"test_get_lmp_day_ahead_hourly_historical_date_{date.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_lmp_day_ahead_hourly(date=date)

        self._check_lmp_day_ahead_hourly(df)

        assert df["Interval Start"].min() == date
        assert df["Interval End"].max() == date + pd.DateOffset(days=1)

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "location_type",
        [
            LOCATION_TYPE_HUB,
            LOCATION_TYPE_INTERFACE,
            LOCATION_TYPE_SETTLEMENT_LOCATION,
        ],
    )
    def test_get_lmp_day_ahead_hourly_filters_location(self, location_type):
        df = self.iso.get_lmp_day_ahead_hourly(
            date="today",
            location_type=location_type,
        )

        self._check_lmp_day_ahead_hourly(df, location_types=[location_type])

    # This is not a method in the class, but the base class calls it. So we need to
    # override these tests
    """get_lmp"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_lmp_date_range(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_lmp_historical(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_lmp_latest(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_lmp_today(self):
        pass

    """get_operating_reserves"""

    OPERATING_RESERVES_COLUMNS = [
        "Time",
        "Interval Start",
        "Interval End",
        "Reserve Zone",
        "Reg_Up_Cleared",
        "Reg_Dn_Cleared",
        "Ramp_Up_Cleared",
        "Ramp_Dn_Cleared",
        "Unc_Up_Cleared",
        "STS_Unc_Up_Cleared",
        "Spin_Cleared",
        "Supp_Cleared",
    ]

    def test_get_operating_reserves(self):
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        end = start + pd.Timedelta(minutes=30)
        with api_vcr.use_cassette(
            f"test_get_operating_reserves_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_operating_reserves(start=start, end=end)
        assert len(df) > 0
        assert df.columns.tolist() == self.OPERATING_RESERVES_COLUMNS

    @pytest.mark.skip(
        reason="SPP BAA column added to outputs - https://www.notion.so/33de835f42aa81f0a147cbf7490f4f85"
    )
    @pytest.mark.integration
    def test_get_operating_reserves_latest(self):
        df = self.iso.get_operating_reserves(date="latest")
        assert len(df) > 0
        assert df.columns.tolist() == self.OPERATING_RESERVES_COLUMNS

    def test_get_operative_reserves_last_interval_of_day(self):
        day = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        last_interval_start = day + pd.Timedelta(hours=23, minutes=55)

        with api_vcr.use_cassette(
            "test_get_operative_reserves_last_interval_of_day.yaml",
        ):
            df = self.iso.get_operating_reserves(
                start=last_interval_start,
                end=last_interval_start + pd.Timedelta(minutes=5),
            )

        assert df["Interval Start"].min() == last_interval_start
        assert df["Interval End"].max() == last_interval_start + pd.Timedelta(minutes=5)
        assert df.columns.tolist() == self.OPERATING_RESERVES_COLUMNS

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "date,end",
        [
            (
                pd.Timestamp("2025-11-02 00:55:00-05:00"),
                pd.Timestamp("2025-11-02 01:25:00-06:00"),
            ),
        ],
    )
    def test_get_operating_reserves_dst_end(self, date, end):
        with api_vcr.use_cassette(
            f"test_get_operating_reserves_dst_end_{date}_{end}.yaml",
        ):
            df = self.iso.get_operating_reserves(
                date,
                end,
            )

        # Note the missing files between '2024-11-03 01:05:00-05:00' and
        # '2024-11-03 01:10:00-06:00'
        assert all(
            df["Interval End"].unique()
            == pd.to_datetime(
                [
                    "2024-11-03 01:00:00-05:00",
                    "2024-11-03 01:05:00-05:00",
                    "2024-11-03 01:10:00-06:00",
                    "2024-11-03 01:15:00-06:00",
                    "2024-11-03 01:20:00-06:00",
                    "2024-11-03 01:25:00-06:00",
                ],
            ),
        )

    DAY_AHEAD_MARGINAL_CLEARING_PRICES_COLUMNS = [
        "Interval Start",
        "Interval End",
        "Market",
        "Reserve Zone",
        "Reg_Up",
        "Reg_Dn",
        "Ramp_Up",
        "Ramp_Dn",
        "Spin",
        "Supp",
        "Unc_Up",
    ]

    def test_get_day_ahead_operating_reserve_prices(self):
        end = pd.Timestamp("2025-11-04", tz=self.iso.default_timezone)
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)

        with api_vcr.use_cassette(
            f"test_get_day_ahead_operating_reserve_prices_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_day_ahead_operating_reserve_prices(
                date=start,
                end=end,
            )

        assert df["Interval Start"].min() == start
        assert df["Interval End"].max() == end
        assert df.columns.tolist() == self.DAY_AHEAD_MARGINAL_CLEARING_PRICES_COLUMNS

    @pytest.mark.integration
    def test_get_day_ahead_operating_reserve_prices_today(self):
        df = self.iso.get_day_ahead_operating_reserve_prices(date="today")

        assert (
            df["Interval Start"].min()
            == pd.Timestamp.now(tz=self.iso.default_timezone).normalize()
        )
        assert df["Interval End"].max() == pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() + pd.Timedelta(days=1)
        assert df.columns.tolist() == self.DAY_AHEAD_MARGINAL_CLEARING_PRICES_COLUMNS

    """get_as_prices_real_time_5_min"""

    REAL_TIME_MCP_COLUMNS = [
        "Interval Start",
        "Interval End",
        "Reserve Zone",
        "Reg Up Service",
        "Reg DN Service",
        "Reg Up Mile",
        "Reg DN Mile",
        "Ramp Up",
        "Ramp DN",
        "Spin",
        "Supp",
        "Unc Up",
    ]

    def _check_as_prices_real_time_5_min(self, df: pd.DataFrame):
        assert list(df.columns) == self.REAL_TIME_MCP_COLUMNS
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()

        for col in self.REAL_TIME_MCP_COLUMNS[3:]:
            assert pd.api.types.is_numeric_dtype(df[col])

    @pytest.mark.integration
    def test_get_as_prices_real_time_5_min_latest(self):
        df = self.iso.get_as_prices_real_time_5_min(date="latest")

        self._check_as_prices_real_time_5_min(df)

    def test_get_as_prices_real_time_5_min_historical_date(self):
        day_anchor = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        date = day_anchor + pd.DateOffset(hours=10, minutes=30)

        cassette_name = f"test_get_as_prices_real_time_5_min_historical_date_{date.strftime('%Y%m%d_%H%M')}.yaml"

        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_as_prices_real_time_5_min(date=date)

        self._check_as_prices_real_time_5_min(df)
        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date

    def test_get_as_prices_real_time_5_min_date_range(self):
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        end = start + pd.DateOffset(hours=2, minutes=15)

        cassette_name = f"test_get_as_prices_real_time_5_min_date_range_{start.strftime('%Y%m%d_%H%M')}_to_{end.strftime('%Y%m%d_%H%M')}.yaml"

        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_as_prices_real_time_5_min(
                date=start,
                end=end,
            )

        self._check_as_prices_real_time_5_min(df)
        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(
            minutes=5,
        )

    @pytest.mark.integration
    def test_get_as_prices_real_time_5_min_with_daily_files(self):
        """Test that we can get AS prices data using daily files."""
        # Use a fixed recent date that is known to have daily files available
        target_date = pd.Timestamp("2026-03-25", tz=self.iso.default_timezone)

        cassette_name = f"test_get_as_prices_real_time_5_min_daily_files_{target_date.strftime('%Y%m%d')}.yaml"

        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_as_prices_real_time_5_min(
                date=target_date,
                use_daily_files=True,
            )

        self._check_as_prices_real_time_5_min(df)

        # Daily files should contain a full day of data
        assert df["Interval Start"].min() == target_date
        assert df["Interval Start"].max() == target_date + pd.Timedelta(
            hours=23,
            minutes=55,
        )

        # Should have significantly more data than single intervals
        # (24 hours * 12 intervals/hour * 6 reserve zones = 1728 rows expected)
        assert len(df) > 1000

    @pytest.mark.integration
    def test_get_as_prices_real_time_5_min_daily_files_latest_not_supported(self):
        """Test that latest is not supported with daily files."""
        with pytest.raises(ValueError, match="Latest not supported with daily files"):
            self.iso.get_as_prices_real_time_5_min(date="latest", use_daily_files=True)

    WEIS_LMP_COLUMNS = [
        "Interval Start",
        "Interval End",
        "Market",
        "Location",
        "Location Type",
        "PNode",
        "LMP",
        "Energy",
        "Congestion",
        "Loss",
    ]

    @pytest.mark.integration
    def test_get_lmp_real_time_weis_latest(self):
        df = self.iso.get_lmp_real_time_weis(date="latest")

        assert len(df) > 0
        assert df.columns.tolist() == self.WEIS_LMP_COLUMNS

    def test_get_lmp_real_time_weis_1_hour_range(self):
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        end = start + pd.Timedelta(minutes=15)

        with api_vcr.use_cassette(
            f"test_get_lmp_real_time_weis_1_hour_range_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_lmp_real_time_weis(
                start=start,
                end=end,
            )

        assert df["Interval Start"].min() == start
        assert df["Interval End"].max() == end
        assert df.columns.tolist() == self.WEIS_LMP_COLUMNS

    def test_get_lmp_real_time_weis_cross_day(self):
        start = pd.Timestamp(
            "2025-11-01 23:45:00",
            tz=self.iso.default_timezone,
        )
        end = start + pd.Timedelta(minutes=25)

        with api_vcr.use_cassette(
            f"test_get_lmp_real_time_weis_cross_day_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_lmp_real_time_weis(
                start=start,
                end=end,
            )

        assert df["Interval Start"].min() == start
        assert df["Interval End"].max() <= end
        assert df.columns.tolist() == self.WEIS_LMP_COLUMNS

    def test_get_lmp_real_time_weis_single_interval(self):
        date = pd.Timestamp(
            "2025-11-01 12:00:00",
            tz=self.iso.default_timezone,
        )
        with api_vcr.use_cassette(
            f"test_get_lmp_real_time_weis_single_interval_{date.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_lmp_real_time_weis(date=date)

        # assert one interval that straddles or starts at date input
        assert df["Interval Start"].min() <= date
        assert df["Interval End"].max() > date
        assert df["Interval Start"].nunique() == 1
        assert df.columns.tolist() == self.WEIS_LMP_COLUMNS

    def test_get_lmp_real_time_weis_last_interval_of_day(self):
        day = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        start = day + pd.Timedelta(hours=23, minutes=55)

        with api_vcr.use_cassette(
            f"test_get_lmp_real_time_weis_last_interval_of_day_{start.strftime('%Y%m%d')}_{(start + pd.Timedelta(minutes=5)).strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_lmp_real_time_weis(
                start=start,
                end=start + pd.Timedelta(minutes=5),
            )

        assert df["Interval Start"].min() == start
        assert df["Interval End"].max() == start + pd.Timedelta(minutes=5)
        assert df.columns.tolist() == self.WEIS_LMP_COLUMNS

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "date,end",
        [
            (
                pd.Timestamp("2025-11-02 00:55:00-05:00"),
                pd.Timestamp("2025-11-02 01:25:00-06:00"),
            ),
        ],
    )
    def test_get_lmp_real_time_weis_dst_end(self, date, end):
        with api_vcr.use_cassette(
            f"test_get_lmp_real_time_weis_dst_end_{date}_{end}.yaml",
        ):
            df = self.iso.get_lmp_real_time_weis(
                date,
                end,
            )

        # Note the missing files between '2024-11-03 01:05:00-05:00' and
        # '2024-11-03 01:10:00-06:00'
        assert all(
            df["Interval End"].unique()
            == pd.to_datetime(
                [
                    "2024-11-03 01:00:00-05:00",
                    "2024-11-03 01:05:00-05:00",
                    "2024-11-03 01:10:00-06:00",
                    "2024-11-03 01:15:00-06:00",
                    "2024-11-03 01:20:00-06:00",
                    "2024-11-03 01:25:00-06:00",
                ],
            ),
        )

    """get_load"""

    @pytest.mark.integration
    def test_get_load_historical(self):
        test_date = pd.Timestamp("2025-11-01").date()
        with api_vcr.use_cassette("test_get_load_historical_2025-11-01.yaml"):
            df = self.iso.get_load(test_date)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "Interval Start" in df.columns
        assert "Interval End" in df.columns
        assert "Load" in df.columns

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_historical_with_date_range(self):
        pass

    def test_get_load_pre_baa_historical(self):
        date = pd.Timestamp("2026-03-30 12:00:00-0500")
        source_df = pd.DataFrame(
            {
                "Interval Start": [date, date + pd.Timedelta(minutes=5)],
                "Interval End": [
                    date + pd.Timedelta(minutes=5),
                    date + pd.Timedelta(minutes=10),
                ],
                "Actual": [20000.0, 20100.0],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_short_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load(date=date, verbose=False)

        assert df.columns.tolist() == ["Interval Start", "Interval End", "Load"]
        assert len(df) == 2
        assert df["Load"].tolist() == [20000.0, 20100.0]

    def test_get_load_post_baa_historical(self):
        date = pd.Timestamp("2026-04-02 12:00:00-0500")
        source_df = pd.DataFrame(
            {
                "Interval Start": [
                    date,
                    date,
                    date + pd.Timedelta(minutes=5),
                    date + pd.Timedelta(minutes=5),
                ],
                "Interval End": [
                    date + pd.Timedelta(minutes=5),
                    date + pd.Timedelta(minutes=5),
                    date + pd.Timedelta(minutes=10),
                    date + pd.Timedelta(minutes=10),
                ],
                "Actual": [15000.0, 5000.0, 15100.0, 5100.0],
                "BAA": ["SPP", "SWPW", "SPP", "SWPW"],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_short_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load(date=date, verbose=False)

        assert df.columns.tolist() == ["Interval Start", "Interval End", "Load"]
        assert len(df) == 2
        assert df["Load"].tolist() == [20000.0, 20200.0]

    def test_get_load_pre_baa_historical_no_baa_column(self):
        date = pd.Timestamp("2026-03-28 08:00:00-0500")
        source_df = pd.DataFrame(
            {
                "Interval Start": [date],
                "Interval End": [date + pd.Timedelta(minutes=5)],
                "Actual": [18000.0],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_short_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load(date=date, verbose=False)

        assert df.columns.tolist() == ["Interval Start", "Interval End", "Load"]
        assert len(df) == 1
        assert df["Load"].iloc[0] == 18000.0

    def test_get_load_pre_baa_historical_null_baa_values(self):
        date = pd.Timestamp("2026-03-29 10:00:00-0500")
        source_df = pd.DataFrame(
            {
                "Interval Start": [date, date + pd.Timedelta(minutes=5)],
                "Interval End": [
                    date + pd.Timedelta(minutes=5),
                    date + pd.Timedelta(minutes=10),
                ],
                "Actual": [19000.0, 19100.0],
                "BAA": [None, None],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_short_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load(date=date, verbose=False)

        assert df.columns.tolist() == ["Interval Start", "Interval End", "Load"]
        assert len(df) == 2

    def test_get_load_null_baa_mixed_load_values(self):
        date = pd.Timestamp("2026-04-02 12:00:00-0500")
        source_df = pd.DataFrame(
            {
                "Interval Start": [date, date],
                "Interval End": [
                    date + pd.Timedelta(minutes=5),
                    date + pd.Timedelta(minutes=5),
                ],
                "Actual": [15000.0, 3500.0],
                "BAA": [None, None],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_short_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load(date=date, verbose=False)

        assert df.columns.tolist() == ["Interval Start", "Interval End", "Load"]
        assert len(df) == 1
        assert df["Load"].iloc[0] == 18500.0

    """get_load_forecast"""

    LOAD_FORECAST_COLS = [
        "Interval Start",
        "Interval End",
        "Publish Time",
        "Load Forecast",
    ]

    # TODO: Refactor Base Tests such that we don't have to skip them everywhere
    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical_with_date_range(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_today(self):
        pass

    def test_get_load_forecast_post_baa(self):
        date = pd.Timestamp("2026-04-02 12:00:00-0500")
        publish_time = date
        source_df = pd.DataFrame(
            {
                "Interval Start": [
                    date,
                    date,
                    date + pd.Timedelta(hours=1),
                    date + pd.Timedelta(hours=1),
                ],
                "Interval End": [
                    date + pd.Timedelta(hours=1),
                    date + pd.Timedelta(hours=1),
                    date + pd.Timedelta(hours=2),
                    date + pd.Timedelta(hours=2),
                ],
                "Publish Time": [publish_time] * 4,
                "MTLF": [15000.0, 4000.0, 15100.0, 4100.0],
                "BAA": ["SPP", "SWPW", "SPP", "SWPW"],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_mid_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load_forecast(date=date, verbose=False)

        assert df.columns.tolist() == self.LOAD_FORECAST_COLS
        assert len(df) == 2
        assert df["Load Forecast"].tolist() == [19000.0, 19200.0]

    def test_get_load_forecast_pre_baa_no_column(self):
        date = pd.Timestamp("2026-03-28 08:00:00-0500")
        publish_time = date
        source_df = pd.DataFrame(
            {
                "Interval Start": [date],
                "Interval End": [date + pd.Timedelta(hours=1)],
                "Publish Time": [publish_time],
                "MTLF": [20000.0],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_mid_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load_forecast(date=date, verbose=False)

        assert df.columns.tolist() == self.LOAD_FORECAST_COLS
        assert len(df) == 1
        assert df["Load Forecast"].iloc[0] == 20000.0

    def test_get_load_forecast_null_baa_mixed_values(self):
        date = pd.Timestamp("2026-04-02 12:00:00-0500")
        publish_time = date
        source_df = pd.DataFrame(
            {
                "Interval Start": [date, date],
                "Interval End": [
                    date + pd.Timedelta(hours=1),
                    date + pd.Timedelta(hours=1),
                ],
                "Publish Time": [publish_time] * 2,
                "MTLF": [14000.0, 3500.0],
                "BAA": [None, None],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_mid_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load_forecast(date=date, verbose=False)

        assert df.columns.tolist() == self.LOAD_FORECAST_COLS
        assert len(df) == 1
        assert df["Load Forecast"].iloc[0] == 17500.0

    """get_load_forecast_by_baa"""

    LOAD_FORECAST_BY_BAA_COLS = [
        "Interval Start",
        "Interval End",
        "Publish Time",
        "BAA",
        "Load Forecast",
    ]

    @pytest.mark.integration
    def test_get_load_forecast_by_baa_post_baa(self):
        now = self.local_now().floor("h")
        publish_time = now
        source_df = pd.DataFrame(
            {
                "Interval Start": [now, now],
                "Interval End": [
                    now + pd.Timedelta(hours=1),
                    now + pd.Timedelta(hours=1),
                ],
                "Publish Time": [publish_time] * 2,
                "MTLF": [15000.0, 4000.0],
                "BAA": ["SPP", "SWPW"],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_mid_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load_forecast_by_baa(date=now, verbose=False)

        assert df.columns.tolist() == self.LOAD_FORECAST_BY_BAA_COLS
        assert set(df["BAA"].unique()) == {"SPP", "SWPW"}
        assert len(df) == 2

    def test_get_load_forecast_by_baa_pre_baa_no_column(self):
        date = pd.Timestamp("2026-03-28 12:00:00-0500")
        publish_time = date
        source_df = pd.DataFrame(
            {
                "Interval Start": [date, date + pd.Timedelta(hours=1)],
                "Interval End": [
                    date + pd.Timedelta(hours=1),
                    date + pd.Timedelta(hours=2),
                ],
                "Publish Time": [publish_time] * 2,
                "MTLF": [20000.0, 20100.0],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_mid_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load_forecast_by_baa(date=date, verbose=False)

        assert df.columns.tolist() == self.LOAD_FORECAST_BY_BAA_COLS
        assert set(df["BAA"].unique()) == {"SPP"}
        assert len(df) == 2

    def test_get_load_forecast_by_baa_null_baa_mixed_values(self):
        date = pd.Timestamp("2026-04-02 12:00:00-0500")
        publish_time = date
        source_df = pd.DataFrame(
            {
                "Interval Start": [date, date],
                "Interval End": [
                    date + pd.Timedelta(hours=1),
                    date + pd.Timedelta(hours=1),
                ],
                "Publish Time": [publish_time] * 2,
                "MTLF": [14000.0, 3500.0],
                "BAA": [None, None],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_mid_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load_forecast_by_baa(date=date, verbose=False)

        assert df.columns.tolist() == self.LOAD_FORECAST_BY_BAA_COLS
        assert set(df["BAA"].unique()) == {"SPP", "SWPW"}
        assert len(df) == 2
        spp_row = df[df["BAA"] == "SPP"]
        swpw_row = df[df["BAA"] == "SWPW"]
        assert spp_row["Load Forecast"].iloc[0] == 14000.0
        assert swpw_row["Load Forecast"].iloc[0] == 3500.0

    def test_get_load_forecast_by_baa_empty_data(self):
        date = pd.Timestamp("2026-03-25 10:00:00-0500")

        with patch.object(
            self.iso,
            "_get_mid_term_forecast_data",
            return_value=None,
        ):
            with pytest.raises(NoDataFoundException):
                self.iso.get_load_forecast_by_baa(date=date, verbose=False)

    """get_load_forecast_short_term"""

    @pytest.mark.integration
    def test_get_load_forecast_short_term_today(self):
        df = self.iso.get_load_forecast_short_term(date="today")

        now = self.iso.now()

        assert df["Publish Time"].min() == now.normalize()
        # Account for small delay in publishing
        assert df["Publish Time"].max() >= (now).floor("5min") - pd.DateOffset(
            minutes=10,
        )

        assert df["Interval Start"].min() <= now
        assert df["Interval Start"].max() >= now.floor("h")

        self._check_load_forecast(df, "SHORT_TERM")

    @pytest.mark.integration
    def test_get_load_forecast_short_term_latest(self):
        latest = self.iso.get_load_forecast_short_term(date="latest")

        # Single publish time
        assert (
            latest["Publish Time"]
            == (self.iso.now() - pd.Timedelta(minutes=2)).floor("5min")
        ).all()

        self._check_load_forecast(latest, "SHORT_TERM")

    def test_get_load_forecast_short_term_historical(self):
        date = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)

        with api_vcr.use_cassette(
            f"test_get_load_forecast_short_term_historical_{date.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_load_forecast_short_term(date=date)

        assert (df["Publish Time"] == date).all()

        # Each file contains data going back into the past
        assert df["Interval Start"].min() <= date
        assert df["Interval Start"].max() >= date - pd.Timedelta(minutes=5)

        self._check_load_forecast(df, "SHORT_TERM")

    def test_get_load_forecast_short_term_hour_24_handling(self):
        # This test checks we can successfully retrieve the 24th hour of the day
        # which has a 00 ((23 + 1) % 24 = 0) for the hour in the file name.
        start = pd.Timestamp(
            "2025-11-01 23:00:00",
            tz=self.iso.default_timezone,
        )

        end = start + pd.Timedelta(hours=1)

        with api_vcr.use_cassette(
            f"test_get_load_forecast_short_term_hour_24_handling_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_load_forecast_short_term(
                date=start,
                end=end,
            )

        assert df["Publish Time"].min() == start
        assert df["Publish Time"].max() == end - pd.Timedelta(minutes=5)

        self._check_load_forecast(df, "SHORT_TERM")

    def test_get_load_forecast_short_term_historical_with_date_range(self):
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        end = start + pd.Timedelta(hours=3, minutes=45)

        with api_vcr.use_cassette(
            f"test_get_load_forecast_short_term_historical_with_date_range_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_load_forecast_short_term(
                start,
                end,
            )

        assert df["Publish Time"].min() == start
        assert df["Publish Time"].max() == end - pd.Timedelta(minutes=5)

        self._check_load_forecast(df, "SHORT_TERM")

    def test_get_load_forecast_short_term_dst_ending(self):
        # Test we can handle DST end with the repeated hour
        start = "2025-11-02 01:00:00-0500"
        end = "2025-11-02 02:00:00-0600"

        with api_vcr.use_cassette(
            f"test_get_load_forecast_short_term_hour_dst_ending_{start}_{end}.yaml",
        ):
            df = self.iso.get_load_forecast_short_term(
                date=start,
                end=end,
            )

        assert df["Publish Time"].min() == pd.Timestamp("2025-11-02 01:00:00-0500")
        assert df["Publish Time"].max() == pd.Timestamp("2025-11-02 01:55:00-0600")
        assert df["Publish Time"].nunique() == 12

        self._check_load_forecast(df, "SHORT_TERM")

    def test_get_load_forecast_short_term_keep_null_forecast_values(self):
        start = "2025-12-02 00:00:00"
        end = "2025-12-02 00:15:00"

        with api_vcr.use_cassette(
            f"test_get_load_forecast_short_term_keep_null_forecast_values_{start}_{end}.yaml",
        ):
            df_dropped = self.iso.get_load_forecast_short_term(
                date=start,
                end=end,
                drop_null_forecast_rows=True,
            )

            df_kept = self.iso.get_load_forecast_short_term(
                date=start,
                end=end,
                drop_null_forecast_rows=False,
            )

        assert len(df_kept) >= len(df_dropped)
        self._check_load_forecast(df_kept, "SHORT_TERM")

    """get_load_forecast_mid_term"""

    @pytest.mark.integration
    def test_get_load_forecast_mid_term_today(self):
        df = self.iso.get_load_forecast_mid_term(date="today")

        now = self.iso.now()

        assert df["Publish Time"].min() == now.normalize()
        assert df["Publish Time"].max() >= (now.floor("h") - pd.DateOffset(hours=1))

        assert df["Interval Start"].min() <= now

        time_in_future = pd.Timedelta(days=5)

        assert df["Interval Start"].max() >= now + time_in_future

        self._check_load_forecast(df, "MID_TERM")

    @pytest.mark.integration
    def test_get_load_forecast_mid_term_latest(self):
        latest = self.iso.get_load_forecast_mid_term(date="latest")

        # Single publish time
        assert (
            latest["Publish Time"]
            == (self.iso.now() - pd.Timedelta(minutes=10)).floor("h")
        ).all()

        self._check_load_forecast(latest, "MID_TERM")

    def test_get_load_forecast_mid_term_historical(self):
        date = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)

        with api_vcr.use_cassette(
            f"test_get_load_forecast_mid_term_historical_{date.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_load_forecast_mid_term(date=date)

        assert (df["Publish Time"].unique() == date).all()

        # Each file contains data going back into the past
        assert df["Interval Start"].min() <= date
        assert df["Interval Start"].max() >= date + pd.Timedelta(days=6)

        self._check_load_forecast(df, "MID_TERM")

    def test_get_load_forecast_mid_term_historical_with_date_range(self):
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        end = start + pd.Timedelta(hours=3, minutes=45)

        with api_vcr.use_cassette(
            f"test_get_load_forecast_mid_term_historical_with_date_range_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_load_forecast_mid_term(
                start,
                end,
            )

        assert df["Publish Time"].min() == start
        assert df["Publish Time"].max() == end - pd.Timedelta(
            minutes=45,
        )

        self._check_load_forecast(df, "MID_TERM")

    def test_get_load_forecast_mid_term_dst_ending(self):
        # Test we can handle DST end with the repeated hour
        start = "2025-11-02 01:00:00-0500"
        end = "2025-11-02 03:00:00-0600"

        with api_vcr.use_cassette(
            f"test_get_load_forecast_mid_term_dst_ending_{start}_{end}.yaml",
        ):
            df = self.iso.get_load_forecast_mid_term(
                date=start,
                end=end,
            )

        assert df["Publish Time"].min() == pd.Timestamp("2025-11-02 01:00:00-0500")
        assert df["Publish Time"].max() == pd.Timestamp("2025-11-02 02:00:00-0600")
        assert df["Publish Time"].nunique() == 2

        self._check_load_forecast(df, "MID_TERM")

    """get_solar_and_wind_forecast_short_term"""

    @pytest.mark.skip(
        reason="SPP BAA column added to outputs - https://www.notion.so/33de835f42aa81f0a147cbf7490f4f85"
    )
    @pytest.mark.integration
    def test_get_solar_and_wind_forecast_short_term_today(self):
        df = self.iso.get_solar_and_wind_forecast_short_term(date="today")

        now = self.iso.now()

        assert df["Publish Time"].min() == now.normalize()
        # Account for small delay in publishing
        assert df["Publish Time"].max() >= (now).floor("5min") - pd.DateOffset(
            minutes=5,
        )

        assert df["Interval Start"].min() <= now

        time_in_future = pd.Timedelta(hours=3)

        assert df["Interval Start"].max() >= now + time_in_future

        self._check_solar_and_wind_forecast(df, "SHORT_TERM")

    @pytest.mark.skip(
        reason="SPP BAA column added to outputs - https://www.notion.so/33de835f42aa81f0a147cbf7490f4f85"
    )
    @pytest.mark.integration
    def test_get_solar_and_wind_forecast_short_term_latest(self):
        latest = self.iso.get_solar_and_wind_forecast_short_term(date="latest")

        # Single publish time
        assert (
            latest["Publish Time"]
            == (self.iso.now() - pd.Timedelta(minutes=2)).floor("5min")
        ).all()

        self._check_solar_and_wind_forecast(latest, "SHORT_TERM")

    def test_get_solar_and_wind_forecast_short_term_historical(self):
        date = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)

        with api_vcr.use_cassette(
            f"test_get_solar_and_wind_forecast_short_term_historical_{date.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_solar_and_wind_forecast_short_term(date=date)

        assert (df["Publish Time"] == date).all()

        # Each file contains data going back into the past
        assert df["Interval Start"].min() <= date
        assert df["Interval Start"].max() >= date + pd.Timedelta(hours=3)

        self._check_solar_and_wind_forecast(df, "SHORT_TERM")

    def test_get_solar_and_wind_forecast_short_term_hour_24_handling(self):
        # This test checks we can successfully retrieve the 24th hour of the day
        # which has a 00 ((23 + 1) % 24 = 0) for the hour in the file name.
        start = pd.Timestamp(
            "2025-11-01 23:00:00",
            tz=self.iso.default_timezone,
        )

        end = start + pd.Timedelta(hours=1)

        with api_vcr.use_cassette(
            f"test_get_solar_and_wind_forecast_short_term_hour_24_handling_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_solar_and_wind_forecast_short_term(
                date=start,
                end=end,
            )

        assert df["Publish Time"].min() == start
        assert df["Publish Time"].max() == end - pd.Timedelta(minutes=5)

        self._check_solar_and_wind_forecast(df, "SHORT_TERM")

    def test_get_solar_and_wind_forecast_short_term_historical_with_date_range(self):
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        end = start + pd.Timedelta(hours=3, minutes=45)

        with api_vcr.use_cassette(
            f"test_get_solar_and_wind_forecast_short_term_historical_with_date_range_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_solar_and_wind_forecast_short_term(
                start,
                end,
            )

        assert df["Publish Time"].min() == start
        assert df["Publish Time"].max() == end - pd.Timedelta(minutes=5)

        self._check_solar_and_wind_forecast(df, "SHORT_TERM")

    def test_get_solar_and_wind_forecast_short_term_dst_ending(self):
        # Test we can handle DST end with the repeated hour
        start = "2025-11-02 01:00:00-0500"
        end = "2025-11-02 02:00:00-0600"

        with api_vcr.use_cassette(
            f"test_get_solar_and_wind_forecast_short_term_dst_ending_{start}_{end}.yaml",
        ):
            df = self.iso.get_solar_and_wind_forecast_short_term(
                date=start,
                end=end,
            )

        assert df["Publish Time"].min() == pd.Timestamp("2025-11-02 01:00:00-0500")
        assert df["Publish Time"].max() == pd.Timestamp("2025-11-02 01:55:00-0600")
        assert df["Publish Time"].nunique() == 12

        self._check_solar_and_wind_forecast(df, "SHORT_TERM")

    """get_solar_and_wind_forecast_mid_term"""

    @pytest.mark.skip(
        reason="SPP BAA column added to outputs - https://www.notion.so/33de835f42aa81f0a147cbf7490f4f85"
    )
    @pytest.mark.integration
    def test_get_solar_and_wind_forecast_mid_term_today(self):
        df = self.iso.get_solar_and_wind_forecast_mid_term(date="today")

        now = self.iso.now()

        assert df["Publish Time"].min() == now.normalize()
        assert df["Publish Time"].max() >= (now.floor("h") - pd.DateOffset(hours=1))

        assert df["Interval Start"].min() <= now

        time_in_future = pd.Timedelta(days=5)

        assert df["Interval Start"].max() >= now + time_in_future

        self._check_solar_and_wind_forecast(df, "MID_TERM")

    @pytest.mark.skip(
        reason="SPP BAA column added to outputs - https://www.notion.so/33de835f42aa81f0a147cbf7490f4f85"
    )
    @pytest.mark.integration
    def test_get_solar_and_wind_forecast_mid_term_latest(self):
        latest = self.iso.get_solar_and_wind_forecast_mid_term(date="latest")

        # Single publish time
        assert (
            latest["Publish Time"]
            == (self.iso.now() - pd.Timedelta(minutes=10)).floor("h")
        ).all()

        self._check_solar_and_wind_forecast(latest, "MID_TERM")

    def test_get_solar_and_wind_forecast_mid_term_historical(self):
        date = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)

        with api_vcr.use_cassette(
            f"test_get_solar_and_wind_forecast_mid_term_historical_{date.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_solar_and_wind_forecast_mid_term(date=date)

        assert (df["Publish Time"].unique() == date).all()

        # Each file contains data going back into the past
        assert df["Interval Start"].min() <= date
        assert df["Interval Start"].max() >= date + pd.Timedelta(days=6)

        self._check_solar_and_wind_forecast(df, "MID_TERM")

    def test_get_solar_and_wind_forecast_mid_term_historical_with_date_range(self):
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        end = start + pd.Timedelta(hours=3, minutes=45)

        with api_vcr.use_cassette(
            f"test_get_solar_and_wind_forecast_mid_term_historical_with_date_range_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_solar_and_wind_forecast_mid_term(
                start,
                end,
            )

        assert df["Publish Time"].min() == start
        assert df["Publish Time"].max() == end - pd.Timedelta(
            minutes=45,
        )

        self._check_solar_and_wind_forecast(df, "MID_TERM")

    def test_get_solar_and_wind_forecast_mid_term_dst_ending(self):
        # Test we can handle DST end with the repeated hour
        # For mid-term hourly forecasts: 100 file has no "d", 200d file has "d"
        # The 200d file represents what would have been 2:00 AM before DST ended
        start = "2025-11-02 01:00:00-0500"
        end = "2025-11-02 03:00:00-0600"

        with api_vcr.use_cassette(
            f"test_get_solar_and_wind_forecast_mid_term_dst_ending_{start}_{end}.yaml",
        ):
            df = self.iso.get_solar_and_wind_forecast_mid_term(
                date=start,
                end=end,
            )

        assert df["Publish Time"].min() == pd.Timestamp("2025-11-02 01:00:00-0500")
        assert df["Publish Time"].max() == pd.Timestamp("2025-11-02 02:00:00-0600")
        assert df["Publish Time"].nunique() == 2

        self._check_solar_and_wind_forecast(df, "MID_TERM")

    """get_load_by_baa"""

    LOAD_BY_BAA_COLS = ["Interval Start", "Interval End", "BAA", "Load"]

    @pytest.mark.integration
    def test_get_load_by_baa_post_baa(self):
        now = self.local_now().floor("5min")
        source_df = pd.DataFrame(
            {
                "Interval Start": [now, now, now],
                "Interval End": [
                    now + pd.Timedelta(minutes=5),
                    now + pd.Timedelta(minutes=5),
                    now + pd.Timedelta(minutes=5),
                ],
                "Actual": [1000.0, 1005.0, 1001.0],
                "BAA": ["SWPW", "SPP", "SWPW"],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_short_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load_by_baa(date=now, verbose=False)

        assert df.columns.tolist() == self.LOAD_BY_BAA_COLS
        assert set(df["BAA"].unique()) == {"SPP", "SWPW"}
        assert len(df) == 2

    def test_get_load_by_baa_pre_baa_no_column(self):
        date = pd.Timestamp("2026-03-28 12:00:00-0500")
        source_df = pd.DataFrame(
            {
                "Interval Start": [date, date + pd.Timedelta(minutes=5)],
                "Interval End": [
                    date + pd.Timedelta(minutes=5),
                    date + pd.Timedelta(minutes=10),
                ],
                "Actual": [20000.0, 20100.0],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_short_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load_by_baa(date=date, verbose=False)

        assert df.columns.tolist() == self.LOAD_BY_BAA_COLS
        assert set(df["BAA"].unique()) == {"SPP"}
        assert len(df) == 2

    def test_get_load_by_baa_pre_baa_null_values(self):
        date = pd.Timestamp("2026-03-29 10:00:00-0500")
        source_df = pd.DataFrame(
            {
                "Interval Start": [date, date + pd.Timedelta(minutes=5)],
                "Interval End": [
                    date + pd.Timedelta(minutes=5),
                    date + pd.Timedelta(minutes=10),
                ],
                "Actual": [19000.0, 19100.0],
                "BAA": [None, None],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_short_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load_by_baa(date=date, verbose=False)

        assert df.columns.tolist() == self.LOAD_BY_BAA_COLS
        assert set(df["BAA"].unique()) == {"SPP"}
        assert len(df) == 2

    def test_get_load_by_baa_null_baa_mixed_load_values(self):
        date = pd.Timestamp("2026-04-02 12:00:00-0500")
        source_df = pd.DataFrame(
            {
                "Interval Start": [date, date],
                "Interval End": [
                    date + pd.Timedelta(minutes=5),
                    date + pd.Timedelta(minutes=5),
                ],
                "Actual": [15000.0, 3500.0],
                "BAA": [None, None],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_short_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load_by_baa(date=date, verbose=False)

        assert df.columns.tolist() == self.LOAD_BY_BAA_COLS
        assert set(df["BAA"].unique()) == {"SPP", "SWPW"}
        assert len(df) == 2
        spp_row = df[df["BAA"] == "SPP"]
        swpw_row = df[df["BAA"] == "SWPW"]
        assert spp_row["Load"].iloc[0] == 15000.0
        assert swpw_row["Load"].iloc[0] == 3500.0

    @pytest.mark.integration
    def test_get_load_by_baa_missing_actual_load_column(self):
        now = self.local_now().floor("5min")
        source_df = pd.DataFrame(
            {
                "Interval Start": [now],
                "Interval End": [now + pd.Timedelta(minutes=5)],
                "Actual Wind MW": [100.0],
                "BAA": ["SWPW"],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_short_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            with pytest.raises(KeyError):
                self.iso.get_load_by_baa(date=now, verbose=False)

    """get_load_by_baa_hourly"""

    @pytest.mark.integration
    def test_get_load_by_baa_hourly_post_baa(self):
        now = self.local_now().floor("h")
        source_df = pd.DataFrame(
            {
                "Interval Start": [now, now],
                "Interval End": [
                    now + pd.Timedelta(hours=1),
                    now + pd.Timedelta(hours=1),
                ],
                "Averaged Actual": [15000.0, 5000.0],
                "BAA": ["SPP", "SWPW"],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_mid_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load_by_baa_hourly(date=now, verbose=False)

        assert df.columns.tolist() == self.LOAD_BY_BAA_COLS
        assert set(df["BAA"].unique()) == {"SPP", "SWPW"}
        assert len(df) == 2

    def test_get_load_by_baa_hourly_pre_baa_no_column(self):
        date = pd.Timestamp("2026-03-28 12:00:00-0500")
        source_df = pd.DataFrame(
            {
                "Interval Start": [date, date + pd.Timedelta(hours=1)],
                "Interval End": [
                    date + pd.Timedelta(hours=1),
                    date + pd.Timedelta(hours=2),
                ],
                "Averaged Actual": [20000.0, 20100.0],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_mid_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load_by_baa_hourly(date=date, verbose=False)

        assert df.columns.tolist() == self.LOAD_BY_BAA_COLS
        assert set(df["BAA"].unique()) == {"SPP"}
        assert len(df) == 2

    def test_get_load_by_baa_hourly_pre_baa_null_values(self):
        date = pd.Timestamp("2026-03-29 10:00:00-0500")
        source_df = pd.DataFrame(
            {
                "Interval Start": [date, date + pd.Timedelta(hours=1)],
                "Interval End": [
                    date + pd.Timedelta(hours=1),
                    date + pd.Timedelta(hours=2),
                ],
                "Averaged Actual": [19000.0, 19100.0],
                "BAA": [None, None],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_mid_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load_by_baa_hourly(date=date, verbose=False)

        assert df.columns.tolist() == self.LOAD_BY_BAA_COLS
        assert set(df["BAA"].unique()) == {"SPP"}
        assert len(df) == 2

    def test_get_load_by_baa_hourly_null_baa_mixed_load_values(self):
        date = pd.Timestamp("2026-04-02 12:00:00-0500")
        source_df = pd.DataFrame(
            {
                "Interval Start": [date, date],
                "Interval End": [
                    date + pd.Timedelta(hours=1),
                    date + pd.Timedelta(hours=1),
                ],
                "Averaged Actual": [14000.0, 4000.0],
                "BAA": [None, None],
            },
        )

        with (
            patch.object(
                self.iso,
                "_get_mid_term_forecast_data",
                return_value=(pd.DataFrame(), "mock-url"),
            ),
            patch.object(
                self.iso,
                "_post_process_load_forecast",
                return_value=source_df,
            ),
        ):
            df = self.iso.get_load_by_baa_hourly(date=date, verbose=False)

        assert df.columns.tolist() == self.LOAD_BY_BAA_COLS
        assert set(df["BAA"].unique()) == {"SPP", "SWPW"}
        assert len(df) == 2
        spp_row = df[df["BAA"] == "SPP"]
        swpw_row = df[df["BAA"] == "SWPW"]
        assert spp_row["Load"].iloc[0] == 14000.0
        assert swpw_row["Load"].iloc[0] == 4000.0

    def test_get_load_by_baa_hourly_no_data(self):
        date = pd.Timestamp("2026-03-25 10:00:00-0500")

        with patch.object(
            self.iso,
            "_get_mid_term_forecast_data",
            return_value=None,
        ):
            df = self.iso.get_load_by_baa_hourly(date=date, verbose=False)

        assert df.columns.tolist() == self.LOAD_BY_BAA_COLS
        assert len(df) == 0

    """get_status"""

    @pytest.mark.integration
    def test_get_status_latest(self):
        with pytest.raises(NotImplementedError):
            super().test_get_status_latest()

    """get_storage"""

    @pytest.mark.integration
    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    @pytest.mark.integration
    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

    """ get_ver_curtailments """

    _ver_curtailment_cols = [
        "Interval Start",
        "Interval End",
        "Wind Redispatch Curtailments",
        "Wind Manual Curtailments",
        "Wind Curtailed For Energy",
        "Solar Redispatch Curtailments",
        "Solar Manual Curtailments",
        "Solar Curtailed For Energy",
    ]

    def _check_ver_curtailments(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == self._ver_curtailment_cols
        assert "BAA" not in df.columns

    def _check_ver_curtailments_by_baa(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == self._ver_curtailment_cols + ["BAA"]

    def test_get_ver_curtailments_historical(self):
        end = pd.Timestamp("2025-11-03")
        start = pd.Timestamp("2025-11-01")
        with api_vcr.use_cassette(
            f"test_get_ver_curtailments_historical_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_ver_curtailments(start=start, end=end)

        assert df["Interval Start"].min().date() == start.date()
        # end date is exclusive; max data is from the day before
        assert df["Interval Start"].max().date() >= (end - pd.Timedelta(days=1)).date()
        self._check_ver_curtailments(df)

    def test_get_ver_curtailments_annual(self):
        year = 2020
        with api_vcr.use_cassette(
            f"test_get_ver_curtailments_annual_{year}.yaml",
        ):
            df = self.iso.get_ver_curtailments_annual(year=year)

        assert df["Interval Start"].min().date() == pd.Timestamp(f"{year}-01-01").date()
        assert df["Interval Start"].max().date() == pd.Timestamp(f"{year}-12-31").date()

        self._check_ver_curtailments(df)

    """ get_ver_curtailments_by_baa """

    def test_get_ver_curtailments_by_baa_historical(self):
        end = pd.Timestamp("2025-11-03")
        start = pd.Timestamp("2025-11-01")
        with api_vcr.use_cassette(
            f"test_get_ver_curtailments_by_baa_historical_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_ver_curtailments_by_baa(start=start, end=end)

        assert df["Interval Start"].min().date() == start.date()
        # end date is exclusive; max data is from the day before
        assert df["Interval Start"].max().date() >= (end - pd.Timedelta(days=1)).date()
        self._check_ver_curtailments_by_baa(df)

    def test_get_ver_curtailments_by_baa_annual(self):
        year = 2020
        with api_vcr.use_cassette(
            f"test_get_ver_curtailments_by_baa_annual_{year}.yaml",
        ):
            df = self.iso.get_ver_curtailments_by_baa_annual(year=year)

        assert df["Interval Start"].min().date() == pd.Timestamp(f"{year}-01-01").date()
        assert df["Interval Start"].max().date() == pd.Timestamp(f"{year}-12-31").date()

        self._check_ver_curtailments_by_baa(df)

    # get_capacity_of_generation_on_outage

    def _check_capacity_of_generation_on_outage(self, df):
        columns = [
            "Publish Time",
            "Interval Start",
            "Interval End",
            "Total Outaged MW",
            "Coal MW",
            "Diesel Fuel Oil MW",
            "Hydro MW",
            "Natural Gas MW",
            "Nuclear MW",
            "Solar MW",
            "Waste Disposal MW",
            "Wind MW",
            "Waste Heat MW",
            "Other MW",
        ]

        assert df.columns.tolist() == columns

    def test_get_capacity_of_generation_on_outage(self):
        end = pd.Timestamp("2025-11-03")
        start = end - pd.Timedelta(days=2)
        with api_vcr.use_cassette(
            f"test_get_capacity_of_generation_on_outage_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_capacity_of_generation_on_outage(
                start=start,
                end=end,
            )

        self._check_capacity_of_generation_on_outage(df)

        # confirm two days of data (end date is exclusive)
        assert df.shape[0] / 168 == 2
        assert df["Publish Time"].dt.date.nunique() == 2

    def test_get_capacity_of_generation_on_outage_annual(self):
        year = 2020
        with api_vcr.use_cassette(
            f"test_get_capacity_of_generation_on_outage_annual_{year}.yaml",
        ):
            df = self.iso.get_capacity_of_generation_on_outage_annual(year=year)

        assert df["Interval Start"].min().date() == pd.Timestamp(f"{year}-01-01").date()

        # 2020 was a leap year
        assert df["Publish Time"].nunique() == 366

        self._check_capacity_of_generation_on_outage(df)

    def _check_solar_and_wind(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Actual Wind MW",
            "Actual Solar MW",
        ]

        assert (df["Actual Wind MW"] >= 0).all()
        assert (df["Actual Solar MW"] >= 0).all()

    def _check_load_forecast(self, df, forecast_type):
        forecast_col = "STLF" if forecast_type == "SHORT_TERM" else "MTLF"
        actual_col = "Actual" if forecast_type == "SHORT_TERM" else "Averaged Actual"

        required_cols = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Forecast Type",
            forecast_col,
            actual_col,
        ]

        interval = (
            pd.Timedelta(minutes=5)
            if forecast_type == "SHORT_TERM"
            else pd.Timedelta(hours=1)
        )

        for col in required_cols:
            assert col in df.columns, f"Missing column: {col}"

        allowed_extra_cols = {"BAA"}
        extra_cols = set(df.columns) - set(required_cols)
        assert extra_cols <= allowed_extra_cols, (
            f"Unexpected columns: {extra_cols - allowed_extra_cols}"
        )

        assert (df[forecast_col] >= 0).all()

        assert (df["Interval End"] - df["Interval Start"] == interval).all()

        assert (df["Forecast Type"] == forecast_type).all()

    def _check_solar_and_wind_forecast(self, df, forecast_type):
        expected_cols = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Forecast Type",
            "Wind Forecast MW",
            "Actual Wind MW",
            "Solar Forecast MW",
            "Actual Solar MW",
        ]

        if forecast_type == "MID_TERM":
            expected_cols.remove("Actual Wind MW")
            expected_cols.remove("Actual Solar MW")

        interval = (
            pd.Timedelta(minutes=5)
            if forecast_type == "SHORT_TERM"
            else pd.Timedelta(hours=1)
        )

        assert df.columns.tolist() == expected_cols

        assert (df["Wind Forecast MW"] >= 0).all()
        assert (df["Solar Forecast MW"] >= 0).all()

        assert (df["Interval End"] - df["Interval Start"] == interval).all()

        assert (df["Forecast Type"] == forecast_type).all()

    """ get_hourly_load_historical (wide format, before 2026-03-24) """

    def _check_hourly_load_historical(self, df):
        assert isinstance(df, pd.DataFrame)

        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "CSWS",
            "EDE",
            "GRDA",
            "INDN",
            "KACY",
            "KCPL",
            "LES",
            "MPS",
            "NPPD",
            "OKGE",
            "OPPD",
            "SECI",
            "SPRM",
            "SPS",
            "WAUE",
            "WFEC",
            "WR",
            "System Total",
        ]

    def test_get_hourly_load_historical(self):
        start = pd.Timestamp("2026-03-20")
        end = pd.Timestamp("2026-03-22")
        with api_vcr.use_cassette(
            f"test_get_hourly_load_historical_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_hourly_load_historical(start=start, end=end)

        assert df["Interval Start"].min().date() == start.date()
        assert df["Interval Start"].max().date() == pd.Timestamp("2026-03-21").date()
        self._check_hourly_load_historical(df)

    def test_get_hourly_load_historical_annual(self):
        year = 2020
        with api_vcr.use_cassette(
            f"test_get_hourly_load_historical_annual_{year}.yaml",
        ):
            df = self.iso.get_hourly_load_annual(year=year)

        assert df["Interval Start"].min().date() == pd.Timestamp(f"{year}-01-01").date()
        assert df["Interval Start"].max().date() == pd.Timestamp(f"{year}-12-31").date()

        self._check_hourly_load_historical(df)

    def test_get_hourly_load_historical_raises_on_new_date(self):
        with pytest.raises(NotSupported):
            self.iso.get_hourly_load_historical(pd.Timestamp("2026-03-24"))

    def test_get_hourly_load_historical_process_raises_on_new_data(self):
        new_format_df = pd.DataFrame(
            {
                "Market Hour": ["03/24/2026 06:00:00"],
                "Balancing Area Name": ["SPP"],
                "Control Zone Name": ["CSWS"],
                "Forecast Area Type": ["CF"],
                "Load MW": [4091.830],
            },
        )
        with pytest.raises(NotSupported):
            self.iso._process_hourly_load(new_format_df)

    """ get_hourly_load (long format, >= 2026-03-24) """

    def _check_hourly_load(self, df):
        assert isinstance(df, pd.DataFrame)

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Balancing Area Name",
            "Control Zone Name",
            "Forecast Area Type",
            "Load",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, US/Central]"
        assert df["Interval End"].dtype == "datetime64[ns, US/Central]"
        assert df["Balancing Area Name"].dtype == "object"
        assert df["Control Zone Name"].dtype == "object"
        assert df["Forecast Area Type"].dtype == "object"
        assert df["Load"].dtype == "float64"
        assert set(df["Forecast Area Type"].unique()).issubset({"CF", "NC"})

    def test_get_hourly_load(self):
        date = pd.Timestamp("2026-03-24")
        with api_vcr.use_cassette(
            f"test_get_hourly_load_{date}.yaml",
        ):
            df = self.iso.get_hourly_load(date)

        self._check_hourly_load(df)
        assert df["Interval Start"].min().date() == date.date()
        assert df["Interval Start"].max().date() == date.date()
        assert len(df) > 0

    def test_get_hourly_load_raises_on_old_date(self):
        with pytest.raises(NoDataFoundException):
            self.iso.get_hourly_load(pd.Timestamp("2026-03-23"))

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "date",
        ["today", "latest", pd.Timestamp.now()],
    )
    def test_get_hourly_load_current_day_not_supported(self, date):
        with pytest.raises(NoDataFoundException):
            self.iso.get_hourly_load(date)

    """get_market_clearing_real_time"""

    def _check_market_clearing_real_time(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Generation",
            "Cleared DR",
            "NSI",
            "SMP",
            "Min LMP",
            "Max LMP",
            "Reg Up",
            "Reg Dn",
            "Ramp Up",
            "Ramp Dn",
            "Unc Up",
            "Spin",
            "Supp",
            "Capacity Available",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()

    @pytest.mark.skip(
        reason="SPP BAA column added to outputs - https://www.notion.so/33de835f42aa81f0a147cbf7490f4f85"
    )
    @pytest.mark.integration
    def test_market_clearing_real_time_latest(self):
        df = self.iso.get_market_clearing_real_time(date="latest")

        self._check_market_clearing_real_time(df)

    def test_market_clearing_real_time_date_range(self):
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        end = start + pd.DateOffset(days=3)

        with api_vcr.use_cassette(
            f"test_market_clearing_real_time_{start.strftime('%Y%m%d')}_to_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_market_clearing_real_time(start=start, end=end)

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(minutes=5)
        self._check_market_clearing_real_time(df)

    """get_market_clearing_day_ahead"""

    def _check_market_clearing_day_ahead(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Generation",
            "Cleared DR",
            "Cleared Demand Bid",
            "Cleared Fixed Demand Bid",
            "Cleared Virtual Bid",
            "Cleared Virtual Offer",
            "Total Demand",
            "NSI",
            "SMP",
            "Min LMP",
            "Max LMP",
            "Reg Up",
            "Reg Dn",
            "Ramp Up",
            "Ramp Dn",
            "Unc Up",
            "Spin",
            "Supp",
            "Capacity Available",
            "Fixed Obligation",
            "Net Capacity",
            "Curtailed Fixed Demand Bid",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=60)
        ).all()

    @pytest.mark.skip(
        reason="SPP BAA column added to outputs - https://www.notion.so/33de835f42aa81f0a147cbf7490f4f85"
    )
    @pytest.mark.integration
    def test_market_clearing_day_ahead_latest(self):
        df = self.iso.get_market_clearing_day_ahead(date="latest")

        self._check_market_clearing_day_ahead(df)

    def test_market_clearing_day_ahead_date_range(self):
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        end = start + pd.DateOffset(days=3)

        with api_vcr.use_cassette(
            f"test_market_clearing_day_ahead_{start.strftime('%Y%m%d')}_to_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_market_clearing_day_ahead(start=start, end=end)

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(hours=1)
        self._check_market_clearing_day_ahead(df)

    """get_binding_constraints_day_ahead"""

    DAY_AHEAD_BINDING_CONSTRAINTS_COLUMNS = [
        "Interval Start",
        "Interval End",
        "Constraint Name",
        "Constraint Type",
        "NERC ID",
        "State",
        "Shadow Price",
        "Monitored Facility",
        "Contingent Facility",
        "Contingency Name",
    ]

    def _check_binding_constraints_day_ahead(self, df: pd.DataFrame):
        assert list(df.columns) == self.DAY_AHEAD_BINDING_CONSTRAINTS_COLUMNS
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

    @pytest.mark.integration
    def test_get_binding_constraints_day_ahead_latest(self):
        df = self.iso.get_binding_constraints_day_ahead_hourly(date="latest")

        self._check_binding_constraints_day_ahead(df)

    def test_get_binding_constraints_day_ahead_historical_date_range(self):
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        end = start + pd.DateOffset(days=1)

        cassette_name = f"test_get_binding_constraints_day_ahead_{start.strftime('%Y%m%d')}_to_{end.strftime('%Y%m%d')}.yaml"

        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_day_ahead_hourly(
                date=start,
                end=end,
            )

        self._check_binding_constraints_day_ahead(df)
        assert df["Interval Start"].min() == start
        # Not end day inclusive - end date is exclusive
        assert df["Interval End"].max() == end

    """get_binding_constraints_real_time_5_min"""

    REAL_TIME_BINDING_CONSTRAINTS_COLUMNS = [
        "Interval Start",
        "Interval End",
        "Constraint Name",
        "Constraint Type",
        "NERC ID",
        "TLR Level",
        "State",
        "Shadow Price",
        "Monitored Facility",
        "Contingent Facility",
    ]

    def _check_binding_constraints_real_time(self, df: pd.DataFrame):
        assert list(df.columns) == self.REAL_TIME_BINDING_CONSTRAINTS_COLUMNS
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()
        # check that NERC ID is integer type and non-negative
        assert pd.api.types.is_integer_dtype(df["NERC ID"]), (
            "NERC ID column must be of integer type"
        )

    @pytest.mark.integration
    def test_get_binding_constraints_real_time_latest(self):
        df = self.iso.get_binding_constraints_real_time_5_min(date="latest")

        self._check_binding_constraints_real_time(df)

    def test_get_binding_constraints_real_time_historical_date_range(self):
        start = pd.Timestamp("2025-11-01", tz=self.iso.default_timezone)
        end = start + pd.DateOffset(hours=2, minutes=15)

        cassette_name = f"test_get_binding_constraints_real_time_{start.strftime('%Y%m%d_%H%M')}_to_{end.strftime('%Y%m%d_%H%M')}.yaml"

        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_real_time_5_min(
                date=start,
                end=end,
            )

        self._check_binding_constraints_real_time(df)
        assert df["Interval Start"].min() == start
        # Daily files return full day, so max is end of day
        assert df["Interval Start"].max() == start + pd.Timedelta(
            hours=23,
            minutes=55,
        )

    @pytest.mark.skip(
        reason="SPP binding_constraints assertion bug - https://www.notion.so/33de835f42aa8115b269f62149f5773c"
    )
    @pytest.mark.integration
    def test_get_binding_constraints_real_time_5_min_range_includes_today(self):
        start_date = self.local_now() - pd.Timedelta(days=1)
        end_date = self.local_now()
        df = self.iso.get_binding_constraints_real_time_5_min(
            date=start_date,
            end=end_date,
        )

        self._check_binding_constraints_real_time(df)
        assert df["Interval Start"].min() == start_date
        assert df["Interval Start"].max() == end_date

    """get_interchange_real_time"""

    interchange_real_time_cols = [
        "Time",
        "Region",
        "Interchange",
    ]

    def _check_interchange_real_time(self, df):
        assert len(df) > 0
        assert df["Time"].dt.tz is not None
        assert list(df.columns) == self.interchange_real_time_cols
        # Core interchange regions are present
        regions = df["Region"].unique()
        assert "SPP NSI" in regions
        assert "SPP NAI" in regions
        # No null interchange values
        assert df["Interchange"].notna().all()

    @pytest.mark.integration
    def test_get_interchange_real_time_latest(self):
        df = self.iso.get_interchange_real_time("latest")

        self._check_interchange_real_time(df)

    @pytest.mark.integration
    def test_get_interchange_real_time_today(self):
        df = self.iso.get_interchange_real_time("today")

        self._check_interchange_real_time(df)

    @pytest.mark.skip(
        reason="Library tz comparison bug in CI (UTC) - https://www.notion.so/345e835f42aa81ec91dbf0a47c52c70c"
    )
    def test_get_interchange_real_time_historical(self):
        with api_vcr.use_cassette(
            "test_get_interchange_real_time_historical.yaml",
        ):
            df = self.iso.get_interchange_real_time(
                pd.Timestamp("2025-01-01", tz=self.iso.default_timezone),
            )

        self._check_interchange_real_time(df)
        # The Jan 2025 file starts Dec 31 UTC-6 due to GMT→Central conversion
        assert df["Time"].min().year >= 2024
        assert df["Time"].max().month == 1
        assert df["Time"].max().year == 2025

    @pytest.mark.skip(
        reason="Library tz comparison bug in CI (UTC) - https://www.notion.so/345e835f42aa81ec91dbf0a47c52c70c"
    )
    def test_get_interchange_real_time_historical_range(self):
        with api_vcr.use_cassette(
            "test_get_interchange_real_time_historical_range.yaml",
        ):
            df = self.iso.get_interchange_real_time(
                date=pd.Timestamp("2025-01-01", tz=self.iso.default_timezone),
                end=pd.Timestamp("2025-03-01", tz=self.iso.default_timezone),
            )

        self._check_interchange_real_time(df)
        # Should span Jan and Feb 2025 (starts Dec 31 due to GMT→Central)
        assert df["Time"].min().year >= 2024
        assert df["Time"].max().month == 2
        assert df["Time"].max().year == 2025

    @pytest.mark.integration
    def test_get_interchange_real_time_no_data(self):
        with api_vcr.use_cassette(
            "test_get_interchange_real_time_no_data.yaml",
        ):
            with pytest.raises(NoDataFoundException):
                self.iso.get_interchange_real_time(
                    pd.Timestamp("2014-02-01"),
                    error="raise",
                )

    """get_west_interchange_real_time"""

    west_interchange_real_time_cols = [
        "Time",
        "Region",
        "Interchange",
    ]

    def _check_west_interchange_real_time(self, df):
        assert len(df) > 0
        assert df["Time"].dt.tz is not None
        assert list(df.columns) == self.west_interchange_real_time_cols
        # Core interchange regions are present
        regions = df["Region"].unique()
        assert "SWPW NSI" in regions
        assert "SWPW NAI" in regions
        # No null interchange values
        assert df["Interchange"].notna().all()

    @pytest.mark.integration
    def test_get_west_interchange_real_time_latest(self):
        df = self.iso.get_west_interchange_real_time("latest")

        self._check_west_interchange_real_time(df)

    @pytest.mark.integration
    def test_get_west_interchange_real_time_today(self):
        df = self.iso.get_west_interchange_real_time("today")

        self._check_west_interchange_real_time(df)

    def test_get_west_interchange_real_time_historical(self):
        with api_vcr.use_cassette(
            "test_get_west_interchange_real_time_historical.yaml",
        ):
            df = self.iso.get_west_interchange_real_time(
                pd.Timestamp("2026-04-01"),
            )

        self._check_west_interchange_real_time(df)
        assert df["Time"].min().month >= 3
        assert df["Time"].max().month == 4
        assert df["Time"].max().year == 2026

    def test_get_west_interchange_real_time_historical_range(self):
        with api_vcr.use_cassette(
            "test_get_west_interchange_real_time_historical_range.yaml",
        ):
            df = self.iso.get_west_interchange_real_time(
                date=pd.Timestamp("2026-03-01"),
                end=pd.Timestamp("2026-05-01"),
            )

        self._check_west_interchange_real_time(df)
        # Should span at least March 2026
        assert df["Time"].min().month >= 3
        assert df["Time"].max().month >= 3
        assert df["Time"].max().year == 2026

    @pytest.mark.integration
    def test_get_west_interchange_real_time_no_data(self):
        with api_vcr.use_cassette(
            "test_get_west_interchange_real_time_no_data.yaml",
        ):
            with pytest.raises(NoDataFoundException):
                self.iso.get_west_interchange_real_time(
                    pd.Timestamp("2025-02-01"),
                    error="raise",
                )


class TestFillBaaColumn:
    """Tests for the fill_baa_column utility function."""

    def test_creates_baa_column_when_missing(self):
        df = pd.DataFrame({"Load": [2000.0, 25000.0, 3000.0]})
        result = fill_baa_column(df, "Load")
        assert "BAA" in result.columns
        assert result["BAA"].tolist() == [
            BAAEnum.SWPW.value,
            BAAEnum.SPP.value,
            BAAEnum.SWPW.value,
        ]

    def test_fills_nan_baa_values(self):
        df = pd.DataFrame(
            {
                "Load": [2000.0, 25000.0, 3000.0],
                "BAA": [BAAEnum.SWPW.value, None, None],
            },
        )
        result = fill_baa_column(df, "Load")
        assert result["BAA"].tolist() == [
            BAAEnum.SWPW.value,
            BAAEnum.SPP.value,
            BAAEnum.SWPW.value,
        ]

    def test_preserves_existing_baa_values(self):
        df = pd.DataFrame(
            {
                "Load": [2000.0, 25000.0],
                "BAA": [BAAEnum.SPP.value, BAAEnum.SWPW.value],
            },
        )
        result = fill_baa_column(df, "Load")
        # Existing non-null values should not be overwritten
        assert result["BAA"].tolist() == [
            BAAEnum.SPP.value,
            BAAEnum.SWPW.value,
        ]

    def test_nan_load_maps_to_spp(self):
        df = pd.DataFrame({"Load": [float("nan"), 2000.0]})
        result = fill_baa_column(df, "Load")
        assert result["BAA"].tolist() == [
            BAAEnum.SPP.value,
            BAAEnum.SWPW.value,
        ]

    def test_threshold_boundary(self):
        df = pd.DataFrame(
            {"Load": [BAA_LOAD_THRESHOLD_MW - 1, BAA_LOAD_THRESHOLD_MW]},
        )
        result = fill_baa_column(df, "Load")
        assert result["BAA"].tolist() == [
            BAAEnum.SWPW.value,
            BAAEnum.SPP.value,
        ]

    def test_returns_same_dataframe(self):
        df = pd.DataFrame({"Load": [2000.0]})
        result = fill_baa_column(df, "Load")
        assert result is df

    def test_all_baa_present_no_changes(self):
        df = pd.DataFrame(
            {
                "Load": [2000.0, 25000.0],
                "BAA": [BAAEnum.SPP.value, BAAEnum.SPP.value],
            },
        )
        result = fill_baa_column(df, "Load")
        assert result["BAA"].tolist() == [BAAEnum.SPP.value, BAAEnum.SPP.value]
