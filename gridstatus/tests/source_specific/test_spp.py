import numpy as np
import pandas as pd
import pytest

from gridstatus import SPP, Markets, NotSupported
from gridstatus.spp import (
    LOCATION_TYPE_BUS,
    LOCATION_TYPE_HUB,
    LOCATION_TYPE_INTERFACE,
    LOCATION_TYPE_SETTLEMENT_LOCATION,
)
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

api_vcr = setup_vcr(
    source="spp",
    record_mode=RECORD_MODE,
)


class TestSPP(BaseTestISO):
    iso = SPP()

    def now(self):
        return pd.Timestamp.now(tz=self.iso.default_timezone)

    """get_fuel_mix"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_date_or_start(self):
        pass

    @pytest.mark.integration
    def test_get_fuel_mix_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_historical_with_date_range(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_range_two_days_with_day_start_endpoint(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_start_end_same_day(self):
        pass

    @pytest.mark.integration
    def test_get_fuel_mix_central_time(self):
        fm = self.iso.get_fuel_mix(date="latest")
        assert fm.Time.iloc[0].tz.zone == self.iso.default_timezone

    @pytest.mark.integration
    def test_get_fuel_mix_self_market(self):
        fm = self.iso.get_fuel_mix(date="latest", detailed=True)

        cols = [
            "Time",
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

        assert fm.columns.tolist() == cols

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

    @pytest.mark.integration
    def test_get_lmp_real_time_5_min_by_location_latest(self):
        df = self.iso.get_lmp_real_time_5_min_by_location(date="latest")

        self._check_lmp_real_time_5_min_by_location(df)

        # Latest data should have one interval

        assert df["Interval Start"].nunique() == 1
        assert df["Interval Start"].max() >= (self.now() - pd.DateOffset(minutes=10))

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_lmp_real_time_5_min_by_location_today(self):
        df = self.iso.get_lmp_real_time_5_min_by_location(date="today", verbose=True)

        self._check_lmp_real_time_5_min_by_location(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() >= self.local_now().floor(
            "5min",
        ) - pd.DateOffset(minutes=10)

    @pytest.mark.integration
    def test_get_lmp_real_time_5_min_by_location_date_range(self):
        three_days_ago = self.local_start_of_today() - pd.DateOffset(days=3)
        three_days_ago_0215 = three_days_ago + pd.DateOffset(hours=2, minutes=15)

        df = self.iso.get_lmp_real_time_5_min_by_location(
            date=three_days_ago,
            end=three_days_ago_0215,
            verbose=True,
        )

        self._check_lmp_real_time_5_min_by_location(df)

        assert df["Interval Start"].min() == three_days_ago
        assert df["Interval End"].max() == three_days_ago_0215

    @pytest.mark.integration
    def test_get_lmp_real_time_5_min_by_location_historical_date(self):
        # For a historical date, the decorator only retrieves a single interval
        thirty_days_ago = self.local_start_of_today() - pd.DateOffset(days=30)

        df = self.iso.get_lmp_real_time_5_min_by_location(
            date=thirty_days_ago,
            verbose=True,
        )

        self._check_lmp_real_time_5_min_by_location(df)

        assert df["Interval Start"].min() == thirty_days_ago
        assert df["Interval End"].max() == thirty_days_ago + pd.DateOffset(minutes=5)

    @pytest.mark.integration
    def test_get_lmp_real_time_5_min_by_location_last_interval_of_day(self):
        two_days_ago = self.local_start_of_today() - pd.DateOffset(days=2)
        two_days_ago_2355 = two_days_ago + pd.DateOffset(hours=23, minutes=55)

        df = self.iso.get_lmp_real_time_5_min_by_location(
            (two_days_ago_2355, two_days_ago_2355 + pd.DateOffset(minutes=5)),
        )

        self._check_lmp_real_time_5_min_by_location(df)

        assert df["Interval Start"].min() == two_days_ago_2355
        assert df["Interval End"].max() == two_days_ago_2355 + pd.DateOffset(minutes=5)

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

    @pytest.mark.integration
    def test_get_lmp_real_time_5_min_by_bus_latest(self):
        df = self.iso.get_lmp_real_time_5_min_by_bus(date="latest")

        self._check_lmp_real_time_5_min_by_bus(df)

        # Latest data should have one interval
        assert df["Interval Start"].nunique() == 1
        assert df["Interval Start"].max() >= (self.now() - pd.DateOffset(minutes=10))

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_lmp_real_time_5_min_by_bus_today(self):
        df = self.iso.get_lmp_real_time_5_min_by_bus(date="today", verbose=True)

        self._check_lmp_real_time_5_min_by_bus(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() >= self.local_now().floor(
            "5min",
        ) - pd.DateOffset(minutes=10)

    @pytest.mark.integration
    def test_get_lmp_real_time_5_min_by_bus_date_range(self):
        three_days_ago = self.local_start_of_today() - pd.DateOffset(days=3)
        three_days_ago_0215 = three_days_ago + pd.DateOffset(hours=2, minutes=15)

        df = self.iso.get_lmp_real_time_5_min_by_bus(
            date=three_days_ago,
            end=three_days_ago_0215,
        )

        self._check_lmp_real_time_5_min_by_bus(df)

        assert df["Interval Start"].min() == three_days_ago
        assert df["Interval End"].max() == three_days_ago_0215

    @pytest.mark.integration
    def test_get_lmp_real_time_5_min_by_bus_historical_date(self):
        # For a historical date, the decorator only retrieves a single interval
        thirty_days_ago = self.local_start_of_today() - pd.DateOffset(days=30)

        df = self.iso.get_lmp_real_time_5_min_by_bus(date=thirty_days_ago)

        self._check_lmp_real_time_5_min_by_bus(df)

        assert df["Interval Start"].min() == thirty_days_ago
        assert df["Interval End"].max() == thirty_days_ago + pd.DateOffset(minutes=5)

    @pytest.mark.integration
    def test_get_lmp_real_time_5_min_by_bus_last_interval_of_day(self):
        two_days_ago = self.local_start_of_today() - pd.DateOffset(days=2)
        two_days_ago_2355 = two_days_ago + pd.DateOffset(hours=23, minutes=55)

        df = self.iso.get_lmp_real_time_5_min_by_bus(
            (two_days_ago_2355, two_days_ago_2355 + pd.DateOffset(minutes=5)),
        )

        self._check_lmp_real_time_5_min_by_bus(df)

        assert df["Interval Start"].min() == two_days_ago_2355
        assert df["Interval End"].max() == two_days_ago_2355 + pd.DateOffset(minutes=5)

    @pytest.mark.integration
    def test_get_lmp_real_time_5_min_by_bus_dst_end(self):
        df = self.iso.get_lmp_real_time_5_min_by_bus(
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

    @pytest.mark.integration
    def test_get_lmp_day_ahead_hourly_date_range(self):
        four_days_ago = self.local_start_of_today() - pd.DateOffset(days=4)
        two_days_ago = four_days_ago + pd.DateOffset(days=2)

        df = self.iso.get_lmp_day_ahead_hourly(
            start=four_days_ago,
            end=two_days_ago,
        )

        self._check_lmp_day_ahead_hourly(df)

        assert df["Interval Start"].min() == four_days_ago
        # Not end day inclusive
        assert df["Interval End"].max() == two_days_ago

    @pytest.mark.integration
    def test_get_lmp_day_ahead_hourly_historical_date(self):
        thirty_days_ago = self.local_start_of_today() - pd.DateOffset(days=30)

        df = self.iso.get_lmp_day_ahead_hourly(date=thirty_days_ago)

        self._check_lmp_day_ahead_hourly(df)

        assert df["Interval Start"].min() == thirty_days_ago
        assert df["Interval End"].max() == thirty_days_ago + pd.DateOffset(days=1)

    @pytest.mark.parametrize(
        "location_type",
        [
            LOCATION_TYPE_HUB,
            LOCATION_TYPE_INTERFACE,
            LOCATION_TYPE_SETTLEMENT_LOCATION,
        ],
    )
    @pytest.mark.integration
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

    @pytest.mark.integration
    def test_get_operating_reserves(self):
        yesterday = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(
            days=1,
        )  # noqa
        yesterday_1230am = yesterday + pd.Timedelta(minutes=30)

        df = self.iso.get_operating_reserves(start=yesterday, end=yesterday_1230am)
        assert len(df) > 0
        assert df.columns.tolist() == self.OPERATING_RESERVES_COLUMNS

    @pytest.mark.integration
    def test_get_operating_reserves_latest(self):
        df = self.iso.get_operating_reserves(date="latest")
        assert len(df) > 0
        assert df.columns.tolist() == self.OPERATING_RESERVES_COLUMNS

    @pytest.mark.integration
    def test_get_operative_reserves_last_interval_of_day(self):
        two_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(
            days=2,
        )
        two_days_ago_2355 = two_days_ago + pd.Timedelta(hours=23, minutes=55)

        df = self.iso.get_operating_reserves(
            start=two_days_ago_2355,
            end=two_days_ago_2355 + pd.Timedelta(minutes=5),
        )

        assert df["Interval Start"].min() == two_days_ago_2355
        assert df["Interval End"].max() == two_days_ago_2355 + pd.Timedelta(minutes=5)
        assert df.columns.tolist() == self.OPERATING_RESERVES_COLUMNS

    @pytest.mark.integration
    def test_get_operating_reserves_dst_end(self):
        df = self.iso.get_operating_reserves(
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

    @pytest.mark.integration
    def test_get_day_ahead_operating_reserve_prices(self):
        tomorrow = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() + pd.Timedelta(
            days=1,
        )
        three_days_ago = tomorrow - pd.Timedelta(days=4)

        df = self.iso.get_day_ahead_operating_reserve_prices(
            date=three_days_ago,
            end=tomorrow,
        )

        assert df["Interval Start"].min() == three_days_ago
        assert df["Interval End"].max() == tomorrow
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

    @pytest.mark.integration
    def test_get_lmp_real_time_weis_1_hour_range(self):
        three_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(
            days=3,
        )  # noqa
        three_days_ago_0015 = three_days_ago + pd.Timedelta(minutes=15)

        df = self.iso.get_lmp_real_time_weis(
            start=three_days_ago,
            end=three_days_ago_0015,
        )

        assert df["Interval Start"].min() == three_days_ago
        assert df["Interval End"].max() == three_days_ago_0015
        assert df.columns.tolist() == self.WEIS_LMP_COLUMNS

    @pytest.mark.integration
    def test_get_lmp_real_time_weis_cross_day(self):
        two_days_ago_2345 = (
            pd.Timestamp.now(tz=self.iso.default_timezone).normalize()
            - pd.Timedelta(days=2)
            + pd.Timedelta(hours=23, minutes=45)
        )  # noqa
        one_day_ago_0010 = two_days_ago_2345 + pd.Timedelta(minutes=25)

        df = self.iso.get_lmp_real_time_weis(
            start=two_days_ago_2345,
            end=one_day_ago_0010,
        )

        assert df["Interval Start"].min() == two_days_ago_2345
        assert df["Interval End"].max() == one_day_ago_0010
        assert df.columns.tolist() == self.WEIS_LMP_COLUMNS

    @pytest.mark.integration
    def test_get_lmp_real_time_weis_single_interval(self):
        three_weeks_ago = pd.Timestamp.now(tz=self.iso.default_timezone) - pd.Timedelta(
            days=21,
        )  # noqa
        df = self.iso.get_lmp_real_time_weis(date=three_weeks_ago)

        # assert one interval that straddles date input
        assert df["Interval Start"].min() < three_weeks_ago
        assert df["Interval End"].max() > three_weeks_ago
        assert df["Interval Start"].nunique() == 1
        assert df.columns.tolist() == self.WEIS_LMP_COLUMNS

    @pytest.mark.integration
    def test_get_lmp_real_time_weis_last_interval_of_day(self):
        two_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(
            days=2,
        )
        two_days_ago_2355 = two_days_ago + pd.Timedelta(hours=23, minutes=55)

        df = self.iso.get_lmp_real_time_weis(
            start=two_days_ago_2355,
            end=two_days_ago_2355 + pd.Timedelta(minutes=5),
        )

        assert df["Interval Start"].min() == two_days_ago_2355
        assert df["Interval End"].max() == two_days_ago_2355 + pd.Timedelta(minutes=5)
        assert df.columns.tolist() == self.WEIS_LMP_COLUMNS

    @pytest.mark.integration
    def test_get_lmp_real_time_weis_dst_end(self):
        df = self.iso.get_lmp_real_time_weis(
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

    """get_load"""

    @pytest.mark.integration
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

    @pytest.mark.integration
    def test_get_load_forecast_short_term_historical(self):
        now = self.iso.now()

        three_days_ago = now.normalize() - pd.Timedelta(days=3)

        df = self.iso.get_load_forecast_short_term(date=three_days_ago)

        assert (df["Publish Time"] == three_days_ago).all()

        # Each file contains data going back into the past
        assert df["Interval Start"].min() <= three_days_ago
        assert df["Interval Start"].max() >= three_days_ago - pd.Timedelta(minutes=5)

        self._check_load_forecast(df, "SHORT_TERM")

    @pytest.mark.integration
    def test_get_load_forecast_short_term_hour_24_handling(self):
        # This test checks we can successfully retrieve the 24th hour of the day
        # which has a 00 ((23 + 1) % 24 = 0) for the hour in the file name.
        two_days_ago_2300 = (
            pd.Timestamp.now(tz=self.iso.default_timezone).normalize()
            - pd.Timedelta(days=2)
            + pd.Timedelta(hours=23, minutes=0)
        )

        one_day_ago_0000 = two_days_ago_2300 + pd.Timedelta(hours=1)

        df = self.iso.get_load_forecast_short_term(
            date=two_days_ago_2300,
            end=one_day_ago_0000,
        )

        assert df["Publish Time"].min() == two_days_ago_2300
        assert df["Publish Time"].max() == one_day_ago_0000 - pd.Timedelta(minutes=5)

        self._check_load_forecast(df, "SHORT_TERM")

    @pytest.mark.integration
    def test_get_load_forecast_short_term_historical_with_date_range(self):
        now = self.iso.now()
        three_days_ago = now.normalize() - pd.Timedelta(days=3)
        three_days_ago_0345 = three_days_ago + pd.Timedelta(hours=3, minutes=45)

        df = self.iso.get_load_forecast_short_term(
            three_days_ago,
            three_days_ago_0345,
        )

        assert df["Publish Time"].min() == three_days_ago
        assert df["Publish Time"].max() == three_days_ago_0345 - pd.Timedelta(minutes=5)

        self._check_load_forecast(df, "SHORT_TERM")

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

    @pytest.mark.integration
    def test_get_load_forecast_mid_term_historical(self):
        now = self.iso.now()

        three_days_ago = now.normalize() - pd.Timedelta(days=3)

        df = self.iso.get_load_forecast_mid_term(date=three_days_ago)

        assert (df["Publish Time"].unique() == three_days_ago).all()

        # Each file contains data going back into the past
        assert df["Interval Start"].min() <= three_days_ago
        assert df["Interval Start"].max() >= three_days_ago + pd.Timedelta(days=6)

        self._check_load_forecast(df, "MID_TERM")

    @pytest.mark.integration
    def test_get_load_forecast_mid_term_historical_with_date_range(self):
        now = self.iso.now()
        three_days_ago = now.normalize() - pd.Timedelta(days=3)
        three_days_ago_0345 = three_days_ago + pd.Timedelta(hours=3, minutes=45)

        df = self.iso.get_load_forecast_mid_term(
            three_days_ago,
            three_days_ago_0345,
        )

        assert df["Publish Time"].min() == three_days_ago
        assert df["Publish Time"].max() == three_days_ago_0345 - pd.Timedelta(
            minutes=45,
        )

        self._check_load_forecast(df, "MID_TERM")

    """get_solar_and_wind_forecast_short_term"""

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

    @pytest.mark.integration
    def test_get_solar_and_wind_forecast_short_term_latest(self):
        latest = self.iso.get_solar_and_wind_forecast_short_term(date="latest")

        # Single publish time
        assert (
            latest["Publish Time"]
            == (self.iso.now() - pd.Timedelta(minutes=2)).floor("5min")
        ).all()

        self._check_solar_and_wind_forecast(latest, "SHORT_TERM")

    @pytest.mark.integration
    def test_get_solar_and_wind_forecast_short_term_historical(self):
        now = self.iso.now()

        three_days_ago = now.normalize() - pd.Timedelta(days=3)

        df = self.iso.get_solar_and_wind_forecast_short_term(date=three_days_ago)

        assert (df["Publish Time"] == three_days_ago).all()

        # Each file contains data going back into the past
        assert df["Interval Start"].min() <= three_days_ago
        assert df["Interval Start"].max() >= three_days_ago + pd.Timedelta(hours=3)

        self._check_solar_and_wind_forecast(df, "SHORT_TERM")

    @pytest.mark.integration
    def test_get_solar_and_wind_forecast_short_term_hour_24_handling(self):
        # This test checks we can successfully retrieve the 24th hour of the day
        # which has a 00 ((23 + 1) % 24 = 0) for the hour in the file name.
        two_days_ago_2300 = (
            pd.Timestamp.now(tz=self.iso.default_timezone).normalize()
            - pd.Timedelta(days=2)
            + pd.Timedelta(hours=23, minutes=0)
        )

        one_day_ago_0000 = two_days_ago_2300 + pd.Timedelta(hours=1)

        df = self.iso.get_solar_and_wind_forecast_short_term(
            date=two_days_ago_2300,
            end=one_day_ago_0000,
        )

        assert df["Publish Time"].min() == two_days_ago_2300
        assert df["Publish Time"].max() == one_day_ago_0000 - pd.Timedelta(minutes=5)

        self._check_solar_and_wind_forecast(df, "SHORT_TERM")

    @pytest.mark.integration
    def test_get_solar_and_wind_forecast_short_term_historical_with_date_range(self):
        now = self.iso.now()
        three_days_ago = now.normalize() - pd.Timedelta(days=3)
        three_days_ago_0345 = three_days_ago + pd.Timedelta(hours=3, minutes=45)

        df = self.iso.get_solar_and_wind_forecast_short_term(
            three_days_ago,
            three_days_ago_0345,
        )

        assert df["Publish Time"].min() == three_days_ago
        assert df["Publish Time"].max() == three_days_ago_0345 - pd.Timedelta(minutes=5)

        self._check_solar_and_wind_forecast(df, "SHORT_TERM")

    """get_solar_and_wind_forecast_mid_term"""

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

    @pytest.mark.integration
    def test_get_solar_and_wind_forecast_mid_term_latest(self):
        latest = self.iso.get_solar_and_wind_forecast_mid_term(date="latest")

        # Single publish time
        assert (
            latest["Publish Time"]
            == (self.iso.now() - pd.Timedelta(minutes=10)).floor("h")
        ).all()

        self._check_solar_and_wind_forecast(latest, "MID_TERM")

    @pytest.mark.integration
    def test_get_solar_and_wind_forecast_mid_term_historical(self):
        now = self.iso.now()

        three_days_ago = now.normalize() - pd.Timedelta(days=3)

        df = self.iso.get_solar_and_wind_forecast_mid_term(date=three_days_ago)

        assert (df["Publish Time"].unique() == three_days_ago).all()

        # Each file contains data going back into the past
        assert df["Interval Start"].min() <= three_days_ago
        assert df["Interval Start"].max() >= three_days_ago + pd.Timedelta(days=6)

        self._check_solar_and_wind_forecast(df, "MID_TERM")

    @pytest.mark.integration
    def test_get_solar_and_wind_forecast_mid_term_historical_with_date_range(self):
        now = self.iso.now()
        three_days_ago = now.normalize() - pd.Timedelta(days=3)
        three_days_ago_0345 = three_days_ago + pd.Timedelta(hours=3, minutes=45)

        df = self.iso.get_solar_and_wind_forecast_mid_term(
            three_days_ago,
            three_days_ago_0345,
        )

        assert df["Publish Time"].min() == three_days_ago
        assert df["Publish Time"].max() == three_days_ago_0345 - pd.Timedelta(
            minutes=45,
        )

        self._check_solar_and_wind_forecast(df, "MID_TERM")

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

    """ get_ver_curtailment """

    def _check_ver_curtailments(self, df):
        assert isinstance(df, pd.DataFrame)

        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "Wind Redispatch Curtailments",
            "Wind Manual Curtailments",
            "Wind Curtailed For Energy",
            "Solar Redispatch Curtailments",
            "Solar Manual Curtailments",
            "Solar Curtailed For Energy",
        ]

    @pytest.mark.integration
    def test_get_ver_curtailments_historical(self):
        two_days_ago = pd.Timestamp.now() - pd.Timedelta(days=2)
        start = two_days_ago - pd.Timedelta(days=2)
        df = self.iso.get_ver_curtailments(start=start, end=two_days_ago)

        assert df["Interval Start"].min().date() == start.date()
        assert df["Interval Start"].max().date() == two_days_ago.date()
        self._check_ver_curtailments(df)

    @pytest.mark.integration
    def test_get_ver_curtailments_annual(self):
        year = 2020
        df = self.iso.get_ver_curtailments_annual(year=year)

        assert df["Interval Start"].min().date() == pd.Timestamp(f"{year}-01-01").date()
        assert df["Interval Start"].max().date() == pd.Timestamp(f"{year}-12-31").date()

        self._check_ver_curtailments(df)

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

    @pytest.mark.integration
    def test_get_capacity_of_generation_on_outage(self):
        two_days_ago = pd.Timestamp.now() - pd.Timedelta(days=2)
        start = two_days_ago - pd.Timedelta(days=2)
        df = self.iso.get_capacity_of_generation_on_outage(
            start=start,
            end=two_days_ago,
        )

        self._check_capacity_of_generation_on_outage(df)

        # confirm three weeks of data
        assert df.shape[0] / 168 == 3
        assert df["Publish Time"].dt.date.nunique() == 3

    @pytest.mark.integration
    def test_get_capacity_of_generation_on_outage_annual(self):
        year = 2020
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

        expected_cols = [
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

        assert df.columns.tolist() == expected_cols

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

    """ get_hourly_load """

    def _check_hourly_load(self, df):
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

    @pytest.mark.integration
    def test_get_hourly_load_historical(self):
        two_days_ago = pd.Timestamp.now() - pd.Timedelta(days=2)
        start = two_days_ago - pd.Timedelta(days=2)
        df = self.iso.get_hourly_load(start=start, end=two_days_ago)

        assert df["Interval Start"].min().date() == start.date()
        assert df["Interval Start"].max().date() == two_days_ago.date()
        self._check_hourly_load(df)

    @pytest.mark.integration
    def test_get_hourly_load_annual(self):
        year = 2020
        df = self.iso.get_hourly_load_annual(year=year)

        assert df["Interval Start"].min().date() == pd.Timestamp(f"{year}-01-01").date()
        assert df["Interval Start"].max().date() == pd.Timestamp(f"{year}-12-31").date()

        self._check_hourly_load(df)

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "date",
        ["today", "latest", pd.Timestamp.now()],
    )
    def test_get_hourly_load_current_day_not_supported(self, date):
        with pytest.raises(NotSupported):
            self.iso.get_hourly_load(date)
