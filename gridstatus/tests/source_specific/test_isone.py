from unittest.mock import patch

import pandas as pd
import pytest

from gridstatus import ISONE
from gridstatus.base import Markets
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

api_vcr = setup_vcr(
    source="isone",
    record_mode=RECORD_MODE,
)

# toggle for debugging
VERBOSE = False

# NOTE(kladar): Enumming the DST boundaries for all past and future DST transitions
# would be a good idea.
DST_BOUNDARIES = [
    "Mar 13, 2022",
    "Nov 6, 2022",
]

# This is the minimum length of a wind or solar forecast. In DST, it seems to be
# one hour longer.
WIND_OR_SOLAR_FORECAST_LENGTH = pd.Timedelta(days=6, hours=22)


class TestISONE(BaseTestISO):
    iso = ISONE()

    # -- Base class tests using today/latest/relative dates → mark integration --

    @pytest.mark.integration
    def test_get_fuel_mix_date_or_start(self):
        super().test_get_fuel_mix_date_or_start()

    def test_get_fuel_mix_historical(self):
        with api_vcr.use_cassette("test_get_fuel_mix_historical.yaml"):
            super().test_get_fuel_mix_historical()

    @pytest.mark.integration
    def test_get_fuel_mix_historical_with_date_range(self):
        super().test_get_fuel_mix_historical_with_date_range()

    @pytest.mark.integration
    def test_get_fuel_mix_range_two_days_with_day_start_endpoint(self):
        super().test_get_fuel_mix_range_two_days_with_day_start_endpoint()

    @pytest.mark.integration
    def test_get_fuel_mix_start_end_same_day(self):
        super().test_get_fuel_mix_start_end_same_day()

    @pytest.mark.integration
    def test_get_fuel_mix_latest(self):
        super().test_get_fuel_mix_latest()

    @pytest.mark.integration
    def test_get_fuel_mix_today(self):
        super().test_get_fuel_mix_today()

    @pytest.mark.integration
    def test_get_load_latest(self):
        super().test_get_load_latest()

    @pytest.mark.integration
    def test_get_load_today(self):
        super().test_get_load_today()

    @pytest.mark.integration
    def test_get_load_forecast_historical(self):
        super().test_get_load_forecast_historical()

    @pytest.mark.integration
    def test_get_load_forecast_historical_with_date_range(self):
        super().test_get_load_forecast_historical_with_date_range()

    @pytest.mark.integration
    def test_get_load_forecast_today(self):
        super().test_get_load_forecast_today()

    @pytest.mark.integration
    def test_get_status_latest(self):
        super().test_get_status_latest()

    """get_fuel_mix"""

    def test_get_fuel_mix_nov_7_2022(self):
        with api_vcr.use_cassette("test_get_fuel_mix_nov_7_2022.yaml"):
            data = self.iso.get_fuel_mix(date="Nov 7, 2022")
            # make sure no nan values are returned
            # nov 7 is a known data where nan values are returned
            assert not data.isna().any().any()

    def test_fuel_mix_across_dst_transition(self):
        # these dates are across the DST transition
        # and caused a bug in the past
        date = (
            pd.Timestamp("2023-11-05 06:50:00+0000", tz="UTC"),
            pd.Timestamp("2023-11-05 21:34:46.206808+0000", tz="UTC"),
        )
        with api_vcr.use_cassette("test_fuel_mix_across_dst_transition.yaml"):
            df = self.iso.get_fuel_mix(date=date)
            self._check_fuel_mix(df)

    @pytest.mark.parametrize("date", DST_BOUNDARIES)
    def test_get_fuel_mix(self, date):
        cassette_name = f"test_get_fuel_mix_{date}.yaml"
        with api_vcr.use_cassette(cassette_name):
            self.iso.get_fuel_mix(date=date, verbose=VERBOSE)

    """get_btm_solar"""

    @pytest.mark.integration
    def test_get_btm_solar(self):
        df = self.iso.get_btm_solar(date="today", verbose=VERBOSE)

        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "BTM Solar",
        ]
        self._check_time_columns(df, "interval")

    def test_get_btm_solar_range(self):
        with api_vcr.use_cassette("test_get_btm_solar_range.yaml"):
            df = self.iso.get_btm_solar(
                date=("Oct 15, 2025", "Oct 17, 2025"),
                verbose=VERBOSE,
            )

            assert df.shape[0] == df.drop_duplicates().shape[0]

            assert df.columns.tolist() == [
                "Time",
                "Interval Start",
                "Interval End",
                "BTM Solar",
            ]
            self._check_time_columns(df, "interval")

    """get_lmp"""

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
    )
    def test_lmp_date_range(self, market):
        start = pd.Timestamp("2025-10-12", tz=self.iso.default_timezone)
        end = pd.Timestamp("2025-10-15", tz=self.iso.default_timezone)
        cassette_name = f"test_lmp_date_range_{market.value.lower()}.yaml"
        with api_vcr.use_cassette(cassette_name):
            df_1 = self.iso.get_lmp(start=start, end=end, market=market)
            df_2 = self.iso.get_lmp(date=(start, end), market=market)
            self._check_lmp_columns(df_1, market)
            assert df_1.equals(df_2)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_HOURLY,
    )
    def test_get_lmp_historical(self, market):
        cassette_name = f"test_get_lmp_historical_{market.value.lower()}.yaml"
        with api_vcr.use_cassette(cassette_name):
            super().test_get_lmp_historical(market=market)

    @pytest.mark.integration
    @with_markets(
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_HOURLY,
    )
    def test_get_lmp_latest(self, market):
        super().test_get_lmp_latest(market=market)

    @pytest.mark.integration
    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_today(self, market):
        super().test_get_lmp_today(market=market)

    @pytest.mark.parametrize("date", DST_BOUNDARIES)
    @pytest.mark.parametrize(
        "market",
        [
            Markets.REAL_TIME_5_MIN,
            Markets.REAL_TIME_HOURLY,
            Markets.DAY_AHEAD_HOURLY,
        ],
    )
    def test_get_lmp_dst_boundaries(self, date, market):
        cassette_name = (
            f"test_get_lmp_dst_boundaries_{date}_{market.value.lower()}.yaml"
        )
        with api_vcr.use_cassette(cassette_name):
            self.iso.get_lmp(
                date=date,
                market=market,
                verbose=VERBOSE,
            )

    @pytest.mark.integration
    def test_get_lmp_real_time_no_intervals_gets_current_data(self):
        date = self.local_now() - pd.DateOffset(hours=2)
        end = date + pd.DateOffset(hours=1)

        # Mock the method _select_intervals_for_data_request in the class to return []
        with patch.object(
            self.iso,
            "_select_intervals_for_data_request",
            return_value=[],
        ):
            df = self.iso.get_lmp(
                date=(date, end),
                market=Markets.REAL_TIME_5_MIN,
                verbose=VERBOSE,
            )

        # Rolling data goes back 4 hours and should go up to the current time or close
        assert df["Interval Start"].min() < self.local_now() - pd.DateOffset(hours=3)
        assert df["Interval Start"].max() > self.local_now() - pd.DateOffset(minutes=15)

    """get_load"""

    @pytest.mark.parametrize("date", DST_BOUNDARIES)
    def test_get_load(self, date):
        cassette_name = f"test_get_load_{date}.yaml"
        with api_vcr.use_cassette(cassette_name):
            self.iso.get_load(date=date, verbose=VERBOSE)

    """get_load_forecast"""

    @pytest.mark.parametrize("date", DST_BOUNDARIES)
    def test_get_load_forecast(self, date):
        cassette_name = f"test_get_load_forecast_{date}.yaml"
        with api_vcr.use_cassette(cassette_name):
            self.iso.get_load_forecast(date=date, verbose=VERBOSE)

    """get_wind_forecast"""

    @pytest.mark.integration
    def test_get_wind_forecast_today(self):
        df = self.iso.get_wind_forecast(date="today", verbose=VERBOSE)
        now = pd.Timestamp.now(tz=self.iso.default_timezone).normalize()

        forecast_length = df["Interval Start"].max() - df["Interval Start"].min()
        assert forecast_length >= WIND_OR_SOLAR_FORECAST_LENGTH

        if now.date() == pd.Timestamp("2024-11-03").date():
            # TODO(kladar): We should break this out into a fixture and test it correctly, IMO,
            # since currently only testable on the actual day of the DST transition.
            pass
        else:
            assert df["Publish Time"].unique() == now + pd.Timedelta(hours=10)
            assert df["Interval Start"].min() == now + pd.Timedelta(hours=10)

        self._check_solar_or_wind_forecast(df, resource_type="Wind")

    @pytest.mark.integration
    def test_get_wind_forecast_latest(self):
        assert self.iso.get_wind_forecast(date="latest", verbose=VERBOSE).equals(
            self.iso.get_wind_forecast(date="today", verbose=VERBOSE),
        )

    def test_get_wind_forecast_historical_date_range(self):
        start = pd.Timestamp(
            "2025-10-15",
            tz=self.iso.default_timezone,
        )
        end = pd.Timestamp(
            "2025-10-18",
            tz=self.iso.default_timezone,
        )

        with api_vcr.use_cassette("test_get_wind_forecast_historical_date_range.yaml"):
            df = self.iso.get_wind_forecast(
                date=(start, end),
                verbose=VERBOSE,
            )

        assert (
            df["Publish Time"].unique()
            == [
                start + pd.Timedelta(hours=10),
                start + pd.Timedelta(days=1, hours=10),
                # Wind forecast is not inclusive of the end date
                start + pd.Timedelta(days=2, hours=10),
            ]
        ).all()

        assert df["Interval Start"].min() == start + pd.Timedelta(hours=10)

        assert df["Interval Start"].max() - df[
            "Interval Start"
        ].min() >= WIND_OR_SOLAR_FORECAST_LENGTH + pd.Timedelta(days=2)

        self._check_solar_or_wind_forecast(df, resource_type="Wind")

    def test_get_wind_forecast_historical_single_date(self):
        date = pd.Timestamp(
            "2025-10-15",
            tz=self.iso.default_timezone,
        )

        with api_vcr.use_cassette("test_get_wind_forecast_historical_single_date.yaml"):
            df = self.iso.get_wind_forecast(date=date, verbose=VERBOSE)

        assert df["Publish Time"].unique() == date + pd.Timedelta(hours=10)
        assert df["Interval Start"].min() == date + pd.Timedelta(hours=10)
        assert (
            df["Interval Start"].max() - df["Interval Start"].min()
            >= WIND_OR_SOLAR_FORECAST_LENGTH
        )

        self._check_solar_or_wind_forecast(df, resource_type="Wind")

    """get_solar_forecast"""

    @pytest.mark.integration
    def test_get_solar_forecast_today(self):
        df = self.iso.get_solar_forecast(date="today", verbose=VERBOSE)
        now = pd.Timestamp.now(tz=self.iso.default_timezone).normalize()

        forecast_length = df["Interval Start"].max() - df["Interval Start"].min()
        assert forecast_length >= WIND_OR_SOLAR_FORECAST_LENGTH

        if now.date() == pd.Timestamp("2024-11-03").date():
            # TODO(kladar): We should break this out into a fixture and test it correctly, IMO,
            # since currently only testable on the actual day of the DST transition.
            pass
        else:
            assert df["Publish Time"].unique() == now + pd.Timedelta(hours=10)
            assert df["Interval Start"].min() == now + pd.Timedelta(hours=10)

        self._check_solar_or_wind_forecast(df, resource_type="Solar")

    @pytest.mark.integration
    def test_get_solar_forecast_latest(self):
        assert self.iso.get_solar_forecast(date="latest", verbose=VERBOSE).equals(
            self.iso.get_solar_forecast(date="today", verbose=VERBOSE),
        )

    def test_get_solar_forecast_historical_date_range(self):
        start = pd.Timestamp(
            "2025-10-15",
            tz=self.iso.default_timezone,
        )
        end = pd.Timestamp(
            "2025-10-18",
            tz=self.iso.default_timezone,
        )

        with api_vcr.use_cassette("test_get_solar_forecast_historical_date_range.yaml"):
            df = self.iso.get_solar_forecast(
                date=(start, end),
                verbose=VERBOSE,
            )

        assert (
            df["Publish Time"].unique()
            == [
                start + pd.Timedelta(hours=10),
                start + pd.Timedelta(days=1, hours=10),
                # Solar forecast is not inclusive of the end date
                start + pd.Timedelta(days=2, hours=10),
            ]
        ).all()

        assert df["Interval Start"].min() == start + pd.Timedelta(hours=10)

        assert df["Interval Start"].max() - df[
            "Interval Start"
        ].min() >= WIND_OR_SOLAR_FORECAST_LENGTH + pd.Timedelta(days=2)

        self._check_solar_or_wind_forecast(df, resource_type="Solar")

    def test_get_solar_forecast_historical_single_date(self):
        date = pd.Timestamp(
            "2025-10-15",
            tz=self.iso.default_timezone,
        )

        with api_vcr.use_cassette(
            "test_get_solar_forecast_historical_single_date.yaml"
        ):
            df = self.iso.get_solar_forecast(date=date, verbose=VERBOSE)

        assert df["Publish Time"].unique() == date + pd.Timedelta(hours=10)
        assert df["Interval Start"].min() == date + pd.Timedelta(hours=10)

        assert (
            df["Interval Start"].max() - df["Interval Start"].min()
            >= WIND_OR_SOLAR_FORECAST_LENGTH
        )

        self._check_solar_or_wind_forecast(df, resource_type="Solar")

    """get_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    @pytest.mark.integration
    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

    def _check_solar_or_wind_forecast(self, df, resource_type):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            f"{resource_type} Forecast",
        ]
        self._check_time_columns(df, "interval", skip_column_named_time=True)

        # Due to a little thing called "night" solar forecast should go to zero
        # at some point in the day
        if resource_type == "Solar":
            assert df[f"{resource_type} Forecast"].min() == 0
        else:
            assert df[f"{resource_type} Forecast"].min() >= 0

    @pytest.mark.skip("File is no longer accessible")
    def test_get_interconnection_queue(self):
        pass

    """utils"""

    def test_select_intervals_for_data_request(self):
        mock_now = pd.Timestamp("2024-01-01 21:00:00").tz_localize(
            self.iso.default_timezone,
        )

        with patch.object(ISONE, "local_now", return_value=mock_now):
            start = pd.Timestamp("2024-01-01 03:00:00").tz_localize(
                self.iso.default_timezone,
            )
            end = None

            assert self.iso._select_intervals_for_data_request(
                start,
                end,
                self.iso.lmp_real_time_intervals,
            ) == ["00-04", "04-08", "08-12", "12-16", "16-20"]

            start = pd.Timestamp("2024-01-01 07:00:00").tz_localize(
                self.iso.default_timezone,
            )
            end = pd.Timestamp("2024-01-01 20:00:00").tz_localize(
                self.iso.default_timezone,
            )

            assert self.iso._select_intervals_for_data_request(
                start,
                end,
                self.iso.lmp_real_time_intervals,
            ) == ["04-08", "08-12", "12-16", "16-20"]

            end = pd.Timestamp("2024-01-01 14:00:00").tz_localize(
                self.iso.default_timezone,
            )

            assert self.iso._select_intervals_for_data_request(
                start,
                end,
                self.iso.lmp_real_time_intervals,
            ) == ["04-08", "08-12"]

            start = pd.Timestamp("2024-01-01 22:00:00").tz_localize(
                self.iso.default_timezone,
            )
            end = pd.Timestamp("2024-01-01 23:00:00").tz_localize(
                self.iso.default_timezone,
            )

            assert (
                self.iso._select_intervals_for_data_request(
                    start,
                    end,
                    self.iso.lmp_real_time_intervals,
                )
                == []
            )

    """get_reserve_zone_prices_designations_real_time_5_min_final"""

    def _check_get_reserve_zone_prices_designations_real_time_5_min_final(self, df):
        """Helper method with common checks for reserve zone data"""
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Reserve Zone ID",
            "Reserve Zone Name",
            "Ten Min Spin Requirement",
            "Ten Min Requirement",
            "Total Requirement",
            "TMSR Designated MW",
            "TMNSR Designated MW",
            "TMOR Designated MW",
            "TMSR Clearing Price",
            "TMR Clearing Price",
            "Total Reserve Clearing Price",
        ]

        # Check that we have 5-minute intervals
        self._check_time_columns(df, "interval", skip_column_named_time=True)

        # Should have multiple reserve zones
        assert len(df["Reserve Zone ID"].unique()) > 1

        # Check data types
        # Reserve Zone ID can be int64 or object after concatenation
        assert df["Reserve Zone ID"].dtype in ["int64", "object"]
        assert df["Reserve Zone Name"].dtype == "object"
        for col in [
            "Ten Min Spin Requirement",
            "Ten Min Requirement",
            "Total Requirement",
            "TMSR Designated MW",
            "TMNSR Designated MW",
            "TMOR Designated MW",
            "TMSR Clearing Price",
            "TMR Clearing Price",
            "Total Reserve Clearing Price",
        ]:
            assert df[col].dtype == "float64"

        # Check intervals are 5 minutes
        intervals = (df["Interval End"] - df["Interval Start"]).unique()
        assert len(intervals) == 1
        assert intervals[0] == pd.Timedelta(minutes=5)

    def test_get_reserve_zone_prices_designations_real_time_5_min_final_date_range(
        self,
    ):
        # Test date range - decorator calls function once per day and concatenates results
        # So we should get data for all days in the range (inclusive of end date)
        start = pd.Timestamp(
            "2025-10-15",
            tz=self.iso.default_timezone,
        )
        end = start + pd.Timedelta(days=1)

        cassette_name = f"test_get_reserve_zone_prices_designations_real_time_5_min_final_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}"

        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_reserve_zone_prices_designations_real_time_5_min_final(
                date=(start, end),
                verbose=VERBOSE,
            )

            self._check_get_reserve_zone_prices_designations_real_time_5_min_final(df)

        # Check exact start and end times for the date range
        # Decorator processes each day separately, then concatenates
        # So we expect data from three_days_ago through end of two_days_ago
        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end + pd.Timedelta(hours=23, minutes=55)

    @pytest.mark.parametrize("date", DST_BOUNDARIES)
    def test_get_reserve_zone_prices_designations_real_time_5_min_final_dst_boundary(
        self,
        date,
    ):
        cassette_name = f"test_get_reserve_zone_prices_designations_real_time_5_min_final_dst_boundary_{date}"

        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_reserve_zone_prices_designations_real_time_5_min_final(
                date=date,
                verbose=VERBOSE,
            )

            self._check_get_reserve_zone_prices_designations_real_time_5_min_final(df)

        assert df["Interval Start"].min() == pd.Timestamp(date).tz_localize(
            self.iso.default_timezone,
        )
        assert df["Interval Start"].max() == pd.Timestamp(date).tz_localize(
            self.iso.default_timezone,
            # Use DateOffset to account for DST switch
        ) + pd.DateOffset(days=1, minutes=-5)

    @pytest.mark.integration
    def test_get_reserve_zone_prices_designations_real_time_5_min_final_latest(self):
        # Test the "latest" option
        cassette_name = (
            "test_get_reserve_zone_prices_designations_real_time_5_min_final_latest"
        )

        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_reserve_zone_prices_designations_real_time_5_min_final(
                date="latest",
                verbose=VERBOSE,
            )

            self._check_get_reserve_zone_prices_designations_real_time_5_min_final(df)
