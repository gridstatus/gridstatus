from unittest.mock import patch

import pandas as pd
import pytest

from gridstatus import ISONE
from gridstatus.base import Markets
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets

# toggle for debugging
VERBOSE = False

DST_BOUNDARIES = [
    "Mar 13, 2022",
    "Nov 6, 2022",
]

# This is the minimum length of a wind or solar forecast. In DST, it seems to be
# one hour longer.
WIND_OR_SOLAR_FORECAST_LENGTH = pd.Timedelta(days=6, hours=22)


class TestISONE(BaseTestISO):
    iso = ISONE()

    """get_fuel_mix"""

    def test_get_fuel_mix_nov_7_2022(self):
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
        df = self.iso.get_fuel_mix(date=date)
        self._check_fuel_mix(df)

    @pytest.mark.parametrize("date", DST_BOUNDARIES)
    def test_get_fuel_mix(self, date):
        self.iso.get_fuel_mix(date=date, verbose=VERBOSE)

    """get_btm_solar"""

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
        df = self.iso.get_btm_solar(
            date=("April 12, 2023", "April 14, 2023"),
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
        super().test_lmp_date_range(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_HOURLY,
    )
    def test_get_lmp_historical(self, market):
        super().test_get_lmp_historical(market=market)

    @with_markets(
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_HOURLY,
    )
    def test_get_lmp_latest(self, market):
        super().test_get_lmp_latest(market=market)

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
        self.iso.get_lmp(
            date=date,
            market=market,
            verbose=VERBOSE,
        )

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
        self.iso.get_load(date=date, verbose=VERBOSE)

    """get_load_forecast"""

    @pytest.mark.parametrize("date", DST_BOUNDARIES)
    def test_get_load_forecast(self, date):
        self.iso.get_load_forecast(date=date, verbose=VERBOSE)

    """get_wind_forecast"""

    def test_get_wind_forecast_today(self):
        df = self.iso.get_wind_forecast(date="today", verbose=VERBOSE)

        assert df["Publish Time"].unique() == pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() + pd.Timedelta(hours=10)

        assert df["Interval Start"].min() == pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() + pd.Timedelta(hours=10)

        assert (
            df["Interval Start"].max() - df["Interval Start"].min()
            >= WIND_OR_SOLAR_FORECAST_LENGTH
        )

        self._check_solar_or_wind_forecast(df, resource_type="Wind")

    def test_get_wind_forecast_latest(self):
        assert self.iso.get_wind_forecast(date="latest", verbose=VERBOSE).equals(
            self.iso.get_wind_forecast(date="today", verbose=VERBOSE),
        )

    def test_get_wind_forecast_historical_date_range(self):
        two_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(days=2)
        five_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(days=5)

        df = self.iso.get_wind_forecast(
            date=(five_days_ago, two_days_ago),
            verbose=VERBOSE,
        )

        assert (
            df["Publish Time"].unique()
            == [
                five_days_ago + pd.Timedelta(hours=10),
                five_days_ago + pd.Timedelta(days=1, hours=10),
                # Wind forecast is not inclusive of the end date
                five_days_ago + pd.Timedelta(days=2, hours=10),
            ]
        ).all()

        assert df["Interval Start"].min() == five_days_ago + pd.Timedelta(hours=10)

        assert df["Interval Start"].max() - df[
            "Interval Start"
        ].min() >= WIND_OR_SOLAR_FORECAST_LENGTH + pd.Timedelta(days=2)

        self._check_solar_or_wind_forecast(df, resource_type="Wind")

    def test_get_wind_forecast_historical_single_date(self):
        four_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(days=4)

        df = self.iso.get_wind_forecast(date=four_days_ago, verbose=VERBOSE)

        assert df["Publish Time"].unique() == four_days_ago + pd.Timedelta(hours=10)
        assert df["Interval Start"].min() == four_days_ago + pd.Timedelta(hours=10)
        assert (
            df["Interval Start"].max() - df["Interval Start"].min()
            >= WIND_OR_SOLAR_FORECAST_LENGTH
        )

        self._check_solar_or_wind_forecast(df, resource_type="Wind")

    """get_solar_forecast"""

    def test_get_solar_forecast_today(self):
        df = self.iso.get_solar_forecast(date="today", verbose=VERBOSE)

        assert df["Publish Time"].unique() == pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() + pd.Timedelta(hours=10)

        assert df["Interval Start"].min() == pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() + pd.Timedelta(hours=10)

        assert (
            df["Interval Start"].max() - df["Interval Start"].min()
            >= WIND_OR_SOLAR_FORECAST_LENGTH
        )

        self._check_solar_or_wind_forecast(df, resource_type="Solar")

    def test_get_solar_forecast_latest(self):
        assert self.iso.get_solar_forecast(date="latest", verbose=VERBOSE).equals(
            self.iso.get_solar_forecast(date="today", verbose=VERBOSE),
        )

    def test_get_solar_forecast_historical_date_range(self):
        two_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(days=2)
        five_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(days=5)

        df = self.iso.get_solar_forecast(
            date=(five_days_ago, two_days_ago),
            verbose=VERBOSE,
        )

        assert (
            df["Publish Time"].unique()
            == [
                five_days_ago + pd.Timedelta(hours=10),
                five_days_ago + pd.Timedelta(days=1, hours=10),
                # Solar forecast is not inclusive of the end date
                five_days_ago + pd.Timedelta(days=2, hours=10),
            ]
        ).all()

        assert df["Interval Start"].min() == five_days_ago + pd.Timedelta(hours=10)

        assert df["Interval Start"].max() - df[
            "Interval Start"
        ].min() >= WIND_OR_SOLAR_FORECAST_LENGTH + pd.Timedelta(days=2)

        self._check_solar_or_wind_forecast(df, resource_type="Solar")

    def test_get_solar_forecast_historical_single_date(self):
        four_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(days=4)

        df = self.iso.get_solar_forecast(date=four_days_ago, verbose=VERBOSE)

        assert df["Publish Time"].unique() == four_days_ago + pd.Timedelta(hours=10)
        assert df["Interval Start"].min() == four_days_ago + pd.Timedelta(hours=10)

        assert (
            df["Interval Start"].max() - df["Interval Start"].min()
            >= WIND_OR_SOLAR_FORECAST_LENGTH
        )

        self._check_solar_or_wind_forecast(df, resource_type="Solar")

    """get_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

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
