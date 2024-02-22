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

        assert df["Interval Start"].max() == pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() + pd.Timedelta(days=7, hours=9)

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
        # Not inclusive of the end date
        assert df["Interval Start"].max() == two_days_ago - pd.Timedelta(
            days=1,
        ) + pd.Timedelta(days=7, hours=9)

        self._check_solar_or_wind_forecast(df, resource_type="Wind")

    def test_get_wind_forecast_historical_single_date(self):
        four_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(days=4)

        df = self.iso.get_wind_forecast(date=four_days_ago, verbose=VERBOSE)

        assert df["Publish Time"].unique() == four_days_ago + pd.Timedelta(hours=10)
        assert df["Interval Start"].min() == four_days_ago + pd.Timedelta(hours=10)
        assert df["Interval Start"].max() == four_days_ago + pd.Timedelta(
            days=7,
            hours=9,
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

        assert df["Interval Start"].max() == pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() + pd.Timedelta(days=7, hours=9)

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
        # Not inclusive of the end date
        assert df["Interval Start"].max() == two_days_ago - pd.Timedelta(
            days=1,
        ) + pd.Timedelta(days=7, hours=9)

        self._check_solar_or_wind_forecast(df, resource_type="Solar")

    def test_get_solar_forecast_historical_single_date(self):
        four_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(days=4)

        df = self.iso.get_solar_forecast(date=four_days_ago, verbose=VERBOSE)

        assert df["Publish Time"].unique() == four_days_ago + pd.Timedelta(hours=10)
        assert df["Interval Start"].min() == four_days_ago + pd.Timedelta(hours=10)
        assert df["Interval Start"].max() == four_days_ago + pd.Timedelta(
            days=7,
            hours=9,
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
        self._check_time_columns(df, "interval")
