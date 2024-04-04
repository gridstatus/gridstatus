import pandas as pd
import pytest

from gridstatus import MISO, NotSupported
from gridstatus.base import Markets
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets


class TestMISO(BaseTestISO):
    iso = MISO()

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

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_range_two_days_with_day_start_endpoint(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_start_end_same_day(self):
        pass

    def test_get_fuel_mix_today(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_today()

    """get_lmp"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_lmp_date_range(self, markets=None):
        pass

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
    )
    def test_get_lmp_historical(self, market):
        super().test_get_lmp_historical(market)

    @with_markets(
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_latest(self, market):
        super().test_get_lmp_latest(market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_today(self, market):
        super().test_get_lmp_today(market=market)

    def test_get_lmp_locations(self):
        data = self.iso.get_lmp(
            date="latest",
            market=Markets.REAL_TIME_5_MIN,
            locations=self.iso.hubs,
        )
        assert set(data["Location"].unique()) == set(self.iso.hubs)

    """get_load"""

    def test_get_load_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_load_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_historical_with_date_range(self):
        pass

    """get_load_forecast"""

    load_forecast_cols = [
        "Interval Start",
        "Interval End",
        "Publish Time",
        "LRZ1 MTLF (MWh)",
        "LRZ2_7 MTLF (MWh)",
        "LRZ3_5 MTLF (MWh)",
        "LRZ4 MTLF (MWh)",
        "LRZ6 MTLF (MWh)",
        "LRZ8_9_10 MTLF (MWh)",
        "MISO MTLF (MWh)",
    ]

    def test_get_load_forecast_today_or_latest(self):
        df = self.iso.get_load_forecast("today")

        assert df.columns.tolist() == self.load_forecast_cols

        assert (df["Publish Time"] == self.local_start_of_today()).all()
        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.Timedelta(
            days=5,
        )

        assert self.iso.get_load_forecast("latest").equals(df)

    def test_get_load_forecast_historical(self):
        past_date = self.local_today() - pd.Timedelta(days=30)
        df = self.iso.get_load_forecast(past_date)

        assert df.columns.tolist() == self.load_forecast_cols

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            past_date,
        ) + pd.Timedelta(days=5)

        assert df["Publish Time"].dt.date.unique() == pd.to_datetime(past_date).date()

    def test_get_load_forecast_historical_with_date_range(self):
        past_date = self.local_today() - pd.Timedelta(days=250)
        end_date = past_date + pd.Timedelta(days=3)

        df = self.iso.get_load_forecast(
            start=past_date,
            end=end_date,
        )

        assert df.columns.tolist() == self.load_forecast_cols
        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            end_date,
        ) + pd.Timedelta(days=4)

    def test_get_load_forecast_dst_start_and_end(self):
        dst_start = pd.Timestamp("2022-03-13")
        df = self.iso.get_load_forecast(dst_start)

        assert df.columns.tolist() == self.load_forecast_cols
        assert df["Interval Start"].min() == self.local_start_of_day(dst_start)

        dst_end = pd.Timestamp("2022-11-06")
        df = self.iso.get_load_forecast(dst_end)

        assert df.columns.tolist() == self.load_forecast_cols
        assert df["Interval Start"].min() == self.local_start_of_day(dst_end)

    solar_and_wind_forecast_cols = [
        "Interval Start",
        "Interval End",
        "Publish Time",
        "North",
        "Central",
        "South",
        "MISO",
    ]

    """get_solar_forecast"""

    def _check_solar_and_wind_forecast(self, df):
        assert df.columns.tolist() == self.solar_and_wind_forecast_cols
        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            "1h",
        )

    def test_get_solar_forecast_historical(self):
        past_date = self.local_today() - pd.Timedelta(days=30)
        df = self.iso.get_solar_forecast(past_date)

        self._check_solar_and_wind_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            past_date,
        ) + pd.Timedelta(days=7)

        assert df["Publish Time"].dt.date.unique() == pd.to_datetime(past_date).date()

    def test_get_solar_forecast_historical_date_range(self):
        past_date = self.local_today() - pd.Timedelta(days=250)
        end_date = past_date + pd.Timedelta(days=3)

        df = self.iso.get_solar_forecast(
            start=past_date,
            end=end_date,
        )

        self._check_solar_and_wind_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            end_date,
        ) + pd.Timedelta(days=6)

        assert df["Publish Time"].dt.date.unique().tolist() == [
            past_date,
            past_date + pd.Timedelta(days=1),
            past_date + pd.Timedelta(days=2),
        ]

    def test_get_solar_forecast_historical_before_schema_change(self):
        # Data schema changed on 2022-06-13
        date = pd.Timestamp("2022-05-12").date()

        df = self.iso.get_solar_forecast(date)

        self._check_solar_and_wind_forecast(df)

    """get_wind_forecast"""

    def test_get_wind_forecast_historical(self):
        past_date = self.local_today() - pd.Timedelta(days=30)
        df = self.iso.get_wind_forecast(past_date)

        self._check_solar_and_wind_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            past_date,
        ) + pd.Timedelta(days=7)

        assert df["Publish Time"].dt.date.unique() == pd.to_datetime(past_date).date()

    def test_get_wind_forecast_historical_date_range(self):
        past_date = self.local_today() - pd.Timedelta(days=250)
        end_date = past_date + pd.Timedelta(days=3)

        df = self.iso.get_wind_forecast(
            start=past_date,
            end=end_date,
        )

        self._check_solar_and_wind_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            end_date,
        ) + pd.Timedelta(days=6)

        assert df["Publish Time"].dt.date.unique().tolist() == [
            past_date,
            past_date + pd.Timedelta(days=1),
            past_date + pd.Timedelta(days=2),
        ]

    def test_get_wind_forecast_historical_before_schema_change(self):
        # Data schema changed on 2022-06-13
        # No south data for 2022-05-12 for wind
        date = pd.Timestamp("2022-05-12").date()

        df = self.iso.get_wind_forecast(date)

        self._check_solar_and_wind_forecast(df)

        assert df["South"].isnull().all()

    """get_status"""

    def test_get_status_latest(self):
        with pytest.raises(NotImplementedError):
            super().test_get_status_latest()

    """get_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()
