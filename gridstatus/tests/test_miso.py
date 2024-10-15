import pandas as pd
import pytest

from gridstatus import MISO, NotSupported
from gridstatus.base import Markets, NoDataFoundException
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

    """get_lmp_weekly"""

    def _check_lmp_weekly(self, df):
        assert df.columns.tolist() == [
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

        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            "5min",
        )

        assert df["Market"].unique().tolist() == [Markets.REAL_TIME_5_MIN_WEEKLY.value]

    def test_get_lmp_weekly_today_or_latest_raises(self):
        with pytest.raises(NotSupported):
            self.iso.get_lmp_weekly("today")

    def test_get_lmp_weekly_historical_date(self):
        date = self.local_today() - pd.Timedelta(days=300)
        df = self.iso.get_lmp_weekly(date)

        most_recent_monday = self.local_start_of_day(date) - pd.DateOffset(
            days=self.local_start_of_day(date).weekday(),
        )

        assert df["Interval Start"].min() == most_recent_monday
        assert df["Interval End"].max() == most_recent_monday + pd.Timedelta(days=7)

        self._check_lmp_weekly(df)

    def test_get_lmp_weekly_historical_date_range(self):
        start = self.local_today() - pd.Timedelta(days=100)
        # Make sure to span a week
        end = start + pd.Timedelta(days=12)
        df = self.iso.get_lmp_weekly(start, end)

        most_recent_monday = self.local_start_of_day(start) - pd.DateOffset(
            days=self.local_start_of_day(start).weekday(),
        )

        assert df["Interval Start"].min() == most_recent_monday
        assert df["Interval End"].max() == most_recent_monday + pd.Timedelta(days=14)

        self._check_lmp_weekly(df)

    def test_get_lmp_weekly_raises_error_if_no_data(self):
        date = self.local_today() - pd.DateOffset(days=5)

        with pytest.raises(NoDataFoundException):
            self.iso.get_lmp_weekly(date)

    """get_lmp"""

    @with_markets(Markets.REAL_TIME_HOURLY_FINAL, Markets.REAL_TIME_HOURLY_PRELIM)
    def test_lmp_date_range(self, market):
        offset_from_today = 5 if market == Markets.REAL_TIME_HOURLY_FINAL else 1
        super().test_lmp_date_range(market, offset_from_today)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_HOURLY_FINAL,
        Markets.REAL_TIME_HOURLY_PRELIM,
    )
    def test_get_lmp_historical(self, market):
        # Prelim data only goes back 4 days
        if market == Markets.REAL_TIME_HOURLY_PRELIM:
            date = self.local_today() - pd.Timedelta(days=2)
        else:
            date = self.local_today() - pd.Timedelta(days=100)

        date_str = date.strftime("%Y-%m-%d")

        super().test_get_lmp_historical(market, date_str=date_str)

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
        "LRZ1 MTLF",
        "LRZ2_7 MTLF",
        "LRZ3_5 MTLF",
        "LRZ4 MTLF",
        "LRZ6 MTLF",
        "LRZ8_9_10 MTLF",
        "MISO MTLF",
    ]

    def test_get_load_forecast_today(self):
        df = self.iso.get_load_forecast("today")

        assert df.columns.tolist() == self.load_forecast_cols

        assert (df["Publish Time"] == self.local_start_of_today()).all()
        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.Timedelta(
            days=6,
        )

    def test_get_load_forecast_latest(self):
        assert self.iso.get_load_forecast("latest").equals(
            self.iso.get_load_forecast("today"),
        )

    def test_get_load_forecast_historical(self):
        past_date = self.local_today() - pd.Timedelta(days=30)
        df = self.iso.get_load_forecast(past_date)

        assert df.columns.tolist() == self.load_forecast_cols

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            past_date,
        ) + pd.Timedelta(days=6)

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
        ) + pd.Timedelta(days=5)

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
        past_date = self.local_today() - pd.Timedelta(days=100)
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
        past_date = self.local_today() - pd.Timedelta(days=100)
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

    """get_outages_forecast"""

    def _check_outages(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Region",
            "Derated Outages MW",
            "Forced Outages MW",
            "Planned Outages MW",
            "Unplanned Outages MW",
        ]

        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            "1d",
        )

        assert (df["Region"].unique() == ["Central", "MISO", "North", "South"]).all()

    def test_get_outages_forecast_latest(self):
        df = self.iso.get_outages_forecast("latest")

        self._check_outages(df)

        # Latest fetches the file published yesterday with the first forecast day today
        expected_start_date = self.local_start_of_today()

        assert df["Publish Time"].unique() == expected_start_date - pd.DateOffset(
            days=1,
        )
        assert df["Interval Start"].min() == expected_start_date
        assert df["Interval End"].max() == expected_start_date + pd.DateOffset(days=7)

    def test_get_outages_forecast_historical_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=100)
        end = start + pd.DateOffset(days=3)

        df = self.iso.get_outages_forecast(start, end)

        self._check_outages(df)

        assert df["Interval Start"].min() == start + pd.DateOffset(days=1)

        assert df["Interval End"].max() == end + pd.DateOffset(days=7)

        assert df["Publish Time"].min() == start
        assert df["Publish Time"].nunique() == 3

    """get_outages_lookback"""

    def test_get_outages_lookback_latest(self):
        df = self.iso.get_outages_lookback("latest")

        self._check_outages(df)

        # Latest fetches the file published yesterday
        expected_start_date = self.local_start_of_today() - pd.DateOffset(days=30)

        assert df[
            "Publish Time"
        ].unique() == self.local_start_of_today() - pd.DateOffset(days=1)

        assert df["Interval Start"].min() == expected_start_date
        assert df["Interval End"].max() == self.local_start_of_today()

    def test_get_outages_lookback_historical_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=100)
        end = start + pd.DateOffset(days=3)

        df = self.iso.get_outages_lookback(start, end)

        self._check_outages(df)

        assert df["Interval Start"].min() == start - pd.DateOffset(days=29)
        assert df["Interval End"].max() == end

        assert df["Publish Time"].min() == start
        assert df["Publish Time"].nunique() == 3
