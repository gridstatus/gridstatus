import pandas as pd
import pytest
from pandas.core.dtypes.common import is_numeric_dtype

from gridstatus import IESO
from gridstatus.base import NotSupported
from gridstatus.ieso import (
    MAXIMUM_DAYS_IN_FUTURE_FOR_ZONAL_LOAD_FORECAST,
    MAXIMUM_DAYS_IN_PAST_FOR_LOAD,
)
from gridstatus.tests.base_test_iso import BaseTestISO


class TestIESO(BaseTestISO):
    iso = IESO()
    default_timezone = iso.default_timezone

    def test_init(self):
        assert self.iso is not None

    # TODO fuel mix tests
    """get_fuel_mix"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_date_or_start(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_historical(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_historical_with_date_range(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_range_two_days_with_day_start_endpoint(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_start_end_same_day(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_latest(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_today(self):
        pass

    """get_interconnection_queue"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_interconnection_queue(self):
        pass

    """get_lmp"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_lmp_date_range(self, market=None):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_lmp_historical(self, market=None):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_lmp_latest(self, market=None):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_lmp_today(self, market=None):
        pass

    """get_load"""

    def test_get_load_today(self):
        df = self.iso.get_load("today")
        self._check_load(df)

        today = pd.Timestamp.now(tz=self.default_timezone)
        # First interval on the day
        assert df["Interval Start"].min() == today.normalize()
        assert df["Interval End"].min() == today.normalize() + pd.Timedelta(minutes=5)
        assert df["Interval Start"].max().date() == today.date()

        assert (df["Interval Start"].dt.date == today.date()).all()

    def test_get_load_latest(self):
        df = self.iso.get_load("latest")

        self._check_load(df)
        now = pd.Timestamp.now(tz=self.default_timezone)
        # First interval should be the first interval of the hour
        assert df["Interval Start"].min() == now.floor("H")

        assert df.shape[0] <= 12

    def test_get_load_yesterday_full_day(self):
        date = (
            pd.Timestamp.now(tz=self.default_timezone) - pd.Timedelta(days=1)
        ).date()
        end = date + pd.Timedelta(days=1)
        df = self.iso.get_load(date, end=end)
        assert df.shape[0] == 288

        beginning_of_date = pd.Timestamp(date, tz=self.default_timezone).replace(
            hour=0,
            minute=0,
            second=0,
        )
        assert df["Interval Start"].min() == beginning_of_date

        end_of_date = beginning_of_date + pd.Timedelta(days=1)
        assert df["Interval End"].max() == end_of_date

    def test_get_load_historical_with_date_range(self):
        num_days = 2
        end = pd.Timestamp.now(
            tz=self.default_timezone,
        ) + pd.Timedelta(days=1)
        start = end - pd.Timedelta(days=num_days)

        data = self.iso.get_load(date=start.date(), end=end.date())
        self._check_load(data)
        # make sure right number of days are returned
        assert data["Interval Start"].dt.day.nunique() == num_days

        data_tuple = self.iso.get_load(date=(start.date(), end.date()))

        assert data_tuple.equals(data)

    def test_get_load_historical(self):
        # pick a test date 2 weeks back
        test_date = (pd.Timestamp.now() - pd.Timedelta(days=14)).date()

        # date string works
        date_str = test_date.strftime("%Y%m%d")
        df = self.iso.get_load(date_str)
        self._check_load(df)
        assert df.loc[0]["Interval Start"].strftime("%Y%m%d") == date_str

        # timestamp object works
        df = self.iso.get_load(test_date)

        self._check_load(df)
        assert df.loc[0]["Interval Start"].strftime(
            "%Y%m%d",
        ) == test_date.strftime("%Y%m%d")

        # datetime object works
        df = self.iso.get_load(test_date)
        self._check_load(df)
        assert df.loc[0]["Interval Start"].strftime(
            "%Y%m%d",
        ) == test_date.strftime("%Y%m%d")

    def test_get_load_tomorrow_raises_error(self):
        with pytest.raises(NotSupported):
            self.iso.get_load(
                pd.Timestamp.now(tz=self.default_timezone).date()
                + pd.Timedelta(days=1),
            )

    def test_get_load_too_far_in_past_raises_error(self):
        with pytest.raises(NotSupported):
            self.iso.get_load(
                pd.Timestamp.now(tz=self.default_timezone).date()
                - pd.Timedelta(days=MAXIMUM_DAYS_IN_PAST_FOR_LOAD + 1),
            )

    """get_load_forecast"""

    def test_get_load_forecast_today(self):
        forecast = self.iso.get_load_forecast("today")
        self._check_load_forecast(forecast)

        assert forecast["Publish Time"].nunique() == 1
        assert forecast["Interval Start"].min() == pd.Timestamp.now(
            tz=self.default_timezone,
        ).normalize() - pd.Timedelta(days=5)

        assert forecast["Interval Start"].max() == pd.Timestamp.now(
            tz=self.default_timezone,
        ).normalize() + pd.Timedelta(days=2)

    def test_get_load_forecast_latest(self):
        assert self.iso.get_load_forecast("latest").equals(
            self.iso.get_load_forecast("today"),
        )

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical_with_date_range(self):
        pass

    """get_zonal_load_forecast"""

    def test_get_zonal_load_forecast_historical(self):
        test_date = (pd.Timestamp.now() - pd.Timedelta(days=3)).date()
        forecast = self.iso.get_zonal_load_forecast(date=test_date)
        self._check_zonal_load_forecast(forecast)

    def test_get_zonal_load_forecast_historical_with_date_range(self):
        end = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
        start = (end - pd.Timedelta(days=2)).date()
        forecast = self.iso.get_zonal_load_forecast(
            start,
            end=end,
        )
        self._check_zonal_load_forecast(forecast)

    def test_get_zonal_load_forecast_today(self):
        forecast = self.iso.get_zonal_load_forecast("today")

        assert (
            forecast["Interval Start"].max().date()
            - pd.Timestamp.now(tz=self.default_timezone).date()
        ).days == MAXIMUM_DAYS_IN_FUTURE_FOR_ZONAL_LOAD_FORECAST

        assert (
            forecast["Interval Start"].min()
            == pd.Timestamp.now(tz=self.default_timezone).normalize()
        )

        self._check_zonal_load_forecast(forecast)

        assert (
            forecast["Interval Start"].min()
            == pd.Timestamp.now(tz=self.default_timezone).normalize()
        )

        self._check_zonal_load_forecast(forecast)

    def test_get_zonal_load_forecast_latest(self):
        assert self.iso.get_zonal_load_forecast("latest").equals(
            self.iso.get_zonal_load_forecast("today"),
        )

    """get_status"""

    def test_get_status_latest(self):
        # ensure there is a homepage if gridstatus can retrieve a status
        assert isinstance(self.iso.status_homepage, str)

    """get_storage"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_storage_historical(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_storage_today(self):
        pass

    def _check_load(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.shape[0] >= 0

        time_type = "interval"
        self._check_time_columns(df, instant_or_interval=time_type)

        for col in ["Market Total Load", "Ontario Load"]:
            assert col in df.columns
            assert is_numeric_dtype(df[col])

    def _check_time_columns(self, df, instant_or_interval="instant"):
        assert isinstance(df, pd.DataFrame)

        time_cols = ["Interval Start", "Interval End"]
        ordered_by_col = "Interval Start"

        assert time_cols == df.columns[: len(time_cols)].tolist()
        # check all time cols are localized timestamps
        for col in time_cols:
            assert isinstance(df.loc[0][col], pd.Timestamp)
            assert df.loc[0][col].tz is not None

        self._check_ordered_by_time(df, ordered_by_col)

    def _check_load_forecast(self, df):
        assert set(df.columns) == set(
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Ontario Load Forecast",
            ],
        )

        assert self._check_is_datetime_type(df["Publish Time"])
        assert self._check_is_datetime_type(df["Interval Start"])
        assert self._check_is_datetime_type(df["Interval End"])
        assert df["Ontario Load Forecast"].dtype == "float64"

    def _check_zonal_load_forecast(self, df):
        assert set(df.columns) == set(
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Ontario Load Forecast",
                "East Load Forecast",
                "West Load Forecast",
            ],
        )

        assert self._check_is_datetime_type(df["Publish Time"])
        assert self._check_is_datetime_type(df["Interval Start"])
        assert self._check_is_datetime_type(df["Interval End"])
        assert df["Ontario Load Forecast"].dtype == "float64"
        assert df["East Load Forecast"].dtype == "float64"
        assert df["West Load Forecast"].dtype == "float64"
