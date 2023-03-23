import pandas as pd
import pytest
from pandas.core.dtypes.common import is_numeric_dtype

from gridstatus.base import GridStatus, _interconnection_columns


class BaseTestISO:

    iso = None

    def test_init(self):
        assert self.iso is not None

    """get_fuel_mix"""

    def test_get_fuel_mix_date_or_start(self):
        num_days = 2
        end = pd.Timestamp.now(tz=self.iso.default_timezone)
        start = end - pd.Timedelta(days=num_days)

        df = self.iso.get_fuel_mix(date=start.date(), end=end.date())
        self._check_fuel_mix(df)

        df = self.iso.get_fuel_mix(
            start=start.date(),
            end=end.date(),
        )
        self._check_fuel_mix(df)

        df = self.iso.get_fuel_mix(date=start.date())
        self._check_fuel_mix(df)

        df = self.iso.get_fuel_mix(start=start.date())
        self._check_fuel_mix(df)

        with pytest.raises(ValueError):
            self.iso.get_fuel_mix(start=start.date(), date=start.date())

    def test_get_fuel_mix_historical(self):
        # date string works
        date_str = "04/03/2022"
        df = self.iso.get_fuel_mix(date_str)
        assert isinstance(df, pd.DataFrame)
        assert df.loc[0]["Time"].strftime("%m/%d/%Y") == date_str
        assert df.loc[0]["Time"].tz is not None
        self._check_fuel_mix(df)

        # timestamp object works
        date_obj = pd.to_datetime("2019/11/19")
        df = self.iso.get_fuel_mix(date_obj)
        assert isinstance(df, pd.DataFrame)
        assert df.loc[0]["Time"].strftime(
            "%Y%m%d",
        ) == date_obj.strftime("%Y%m%d")
        assert df.loc[0]["Time"].tz is not None
        self._check_fuel_mix(df)

        # datetime object works
        date_obj = pd.to_datetime("2021/05/09").date()
        df = self.iso.get_fuel_mix(date_obj)
        assert isinstance(df, pd.DataFrame)
        assert df.loc[0]["Time"].strftime(
            "%Y%m%d",
        ) == date_obj.strftime("%Y%m%d")
        assert df.loc[0]["Time"].tz is not None
        self._check_fuel_mix(df)

    def test_get_fuel_mix_historical_with_date_range(self):
        # range not inclusive, add one to include today
        num_days = 7
        end = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ) + pd.Timedelta(days=1)
        start = end - pd.Timedelta(days=num_days)

        df = self.iso.get_fuel_mix(date=start.date(), end=end.date())
        self._check_fuel_mix(df)

        # make sure right number of days are returned
        assert df["Time"].dt.day.nunique() == num_days

    def test_get_fuel_mix_latest(self):
        df = self.iso.get_fuel_mix("latest")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert isinstance(df.Time.iloc[0], pd.Timestamp)
        assert df.index.name is None
        assert df.index.tolist() == [0]
        self._check_fuel_mix(df)

    def test_get_fuel_mix_today(self):
        df = self.iso.get_fuel_mix("today")
        self._check_fuel_mix(df)

    """get_interconnection_queue"""

    def test_get_interconnection_queue(self):
        queue = self.iso.get_interconnection_queue()
        # todo make sure datetime columns are right type
        assert isinstance(queue, pd.DataFrame)
        assert queue.shape[0] > 0
        assert set(_interconnection_columns).issubset(queue.columns)

    """get_lmp"""

    # @pytest.mark.parametrize in ISO
    def test_get_lmp_historical(self, market=None):
        date_str = "2022-07-22"
        if market is not None:
            hist = self.iso.get_lmp(date_str, market=market)
            assert isinstance(hist, pd.DataFrame)
            self._check_lmp_columns(hist, market)

    # @pytest.mark.parametrize in ISO
    def test_get_lmp_latest(self, market=None):
        if market is not None:
            df = self.iso.get_lmp("latest", market=market)
            assert isinstance(df, pd.DataFrame)
            self._check_lmp_columns(df, market)

    # @pytest.mark.parametrize in ISO
    def test_get_lmp_today(self, market=None):
        if market is not None:
            df = self.iso.get_lmp("today", market=market)
            assert isinstance(df, pd.DataFrame)
            self._check_lmp_columns(df, market)

    """get_load"""

    def test_get_load_historical_with_date_range(self):
        num_days = 7
        end = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ) + pd.Timedelta(days=1)
        start = end - pd.Timedelta(days=num_days)
        data = self.iso.get_load(date=start.date(), end=end.date())
        self._check_load(data)
        # make sure right number of days are returned
        assert data["Time"].dt.day.nunique() == num_days

    def test_get_load_historical(self):
        # pick a test date 2 weeks back
        test_date = (pd.Timestamp.now() - pd.Timedelta(days=14)).date()

        # date string works
        date_str = test_date.strftime("%Y%m%d")
        df = self.iso.get_load(date_str)
        self._check_load(df)
        assert df.loc[0]["Time"].strftime("%Y%m%d") == date_str

        # timestamp object works
        df = self.iso.get_load(test_date)

        self._check_load(df)
        assert df.loc[0]["Time"].strftime(
            "%Y%m%d",
        ) == test_date.strftime("%Y%m%d")

        # datetime object works
        df = self.iso.get_load(test_date)
        self._check_load(df)
        assert df.loc[0]["Time"].strftime(
            "%Y%m%d",
        ) == test_date.strftime("%Y%m%d")

    def test_get_load_latest(self):
        load = self.iso.get_load("latest")
        set(["time", "load"]) == load.keys()
        assert is_numeric_dtype(type(load["load"]))
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        assert load["time"].date() == today

    def test_get_load_today(self):
        df = self.iso.get_load("today")
        self._check_load(df)
        today = pd.Timestamp.now(tz=self.iso.default_timezone)
        assert (df["Time"].dt.date == today.date()).all()
        return df

    """get_load_forecast"""

    def test_get_load_forecast_historical(self):
        test_date = (pd.Timestamp.now() - pd.Timedelta(days=14)).date()
        forecast = self.iso.get_load_forecast(date=test_date)
        self._check_forecast(forecast)

    def test_get_load_forecast_historical_with_date_range(self):
        end = pd.Timestamp.now().normalize() - pd.Timedelta(days=14)
        start = (end - pd.Timedelta(days=7)).date()
        forecast = self.iso.get_load_forecast(
            start,
            end=end,
        )
        self._check_forecast(forecast)

    def test_get_load_forecast_today(self):
        forecast = self.iso.get_load_forecast("today")
        self._check_forecast(forecast)

    """get_status"""

    def test_get_status_latest(self):
        status = self.iso.get_status("latest")
        assert isinstance(status, GridStatus)

        # ensure there is a homepage if gridstatus can retrieve a status
        assert isinstance(self.iso.status_homepage, str)

    """get_storage"""

    def test_get_storage_historical(self):
        test_date = (pd.Timestamp.now() - pd.Timedelta(days=14)).date()
        storage = self.iso.get_storage(date=test_date)
        self._check_storage(storage)

    def test_get_storage_today(self):
        storage = self.iso.get_storage("today")
        self._check_storage(storage)

    """other"""

    def _check_ordered_by_time(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.shape[0] > 0
        assert df["Interval Start"].is_monotonic_increasing

    def _check_time_columns(self, df):
        assert isinstance(df, pd.DataFrame)
        time_cols = ["Time", "Interval Start", "Interval End"]

        assert time_cols == df.columns[: len(time_cols)].tolist()
        # check all time cols are localized timestamps
        for col in time_cols:
            assert isinstance(df.loc[0][col], pd.Timestamp)
            assert df.loc[0][col].tz is not None

        self._check_ordered_by_time(df)

    def _check_fuel_mix(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.name is None
        self._check_time_columns(df)

    def _check_load(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.shape[0] >= 0
        self._check_time_columns(df)
        assert "Load" in df.columns
        assert is_numeric_dtype(df["Load"])

    def _check_forecast(self, df):
        assert set(df.columns) == set(
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Forecast Time",
                "Load Forecast",
            ],
        )

        assert self._check_is_datetime_type(df["Forecast Time"])
        assert self._check_is_datetime_type(df["Time"])

    def _check_is_datetime_type(self, series):
        return pd.core.dtypes.common.is_datetime64_ns_dtype(
            series,
        ) | pd.core.dtypes.common.is_timedelta64_ns_dtype(series)

    def _check_lmp_columns(self, df, market):
        # todo in future all ISO should return same columns
        # maybe with the exception of "LMP" breakdown
        self._check_time_columns(df)

        assert set(
            [
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
            ],
        ).issubset(df.columns)

        assert len(df["Market"].unique()) == 1
        assert df["Market"].unique()[0] == market.value
        assert df.shape[0] >= 0

    def _check_storage(self, df):
        assert set(["Time", "Interval Start", "Interval End", "Supply"]).issubset(
            df.columns,
        )
