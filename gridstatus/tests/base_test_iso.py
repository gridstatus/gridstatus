import pandas as pd
import pytest
from pandas.core.dtypes.common import is_numeric_dtype

import gridstatus
from gridstatus.base import GridStatus, _interconnection_columns


class TestHelperMixin:
    iso = None

    def local_now(self):
        return pd.Timestamp.now(tz=self.iso.default_timezone)

    def local_today(self):
        return self.local_now().date()

    def local_start_of_day(self, date: str | pd.Timestamp | None):
        return pd.Timestamp(date).tz_localize(self.iso.default_timezone).normalize()

    def local_start_of_today(self):
        return self.local_start_of_day(self.local_today())

    def _check_ordered_by_time(self, df, col):
        assert isinstance(df, pd.DataFrame)
        assert df.shape[0] > 0
        assert df[col].is_monotonic_increasing

    def _check_time_columns(
        self,
        df,
        instant_or_interval="interval",
        skip_column_named_time=False,
        sced=False,
    ):
        assert isinstance(df, pd.DataFrame)

        if instant_or_interval == "interval":
            # TODO: remove "Time" from time_cols for "interval"
            time_cols = ["Time", "Interval Start", "Interval End"]
            ordered_by_col = "Interval Start"
        elif instant_or_interval == "instant":
            if sced:
                time_cols = ["SCED Timestamp"]
                ordered_by_col = "SCED Timestamp"
            else:
                time_cols = ["Time"]
                ordered_by_col = "Time"
            assert "Interval Start" not in df.columns
            assert "Interval End" not in df.columns
        else:
            raise ValueError(
                "instant_or_interval must be 'interval' or 'instant'",
            )

        if skip_column_named_time:
            time_cols.remove("Time")

        assert time_cols == df.columns[: len(time_cols)].tolist()
        # check all time cols are localized timestamps
        for col in time_cols:
            assert isinstance(df.loc[0][col], pd.Timestamp)
            assert df.loc[0][col].tz is not None

        self._check_ordered_by_time(df, ordered_by_col)


class BaseTestISO(TestHelperMixin):
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

    def test_get_fuel_mix_historical(self, time_column="Time"):
        # date string works
        date_str = "04/03/2022"
        df = self.iso.get_fuel_mix(date_str)

        assert isinstance(df, pd.DataFrame)
        assert df.loc[0][time_column].strftime("%m/%d/%Y") == date_str
        assert df.loc[0][time_column].tz is not None
        self._check_fuel_mix(df)

        # timestamp object works
        date_obj = pd.to_datetime("2019/11/19")
        df = self.iso.get_fuel_mix(date_obj)
        assert isinstance(df, pd.DataFrame)
        assert df.loc[0][time_column].strftime(
            "%Y%m%d",
        ) == date_obj.strftime("%Y%m%d")
        assert df.loc[0][time_column].tz is not None
        self._check_fuel_mix(df)

        # datetime object works
        date_obj = pd.to_datetime("2021/05/09").date()
        df = self.iso.get_fuel_mix(date_obj)
        assert isinstance(df, pd.DataFrame)
        assert df.loc[0][time_column].strftime(
            "%Y%m%d",
        ) == date_obj.strftime("%Y%m%d")
        assert df.loc[0][time_column].tz is not None
        self._check_fuel_mix(df)

    def test_get_fuel_mix_historical_with_date_range(self, time_column="Time"):
        # range not inclusive, add one to include today
        num_days = 7
        end = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ) + pd.Timedelta(days=1)
        start = end - pd.Timedelta(days=num_days)

        df = self.iso.get_fuel_mix(date=start.date(), end=end.date())
        self._check_fuel_mix(df)

        # make sure right number of days are returned
        assert df[time_column].dt.day.nunique() == num_days

    def test_get_fuel_mix_range_two_days_with_day_start_endpoint(self):
        yesterday = gridstatus.utils._handle_date(
            "today",
            self.iso.default_timezone,
        ) - pd.Timedelta(days=1)
        yesterday = yesterday.replace(hour=1, minute=0, second=0, microsecond=0)
        start = yesterday - pd.Timedelta(hours=3)

        # add one minute since pjm is exclusive of end date
        # and does not include the whole day like other isos
        df = self.iso.get_fuel_mix(start=start, end=yesterday + pd.Timedelta(minutes=1))

        assert df["Time"].max() >= yesterday.replace(hour=0, minute=0, second=0)
        assert df["Time"].min() <= start

        self._check_fuel_mix(df)

    def test_get_fuel_mix_start_end_same_day(self):
        yesterday = gridstatus.utils._handle_date(
            "today",
            self.iso.default_timezone,
        ) - pd.Timedelta(days=1)
        start = yesterday.replace(hour=0, minute=5, second=0, microsecond=0)
        end = yesterday.replace(hour=6, minute=5, second=0, microsecond=0)
        df = self.iso.get_fuel_mix(start=start, end=end)
        # ignore last row, since it is sometime midnight of next day
        assert df["Time"].iloc[:-1].dt.date.unique().tolist() == [yesterday.date()]
        self._check_fuel_mix(df)

    def test_get_fuel_mix_latest(self, time_column="Time"):
        df = self.iso.get_fuel_mix("latest")
        assert isinstance(df, pd.DataFrame)
        assert isinstance(df[time_column].iloc[0], pd.Timestamp)
        assert df.index.name is None
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
    def test_lmp_date_range(self, market=None, offset_from_today=0):
        end = pd.Timestamp.now(tz=self.iso.default_timezone).date() - pd.DateOffset(
            days=offset_from_today,
        )
        three_days_ago = end - pd.Timedelta(days=3)

        df_1 = self.iso.get_lmp(start=three_days_ago, end=end, market=market)
        df_2 = self.iso.get_lmp(date=(three_days_ago, end), market=market)

        self._check_lmp_columns(df_1, market)
        assert df_1.equals(df_2)

    # @pytest.mark.parametrize in ISO
    # Allow for passing in a date string in case the ISO's historical lmp data
    # is not available for the selected date.
    def test_get_lmp_historical(self, market=None, date_str="2022-07-22"):
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

    @pytest.mark.integration
    def test_get_load_historical_with_date_range(self):
        num_days = 4
        end = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ) + pd.Timedelta(days=1)
        start = end - pd.Timedelta(days=num_days)

        data = self.iso.get_load(date=start.date(), end=end.date())
        self._check_load(data)
        # make sure right number of days are returned
        assert data["Time"].dt.day.nunique() == num_days

        data_tuple = self.iso.get_load(date=(start.date(), end.date()))

        assert data_tuple.equals(data)

    @pytest.mark.integration
    def test_get_load_historical(self, lookback_days=14):
        # pick a test date lookback_days ago
        test_date = (pd.Timestamp.now() - pd.Timedelta(days=lookback_days)).date()

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
        df = self.iso.get_load("latest")
        self._check_load(df)
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        assert df["Time"].max().date() == today

    def test_get_load_today(self):
        df = self.iso.get_load("today")
        self._check_load(df)
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()

        # okay as long as one of these columns is only today
        assert (
            (df["Time"].dt.date == today).all()
            or (df["Interval Start"].dt.date == today).all()
            or (df["Interval End"].dt.date == today).all()
        )

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

    def _check_fuel_mix(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.name is None

        time_type = "interval"
        if self.iso.iso_id in ["nyiso", "isone", "ercot"]:
            time_type = "instant"
        elif self.iso.iso_id in ["caiso", "spp", "miso", "pjm", "ieso"]:
            time_type = "interval"
        else:
            raise ValueError("Unknown ISO ID")
        self._check_time_columns(df, instant_or_interval=time_type)

    def _check_load(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.shape[0] >= 0

        if self.iso.iso_id in ["nyiso"]:
            time_type = "instant"
        elif self.iso.iso_id in [
            "caiso",
            "isone",
            "spp",
            "miso",
            "pjm",
            "ercot",
            "ieso",
        ]:
            time_type = "interval"
        self._check_time_columns(df, instant_or_interval=time_type)
        assert "Load" in df.columns
        assert is_numeric_dtype(df["Load"])

    def _check_forecast(self, df, expected_columns=None):
        if expected_columns is not None:
            assert set(df.columns) == set(expected_columns)
        else:
            # support both old and new format
            # eventually all ISOs should return publish time
            publish_column_name = (
                "Forecast Time" if "Forecast Time" in df.columns else "Publish Time"
            )
            assert set(df.columns) == set(
                [
                    "Time",
                    "Interval Start",
                    "Interval End",
                    publish_column_name,
                    "Load Forecast",
                ],
            )

        assert self._check_is_datetime_type(
            df["Forecast Time" if "Forecast Time" in df.columns else "Publish Time"],
        )
        assert self._check_is_datetime_type(df["Time"])

    def _check_is_datetime_type(self, series):
        # any_dtype allows for TZ-aware or naive datetimes
        return pd.api.types.is_datetime64_any_dtype(
            series,
        ) | pd.api.types.is_timedelta64_dtype(series)

    lmp_cols = [
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

    def _check_lmp_columns(self, df, market):
        # todo in future all ISO should return same columns
        # maybe with the exception of "LMP" breakdown
        self._check_time_columns(df)

        assert set(
            self.lmp_cols,
        ).issubset(df.columns)

        assert len(df["Market"].unique()) == 1
        assert df["Market"].unique()[0] == market.value
        assert df.shape[0] >= 0

    def _check_storage(self, df):
        assert set(["Time", "Interval Start", "Interval End", "Supply"]).issubset(
            df.columns,
        )
