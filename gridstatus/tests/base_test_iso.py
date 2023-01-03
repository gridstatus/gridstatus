import pandas as pd
from pandas.core.dtypes.common import is_numeric_dtype

from gridstatus.base import FuelMix, GridStatus


class BaseTestISO:

    iso = None

    def test_init(self):
        assert self.iso is not None

    def test_get_fuel_mix_historical(self):
        # date string works
        date_str = "04/03/2022"
        df = self.iso.get_fuel_mix(date_str)
        assert isinstance(df, pd.DataFrame)
        assert df.loc[0]["Time"].strftime("%m/%d/%Y") == date_str
        assert df.loc[0]["Time"].tz is not None

        # timestamp object works
        date_obj = pd.to_datetime("2019/11/19")
        df = self.iso.get_fuel_mix(date_obj)
        assert isinstance(df, pd.DataFrame)
        assert df.loc[0]["Time"].strftime("%Y%m%d") == date_obj.strftime("%Y%m%d")
        assert df.loc[0]["Time"].tz is not None

        # datetime object works
        date_obj = pd.to_datetime("2021/05/09").date()
        df = self.iso.get_fuel_mix(date_obj)
        assert isinstance(df, pd.DataFrame)
        assert df.loc[0]["Time"].strftime("%Y%m%d") == date_obj.strftime("%Y%m%d")
        assert df.loc[0]["Time"].tz is not None

    def test_get_fuel_mix_historical_with_date_range(self):
        # range not inclusive, add one to include today
        num_days = 7
        end = pd.Timestamp.now(tz=self.iso.default_timezone) + pd.Timedelta(days=1)
        start = end - pd.Timedelta(days=num_days)

        data = self.iso.get_fuel_mix(date=start.date(), end=end.date())
        # make sure right number of days are returned
        assert data["Time"].dt.day.nunique() == num_days

    def test_get_fuel_mix_latest(self):
        mix = self.iso.get_fuel_mix("latest")
        assert isinstance(mix, FuelMix)
        assert isinstance(mix.time, pd.Timestamp)
        assert isinstance(mix.mix, pd.DataFrame)
        assert repr(mix)
        assert len(mix.mix) > 0
        assert mix.iso == self.iso.name
        assert isinstance(repr(mix), str)

    def test_get_fuel_mix_today(self):
        df = self.iso.get_fuel_mix("today")
        assert isinstance(df, pd.DataFrame)

    # @pytest.mark.parametrize in ISO
    def test_get_lmp_historical(self, market=None):
        date_str = "20220722"
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

    def test_get_load_historical(self):
        # pick a test date 2 weeks back
        test_date = (pd.Timestamp.now() - pd.Timedelta(days=14)).date()

        # date string works
        date_str = test_date.strftime("%Y%m%d")
        df = self.iso.get_load(date_str)
        assert isinstance(df, pd.DataFrame)
        assert set(["Time", "Load"]) == set(df.columns)
        assert df.loc[0]["Time"].strftime("%Y%m%d") == date_str
        assert is_numeric_dtype(df["Load"])

        # timestamp object works
        df = self.iso.get_load(test_date)
        assert isinstance(df, pd.DataFrame)
        assert set(["Time", "Load"]) == set(df.columns)
        assert df.loc[0]["Time"].strftime("%Y%m%d") == test_date.strftime("%Y%m%d")
        assert is_numeric_dtype(df["Load"])

        # datetime object works
        df = self.iso.get_load(test_date)
        assert isinstance(df, pd.DataFrame)
        assert set(["Time", "Load"]) == set(df.columns)
        assert df.loc[0]["Time"].strftime("%Y%m%d") == test_date.strftime("%Y%m%d")
        assert is_numeric_dtype(df["Load"])

    def test_get_load_latest(self):
        load = self.iso.get_load("latest")
        set(["time", "load"]) == load.keys()
        assert is_numeric_dtype(type(load["load"]))

    def test_get_load_today(self):
        df = self.iso.get_load("today")
        assert isinstance(df, pd.DataFrame)
        assert ["Time", "Load"] == df.columns.tolist()
        assert is_numeric_dtype(df["Load"])
        assert isinstance(df.loc[0]["Time"], pd.Timestamp)
        assert df.loc[0]["Time"].tz is not None

    def test_get_status_latest(self):
        status = self.iso.get_status("latest")
        assert isinstance(status, GridStatus)

        # ensure there is a homepage if gridstatus can retrieve a status
        assert isinstance(self.iso.status_homepage, str)

    def _check_forecast(self, df):
        assert set(df.columns) == set(
            ["Forecast Time", "Time", "Load Forecast"],
        )

        assert self._check_is_datetime_type(df["Forecast Time"])
        assert self._check_is_datetime_type(df["Time"])

    def _check_is_datetime_type(self, series):
        return pd.core.dtypes.common.is_datetime64_ns_dtype(
            series,
        ) | pd.core.dtypes.common.is_timedelta64_ns_dtype(series)

    @staticmethod
    def _check_lmp_columns(df, market):
        assert set(
            [
                "Time",
                "Market",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ],
        ).issubset(df.columns)
        assert df["Market"].unique()[0] == market.value
