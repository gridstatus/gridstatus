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
