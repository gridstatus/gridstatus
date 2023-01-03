import pandas as pd

from gridstatus.base import FuelMix, GridStatus


class BaseTestISO:

    iso = None

    def test_init(self):
        assert self.iso is not None

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

    def test_get_status_latest(self):
        status = self.iso.get_status("latest")
        assert isinstance(status, GridStatus)

        # ensure there is a homepage if gridstatus can retrieve a status
        assert isinstance(self.iso.status_homepage, str)
