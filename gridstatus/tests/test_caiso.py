import pytest

from gridstatus import CAISO, Markets
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets


class TestCAISO(BaseTestISO):
    iso = CAISO()

    """get_as"""

    def test_get_as_prices(self):
        date = "Oct 15, 2022"
        df = self.iso.get_as_prices(date)

        assert df.shape[0] > 0

        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "Region",
            "Market",
            "Non-Spinning Reserves",
            "Regulation Down",
            "Regulation Mileage Down",
            "Regulation Mileage Up",
            "Regulation Up",
            "Spinning Reserves",
        ]

    def test_get_as_procurement(self):
        date = "Oct 15, 2022"
        for market in ["DAM", "RTM"]:
            df = self.iso.get_as_procurement(date, market=market)
            self._check_as_data(df, market)

    """get_curtailment"""

    def _check_curtailment(self, df):
        assert df.shape[0] > 0
        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "Curtailment Type",
            "Curtailment Reason",
            "Fuel Type",
            "Curtailment (MWh)",
            "Curtailment (MW)",
        ]
        self._check_time_columns(df)

    def test_get_curtailment(self):
        date = "Oct 15, 2022"
        df = self.iso.get_curtailment(date)
        assert df.shape == (31, 8)
        self._check_curtailment(df)

    def test_get_curtailment_2_pages(self):
        # test that the function can handle 3 pages of data
        date = "March 15, 2022"
        df = self.iso.get_curtailment(date)
        assert df.shape == (55, 8)
        self._check_curtailment(df)

    def test_get_curtailment_3_pages(self):
        # test that the function can handle 3 pages of data
        date = "March 16, 2022"
        df = self.iso.get_curtailment(date)
        assert df.shape == (76, 8)
        self._check_curtailment(df)

    """get_gas_prices"""

    def test_get_gas_prices(self):
        date = "Oct 15, 2022"
        # no fuel region
        df = self.iso.get_gas_prices(date=date)

        n_unique = 153
        assert df["Fuel Region Id"].nunique() == n_unique
        assert len(df) == n_unique * 24

        # single fuel region
        test_region_1 = "FRPGE2GHG"
        df = self.iso.get_gas_prices(date=date, fuel_region_id=test_region_1)
        assert df["Fuel Region Id"].unique()[0] == test_region_1
        assert len(df) == 24

        # list of fuel regions
        test_region_2 = "FRSCE8GHG"
        df = self.iso.get_gas_prices(
            date=date,
            fuel_region_id=[
                test_region_1,
                test_region_2,
            ],
        )
        assert set(df["Fuel Region Id"].unique()) == set(
            [test_region_1, test_region_2],
        )
        assert len(df) == 24 * 2

    """get_ghg_allowance"""

    def test_get_ghg_allowance(self):
        date = "Oct 15, 2022"
        df = self.iso.get_ghg_allowance(date)

        assert len(df) == 1
        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "GHG Allowance Price",
        ]

    """get_lmp"""

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_15_MIN,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_historical(self, market):
        super().test_get_lmp_historical(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_15_MIN,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_latest(self, market):
        super().test_get_lmp_latest(market=market)

    def test_get_lmp_locations_must_be_list(self):
        date = "today"
        with pytest.raises(AssertionError):
            self.iso.get_lmp(date, locations="foo", market="REAL_TIME_5_MIN")

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_15_MIN,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_today(self, market):
        super().test_get_lmp_today(market=market)

    @staticmethod
    def _check_as_data(df, market):
        columns = [
            "Time",
            "Interval Start",
            "Interval End",
            "Region",
            "Market",
            "Non-Spinning Reserves Procured (MW)",
            "Non-Spinning Reserves Self-Provided (MW)",
            "Non-Spinning Reserves Total (MW)",
            "Non-Spinning Reserves Total Cost",
            "Regulation Down Procured (MW)",
            "Regulation Down Self-Provided (MW)",
            "Regulation Down Total (MW)",
            "Regulation Down Total Cost",
            "Regulation Mileage Down Procured (MW)",
            "Regulation Mileage Down Self-Provided (MW)",
            "Regulation Mileage Down Total (MW)",
            "Regulation Mileage Down Total Cost",
            "Regulation Mileage Up Procured (MW)",
            "Regulation Mileage Up Self-Provided (MW)",
            "Regulation Mileage Up Total (MW)",
            "Regulation Mileage Up Total Cost",
            "Regulation Up Procured (MW)",
            "Regulation Up Self-Provided (MW)",
            "Regulation Up Total (MW)",
            "Regulation Up Total Cost",
            "Spinning Reserves Procured (MW)",
            "Spinning Reserves Self-Provided (MW)",
            "Spinning Reserves Total (MW)",
            "Spinning Reserves Total Cost",
        ]
        assert df.columns.tolist() == columns
        assert df["Market"].unique()[0] == market
        assert df.shape[0] > 0

    """other"""

    def test_get_pnodes(self):
        df = self.iso.get_pnodes()
        assert df.shape[0] > 0
