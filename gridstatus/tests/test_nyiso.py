import pandas as pd
import pytest

import gridstatus
from gridstatus import NYISO, Markets
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets


class TestNYISO(BaseTestISO):
    iso = NYISO()

    """"get_capacity_prices"""

    def test_get_capacity_prices(self):
        # test 2022, 2023, and today
        df = self.iso.get_capacity_prices(date="Dec 1, 2022", verbose=True)
        assert not df.empty, "DataFrame came back empty"

        df = self.iso.get_capacity_prices(date="Jan 1, 2023", verbose=True)
        assert not df.empty, "DataFrame came back empty"

        df = self.iso.get_capacity_prices(date="today", verbose=True)
        assert not df.empty, "DataFrame came back empty"

    """get_fuel_mix"""

    def test_get_fuel_mix_date_range(self):
        df = self.iso.get_fuel_mix(start="Aug 1, 2022", end="Oct 22, 2022")
        assert df.shape[0] >= 0

    def test_range_two_days_across_month(self):
        today = gridstatus.utils._handle_date("today", self.iso.default_timezone)
        first_day_of_month = today.replace(day=1, hour=5, minute=0, second=0)
        last_day_of_prev_month = first_day_of_month - pd.Timedelta(days=1)
        df = self.iso.get_fuel_mix(start=last_day_of_prev_month, end=first_day_of_month)

        # Midnight of the end date
        assert df["Time"].max() == first_day_of_month.normalize() + pd.Timedelta(days=1)
        # First 5 minute interval of the start date
        assert df["Time"].min() == last_day_of_prev_month.normalize() + pd.Timedelta(
            minutes=5,
        )

        assert df["Time"].dt.date.nunique() == 3  # 2 days + 1 day for midnight
        self._check_fuel_mix(df)

    def test_month_start_multiple_months(self):
        start_date = pd.Timestamp("2022-01-01T06:00:00Z", tz=self.iso.default_timezone)
        end_date = pd.Timestamp("2022-03-01T06:00:00Z", tz=self.iso.default_timezone)

        df = self.iso.get_fuel_mix(start=start_date, end=end_date)

        # Midnight of the end date
        assert df["Time"].max() == end_date.replace(minute=0, hour=0) + pd.Timedelta(
            days=1,
        )
        # First 5 minute interval of the start date
        assert df["Time"].min() == start_date.replace(minute=5, hour=0)

        assert (df["Time"].dt.month.unique() == [1, 2, 3]).all()

        self._check_fuel_mix(df)

    """get_generators"""

    # todo
    @pytest.mark.skip(reason="Needs to be updated to 2023 data")
    def test_get_generators(self):
        df = self.iso.get_generators()
        columns = [
            "Generator Name",
            "PTID",
            "Subzone",
            "Zone",
            "Latitude",
            "Longitude",
        ]
        assert set(df.columns).issuperset(set(columns))
        assert df.shape[0] >= 0

    """get_load"""

    def test_get_load_contains_zones(self):
        df = self.iso.get_load("today")
        nyiso_load_cols = [
            "Time",
            "Load",
            "CAPITL",
            "CENTRL",
            "DUNWOD",
            "GENESE",
            "HUD VL",
            "LONGIL",
            "MHK VL",
            "MILLWD",
            "N.Y.C.",
            "NORTH",
            "WEST",
        ]
        assert df.columns.tolist() == nyiso_load_cols

    def test_get_load_month_range(self):
        df = self.iso.get_load(start="2023-04-01", end="2023-05-16")
        assert df.shape[0] >= 0

    """get_lmp"""

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
    )
    def test_lmp_date_range(self, market):
        super().test_lmp_date_range(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_historical(self, market):
        super().test_get_lmp_historical(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_today(self, market):
        super().test_get_lmp_today(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_latest(self, market):
        super().test_get_lmp_latest(market=market)

    def test_get_lmp_historical_with_range(self):
        start = "2021-12-01"
        end = "2022-2-02"
        df = self.iso.get_lmp(
            start=start,
            end=end,
            market=Markets.REAL_TIME_5_MIN,
        )
        assert df.shape[0] >= 0

    def test_get_lmp_location_type_parameter(self):
        date = "2022-06-09"

        df_zone = self.iso.get_lmp(
            date=date,
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="zone",
        )
        assert (df_zone["Location Type"] == "Zone").all()
        df_gen = self.iso.get_lmp(
            date=date,
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="generator",
        )
        assert (df_gen["Location Type"] == "Generator").all()

        df_zone = self.iso.get_lmp(
            date="today",
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="zone",
        )
        assert (df_zone["Location Type"] == "Zone").all()
        df_gen = self.iso.get_lmp(
            date="today",
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="generator",
        )
        assert (df_gen["Location Type"] == "Generator").all()

        df_zone = self.iso.get_lmp(
            date="latest",
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="zone",
        )
        assert (df_zone["Location Type"] == "Zone").all()
        df_gen = self.iso.get_lmp(
            date="latest",
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="generator",
        )
        assert (df_gen["Location Type"] == "Generator").all()

        with pytest.raises(ValueError):
            self.iso.get_lmp(
                date="latest",
                market=Markets.DAY_AHEAD_HOURLY,
                location_type="dummy",
            )

    """get_loads"""

    def test_get_loads(self):
        df = self.iso.get_loads()
        columns = [
            "Load Name",
            "PTID",
            "Subzone",
            "Zone",
        ]
        assert set(df.columns) == set(columns)
        assert df.shape[0] >= 0

    """get_status"""

    def test_get_status_historical_status(self):
        date = "20220609"
        status = self.iso.get_status(date)
        self._check_status(status)

        start = "2022-05-01"
        end = "2022-10-02"
        status = self.iso.get_status(start=start, end=end)
        self._check_status(status)

    """get_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

    def test_various_edt_to_est(self):
        # number of rows hardcoded based on when this test was written. should stay same

        date = "Nov 7, 2021"

        df = self.iso.get_status(date=date)
        assert df.shape[0] >= 1

        df = self.iso.get_fuel_mix(date=date)
        assert df.shape[0] >= 307

        df = self.iso.get_load_forecast(date=date)
        assert df.shape[0] >= 145
        df = self.iso.get_lmp(date=date, market=Markets.REAL_TIME_5_MIN)
        assert df.shape[0] >= 4605
        df = self.iso.get_lmp(date=date, market=Markets.DAY_AHEAD_HOURLY)
        assert df.shape[0] >= 375

        df = self.iso.get_load(date=date)
        assert df.shape[0] >= 307

    def test_various_est_to_edt(self):
        # number of rows hardcoded based on when this test was written. should stay same

        date = "March 14, 2021"

        df = self.iso.get_status(date=date)
        assert df.shape[0] >= 5

        df = self.iso.get_lmp(date=date, market=Markets.REAL_TIME_5_MIN)
        assert df.shape[0] >= 4215

        df = self.iso.get_lmp(date=date, market=Markets.DAY_AHEAD_HOURLY)
        assert df.shape[0] >= 345

        df = self.iso.get_load_forecast(date=date)
        assert df.shape[0] >= 143

        df = self.iso.get_fuel_mix(date=date)
        assert df.shape[0] >= 281

        df = self.iso.get_load(date=date)
        assert df.shape[0] >= 281

    # test btm solar
    def test_get_btm_solar(self):
        # published ~8 hours after finish of previous day
        two_days_ago = pd.Timestamp.now(tz="US/Eastern").date() - pd.Timedelta(days=2)
        df = self.iso.get_btm_solar(
            date=two_days_ago,
            verbose=True,
        )

        columns = [
            "Time",
            "Interval Start",
            "Interval End",
            "SYSTEM",
            "CAPITL",
            "CENTRL",
            "DUNWOD",
            "GENESE",
            "HUD VL",
            "LONGIL",
            "MHK VL",
            "MILLWD",
            "N.Y.C.",
            "NORTH",
            "WEST",
        ]

        assert df.columns.tolist() == columns
        assert df.shape[0] >= 0

        # test range last month
        start = "2023-04-30"
        end = "2023-05-02"
        df = self.iso.get_btm_solar(
            start=start,
            end=end,
            verbose=True,
        )

        assert df["Time"].dt.date.nunique() == 3

    def test_get_btm_solar_forecast(self):
        df = self.iso.get_btm_solar_forecast(
            date="today",
            verbose=True,
        )

        columns = [
            "Time",
            "Interval Start",
            "Interval End",
            "SYSTEM",
            "CAPITL",
            "CENTRL",
            "DUNWOD",
            "GENESE",
            "HUD VL",
            "LONGIL",
            "MHK VL",
            "MILLWD",
            "N.Y.C.",
            "NORTH",
            "WEST",
        ]

        assert df.columns.tolist() == columns
        assert df.shape[0] >= 0

        # test range last month
        start = "2023-04-30"
        end = "2023-05-02"
        df = self.iso.get_btm_solar_forecast(
            start=start,
            end=end,
            verbose=True,
        )

        assert df["Time"].dt.date.nunique() == 3

    @staticmethod
    def _check_status(df):
        assert set(df.columns) == set(
            ["Time", "Status", "Notes"],
        )
