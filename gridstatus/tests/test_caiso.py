import math

import pandas as pd
import pytest

from gridstatus import CAISO, Markets
from gridstatus.base import NoDataFoundException
from gridstatus.caiso import (
    REAL_TIME_DISPATCH_MARKET_RUN_ID,
)
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets


class TestCAISO(BaseTestISO):
    iso = CAISO()

    trading_hub_locations = iso.trading_hub_locations

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

    """get_fuel_mix"""

    def test_fuel_mix_across_dst_transition(self):
        # these dates are across the DST transition
        # and caused a bug in the past
        date = (
            pd.Timestamp("2023-11-05 09:55:00+0000", tz="UTC"),
            pd.Timestamp("2023-11-05 20:49:26.038069+0000", tz="UTC"),
        )
        df = self.iso.get_fuel_mix(date=date)
        self._check_fuel_mix(df)

    """get_load_forecast"""

    def test_get_load_forecast_publish_time(self):
        df = self.iso.get_load_forecast("today")

        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Load Forecast",
        ]

        assert df["Publish Time"].nunique() == 1
        assert df["Publish Time"].max() < self.local_now()

    """get_solar_and_wind_forecast_dam"""

    def _check_solar_and_wind_forecast(self, df, expected_count_unique_publish_times):
        assert df.shape[0] > 0

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Location",
            "Solar MW",
            "Wind MW",
        ]

        assert df["Location"].unique().tolist() == ["CAISO", "NP15", "SP15", "ZP26"]

        totals = df.loc[df["Location"] == "CAISO"]
        non_totals = df.loc[df["Location"] != "CAISO"]

        assert math.isclose(
            totals["Solar MW"].sum(),
            non_totals["Solar MW"].sum(),
            rel_tol=0.01,
        )

        assert math.isclose(
            totals["Wind MW"].sum(),
            non_totals["Wind MW"].sum(),
            rel_tol=0.01,
        )

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

        # Make sure there are no future publish times
        assert df["Publish Time"].max() < self.local_now()
        assert df["Publish Time"].nunique() == expected_count_unique_publish_times

    def test_get_solar_and_wind_forecast_dam_today(self):
        df = self.iso.get_solar_and_wind_forecast_dam("today")
        self._check_solar_and_wind_forecast(df, 1)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval Start"].max() == self.local_start_of_today() + pd.Timedelta(
            hours=23,
        )

    def test_get_solar_and_wind_forecast_dam_latest(self):
        assert self.iso.get_solar_and_wind_forecast_dam("latest").equals(
            self.iso.get_solar_and_wind_forecast_dam("today"),
        )

    def test_get_solar_and_wind_forecast_dam_historical_date(self):
        df = self.iso.get_solar_and_wind_forecast_dam("2024-02-20")
        self._check_solar_and_wind_forecast(df, 1)

        assert df["Interval Start"].min() == self.local_start_of_day("2024-02-20")
        assert df["Interval Start"].max() == self.local_start_of_day(
            "2024-02-20",
        ) + pd.Timedelta(hours=23)

    def test_get_solar_and_wind_forecast_dam_historical_range(self):
        start = pd.Timestamp("2023-08-15")
        end = pd.Timestamp("2023-08-21")

        df = self.iso.get_solar_and_wind_forecast_dam(start, end=end)

        # Only 6 days of data because the end date is exclusive
        self._check_solar_and_wind_forecast(df, 6)

        assert df["Interval Start"].min() == self.local_start_of_day(start)
        assert df["Interval Start"].max() == self.local_start_of_day(
            end,
        ) - pd.Timedelta(hours=1)

    def test_get_solar_and_wind_forecast_dam_future_date_range(self):
        start = self.local_today() + pd.Timedelta(days=1)
        end = start + pd.Timedelta(days=2)

        df = self.iso.get_solar_and_wind_forecast_dam(start, end=end)

        self._check_solar_and_wind_forecast(df, 1)

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

    """get_fuel_regions"""

    def test_get_fuel_regions(self):
        df = self.iso.get_fuel_regions()
        assert df.columns.tolist() == [
            "Fuel Region Id",
            "Pricing Hub",
            "Transportation Cost",
            "Fuel Reimbursement Rate",
            "Cap and Trade Credit",
            "Miscellaneous Costs",
            "Balancing Authority",
        ]
        assert df.shape[0] > 180

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
    )
    def test_lmp_date_range(self, market):
        super().test_lmp_date_range(market=market)

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

    def test_get_lmp_with_locations_range_dam(self):
        end = pd.Timestamp("today").normalize()
        start = end - pd.Timedelta(days=3)
        locations = self.iso.trading_hub_locations
        df = self.iso.get_lmp(
            start=start,
            end=end,
            locations=locations,
            market="DAY_AHEAD_HOURLY",
        )
        # assert all days are present
        assert df["Location"].nunique() == len(locations)

    # all nodes having problems
    # also not working on oasis web portal
    # as of may 11, 2023
    # def test_get_lmp_all_locations_dam(self):
    #     yesterday = pd.Timestamp("today").normalize() - pd.Timedelta(days=1)
    #     df = self.iso.get_lmp(
    #         date=yesterday,
    #         locations="ALL",
    #         market="DAY_AHEAD_HOURLY",
    #         verbose=True,
    #     )
    #     # assert approx 16000 locations
    #     assert df["Location"].nunique() > 16000

    def test_get_lmp_all_ap_nodes_locations(self):
        yesterday = pd.Timestamp("today").normalize() - pd.Timedelta(days=1)
        df = self.iso.get_lmp(
            date=yesterday,
            locations="ALL_AP_NODES",
            market="DAY_AHEAD_HOURLY",
        )
        # assert approx 2300 locations
        assert df["Location"].nunique() > 2300

    def test_get_lmp_with_all_locations_range(self):
        end = pd.Timestamp("today").tz_localize(
            self.iso.default_timezone,
        ).normalize() - pd.Timedelta(days=2)
        start = end - pd.Timedelta(days=3)
        df = self.iso.get_lmp(
            start=start,
            end=end,
            locations="ALL_AP_NODES",
            market="DAY_AHEAD_HOURLY",
        )
        # assert all days are present
        assert df["Time"].dt.date.nunique() == 3

    def test_get_lmp_all_locations_real_time_2_hour(self):
        # test two hours
        start = pd.Timestamp("now").tz_localize("UTC").normalize() - pd.Timedelta(
            days=1,
        )
        end = start + pd.Timedelta(hours=2)
        df = self.iso.get_lmp(
            start=start,
            end=end,
            locations="ALL_AP_NODES",
            market="REAL_TIME_15_MIN",
            verbose=True,
        )
        # assert approx 2300 locations
        assert df["Location"].nunique() > 2300
        assert df["Interval Start"].dt.hour.nunique() == 2

    def test_get_lmp_too_far_in_past_raises_custom_exception(self):
        too_old_date = pd.Timestamp.now().date() - pd.Timedelta(days=1201)

        with pytest.raises(NoDataFoundException):
            self.iso.get_lmp(
                date=too_old_date,
                locations=self.trading_hub_locations,
                market="REAL_TIME_15_MIN",
            )

        valid_date = pd.Timestamp.now().date() - pd.Timedelta(days=1000)

        df = self.iso.get_lmp(
            date=valid_date,
            locations=self.trading_hub_locations,
            market="REAL_TIME_15_MIN",
        )

        assert not df.empty

    def test_warning_no_end_date(self):
        start = pd.Timestamp("2021-04-01T03:00").tz_localize("UTC")
        with (
            pytest.warns(
                UserWarning,
                match="Only 1 hour of data will be returned for real time markets if end is not specified and all nodes are requested",  # noqa
            ),
        ):
            try:
                self.iso.get_lmp(
                    start=start,
                    locations="ALL_AP_NODES",
                    market="REAL_TIME_15_MIN",
                )
            except NoDataFoundException:
                pass

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

    def test_get_curtailed_non_operational_generator_report(self):
        columns = [
            "Publish Time",
            "Outage MRID",
            "Resource Name",
            "Resource ID",
            "Outage Type",
            "Nature of Work",
            "Curtailment Start Time",
            "Curtailment End Time",
            "Curtailment MW",
            "Resource PMAX MW",
            "Net Qualifying Capacity MW",
        ]

        start_of_data = pd.Timestamp("2021-06-17")
        df = self.iso.get_curtailed_non_operational_generator_report(
            date=start_of_data,
        )
        assert df.shape[0] > 0
        assert df.columns.tolist() == columns

        two_days_ago = pd.Timestamp("today") - pd.Timedelta(days=2)
        df = self.iso.get_curtailed_non_operational_generator_report(
            date=two_days_ago.normalize(),
        )
        assert df.shape[0] > 0
        assert df.columns.tolist() == columns

        date_with_duplicates = pd.Timestamp("2021-11-07")
        df = self.iso.get_curtailed_non_operational_generator_report(
            date=date_with_duplicates,
        )
        assert df.shape[0] > 0
        assert df.columns.tolist() == columns

        # errors for a date before 2021-06-17
        with pytest.raises(ValueError):
            self.iso.get_curtailed_non_operational_generator_report(
                date="2021-06-16",
            )

        assert df.shape[0] > 0

    """get_tie_flows_real_time"""

    def _check_tie_flows_real_time(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Interface ID",
            "Tie Name",
            "From BAA",
            "To BAA",
            "Market",
            "MW",
        ]

        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            minutes=5,
        )

        assert df["Market"].unique() == REAL_TIME_DISPATCH_MARKET_RUN_ID

        assert not df.duplicated(
            subset=["Interval Start", "Tie Name", "From BAA", "To BAA"],
        ).any()

    def test_get_tie_flows_real_time_latest(self):
        df = self.iso.get_tie_flows_real_time("latest")
        self._check_tie_flows_real_time(df)

        assert df["Interval Start"].min() == pd.Timestamp.utcnow().round("5min")
        assert df["Interval End"].max() == pd.Timestamp.utcnow().round(
            "5min",
        ) + pd.Timedelta(minutes=5)

    def test_get_tie_flows_real_time_today(self):
        df = self.iso.get_tie_flows_real_time("today")
        self._check_tie_flows_real_time(df)

        assert df["Interval Start"].min() == self.local_start_of_today()

    def test_get_tie_flows_real_time_historical_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=100)
        end = start + pd.DateOffset(days=2)

        df = self.iso.get_tie_flows_real_time(start, end=end)
        self._check_tie_flows_real_time(df)

        assert df["Interval Start"].min() == start
        assert df["Interval End"].max() == end

    """other"""

    def test_oasis_no_data(self):
        df = self.iso.get_oasis_dataset(
            dataset="as_clearing_prices",
            date=pd.Timestamp.now() + pd.Timedelta(days=7),
        )

        assert df.empty

    def test_get_pnodes(self):
        df = self.iso.get_pnodes()
        assert df.shape[0] > 0
