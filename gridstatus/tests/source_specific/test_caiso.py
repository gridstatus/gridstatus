import math

import pandas as pd
import pytest

from gridstatus import CAISO, Markets
from gridstatus.base import NoDataFoundException
from gridstatus.caiso.caiso_constants import REAL_TIME_DISPATCH_MARKET_RUN_ID
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

caiso_vcr = setup_vcr(
    source="caiso",
    record_mode=RECORD_MODE,
)


class TestCAISO(BaseTestISO):
    iso = CAISO()

    trading_hub_locations = CAISO().trading_hub_locations

    """get_as"""

    @pytest.mark.parametrize("date", ["2022-10-15", "2022-10-16"])
    def test_get_as_prices(self, date):
        with caiso_vcr.use_cassette(f"test_get_as_prices_{date}.yaml"):
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

    @pytest.mark.parametrize("date", ["2022-10-15", "2022-10-16"])
    def test_get_as_procurement(self, date):
        with caiso_vcr.use_cassette(f"test_get_as_procurement_{date}.yaml"):
            for market in ["DAM", "RTM"]:
                df = self.iso.get_as_procurement(date, market=market)
                self._check_as_data(df, market)

    """get_fuel_mix"""

    # NOTE: these dates are across the DST transition which caused a bug in the past
    @pytest.mark.parametrize(
        "date",
        [
            (
                pd.Timestamp("2023-11-05 09:55:00+0000", tz="UTC"),
                pd.Timestamp("2023-11-05 20:49:26.038069+0000", tz="UTC"),
            ),
        ],
    )
    def test_fuel_mix_across_dst_transition(self, date):
        with caiso_vcr.use_cassette(
            f"test_fuel_mix_dst_transition_{date[0].strftime('%Y-%m-%d')}.yaml",
            match_on=["method", "scheme", "host", "port", "path"],
        ):
            df = self.iso.get_fuel_mix(date=date)
            self._check_fuel_mix(df)

    """get_load_forecast"""

    def _check_load_forecast(
        self,
        df: pd.DataFrame,
        expected_interval_minutes: int | None = None,
    ):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "TAC Area Name",
            "Load Forecast",
        ]

        if expected_interval_minutes:
            interval_minutes = (
                df["Interval End"] - df["Interval Start"]
            ).dt.total_seconds() / 60
            assert (interval_minutes == expected_interval_minutes).all()

        assert df["Publish Time"].max() < self.local_now()

    @pytest.mark.parametrize(
        "date, end",
        [
            (
                pd.Timestamp.today(tz=iso.default_timezone).normalize()
                - pd.Timedelta(days=5),
                pd.Timestamp.today(tz=iso.default_timezone).normalize()
                - pd.Timedelta(days=2),
            ),
        ],
    )
    def test_get_load_forecast_15_min_date_range(self, date, end):
        with caiso_vcr.use_cassette(
            f"test_get_load_forecast_15_min_range_{date.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_load_forecast_15_min(date, end=end)
            self._check_load_forecast(df, expected_interval_minutes=15)

    @pytest.mark.parametrize(
        "date, end",
        [
            (
                pd.Timestamp.today(tz=iso.default_timezone).normalize()
                - pd.Timedelta(days=5),
                pd.Timestamp.today(tz=iso.default_timezone).normalize()
                - pd.Timedelta(days=2),
            ),
        ],
    )
    def test_get_load_forecast_5_min_date_range(self, date, end):
        with caiso_vcr.use_cassette(
            f"test_get_load_forecast_5_min_range_{date.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_load_forecast_5_min(date, end=end)
            self._check_load_forecast(df, expected_interval_minutes=5)

    @pytest.mark.parametrize(
        "date, end",
        [
            (
                pd.Timestamp.today(tz=iso.default_timezone).normalize()
                - pd.Timedelta(days=3),
                pd.Timestamp.today(tz=iso.default_timezone).normalize()
                - pd.Timedelta(days=1),
            ),
        ],
    )
    def test_get_load_forecast_day_ahead_date_range(self, date, end):
        with caiso_vcr.use_cassette(
            f"test_get_load_forecast_day_ahead_range_{date.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_load_forecast_day_ahead(date, end=end)
            self._check_load_forecast(df, expected_interval_minutes=60)

    @pytest.mark.parametrize(
        "date, end",
        [
            (
                pd.Timestamp.today(tz=iso.default_timezone).normalize()
                - pd.Timedelta(days=3),
                pd.Timestamp.today(tz=iso.default_timezone).normalize()
                - pd.Timedelta(days=1),
            ),
        ],
    )
    def test_get_load_forecast_two_day_ahead_date_range(self, date, end):
        with caiso_vcr.use_cassette(
            f"test_get_load_forecast_two_day_ahead_range_{date.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_load_forecast_two_day_ahead(date, end=end)
            self._check_load_forecast(df, expected_interval_minutes=60)

    @pytest.mark.parametrize(
        "date, end",
        [
            (
                pd.Timestamp.today(tz=iso.default_timezone).normalize()
                - pd.Timedelta(days=3),
                pd.Timestamp.today(tz=iso.default_timezone).normalize()
                - pd.Timedelta(days=1),
            ),
        ],
    )
    def test_get_load_forecast_seven_day_ahead_date_range(self, date, end):
        with caiso_vcr.use_cassette(
            f"test_get_load_forecast_seven_day_ahead_range_{date.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_load_forecast_seven_day_ahead(date, end=end)
            self._check_load_forecast(df, expected_interval_minutes=60)

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

    def test_get_renewables_forecast_dam_today(self):
        with caiso_vcr.use_cassette(
            "test_get_renewables_forecast_dam_today.yaml",
        ):
            df = self.iso.get_renewables_forecast_dam("today")
            self._check_solar_and_wind_forecast(df, 1)

            assert df["Interval Start"].min() == self.local_start_of_today()
            assert df[
                "Interval Start"
            ].max() == self.local_start_of_today() + pd.Timedelta(
                hours=23,
            )

    def test_get_renewables_forecast_dam_latest(self):
        with caiso_vcr.use_cassette(
            "test_get_renewables_forecast_dam_latest.yaml",
        ):
            assert self.iso.get_renewables_forecast_dam("latest").equals(
                self.iso.get_renewables_forecast_dam("today"),
            )

    @pytest.mark.parametrize("date", ["2024-02-20"])
    def test_get_renewables_forecast_dam_historical_date(self, date):
        with caiso_vcr.use_cassette(
            f"test_get_renewables_forecast_dam_{date}.yaml",
        ):
            df = self.iso.get_renewables_forecast_dam(date)
            self._check_solar_and_wind_forecast(df, 1)

            assert df["Interval Start"].min() == self.local_start_of_day(date)
            assert df["Interval Start"].max() == self.local_start_of_day(
                date,
            ) + pd.Timedelta(hours=23)

    @pytest.mark.parametrize("start, end", [("2023-08-15", "2023-08-21")])
    def test_get_renewables_forecast_dam_historical_range(self, start, end):
        with caiso_vcr.use_cassette(
            f"test_get_renewables_forecast_dam_{start}_{end}.yaml",
        ):
            start = pd.Timestamp(start)
            end = pd.Timestamp(end)

            df = self.iso.get_renewables_forecast_dam(start, end=end)

            # Only 6 days of data because the end date is exclusive
            self._check_solar_and_wind_forecast(df, 6)

            assert df["Interval Start"].min() == self.local_start_of_day(start)
            assert df["Interval Start"].max() == self.local_start_of_day(
                end,
            ) - pd.Timedelta(hours=1)

    def test_get_renewables_forecast_dam_future_date_range(self):
        with caiso_vcr.use_cassette(
            "test_get_renewables_forecast_dam_future_date_range.yaml",
        ):
            start = self.local_today() + pd.Timedelta(days=1)
            end = start + pd.Timedelta(days=2)

            df = self.iso.get_renewables_forecast_dam(start, end=end)

            self._check_solar_and_wind_forecast(df, 1)

    def test_get_renewables_forecast_hasp_latest(self):
        with caiso_vcr.use_cassette(
            "test_get_renewables_forecast_hasp_latest.yaml",
        ):
            df = self.iso.get_renewables_forecast_hasp("latest")
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Location",
                "Solar",
                "Wind",
            ]
            assert (
                (df["Interval Start"] - df["Publish Time"]) == pd.Timedelta(minutes=90)
            ).all()

    @pytest.mark.parametrize(
        "date, end",
        [
            (
                "2025-03-20",
                "2025-03-22",
            ),
        ],
    )
    def test_get_renewables_forecast_hasp_date_range(self, date, end):
        with caiso_vcr.use_cassette(
            f"test_get_renewables_forecast_hasp_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_renewables_forecast_hasp(date, end=end)
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Location",
                "Solar",
                "Wind",
            ]
            assert df["Interval Start"].min() >= pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            )
            assert (
                (df["Interval Start"] - df["Publish Time"]) == pd.Timedelta(minutes=90)
            ).all()

    def test_get_renewables_hourly_latest(self):
        with caiso_vcr.use_cassette(
            "test_get_renewables_hourly_latest.yaml",
        ):
            df = self.iso.get_renewables_hourly("latest")
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location",
                "Solar",
                "Wind",
            ]
            assert df["Interval Start"].min() >= self.local_start_of_today()

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2025-03-20", "2025-03-22"),
        ],
    )
    def test_get_renewables_hourly_date_range(self, date, end):
        with caiso_vcr.use_cassette(
            f"test_get_renewables_hourly_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_renewables_hourly(date, end=end)
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location",
                "Solar",
                "Wind",
            ]
            assert df["Interval Start"].min() >= pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            )

    def test_get_renewables_forecast_rtd_latest(self):
        with caiso_vcr.use_cassette(
            "test_get_renewables_forecast_rtd_latest.yaml",
        ):
            df = self.iso.get_renewables_forecast_rtd("latest")
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Location",
                "Solar",
                "Wind",
            ]
            assert df["Interval Start"].min() >= self.local_start_of_today()

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2025-03-20", "2025-03-22"),
        ],
    )
    def test_get_renewables_forecast_rtd_date_range(self, date, end):
        with caiso_vcr.use_cassette(
            f"test_get_renewables_forecast_rtd_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_renewables_forecast_rtd(date, end=end)
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Location",
                "Solar",
                "Wind",
            ]
            assert df["Interval Start"].min() >= pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            )

    def test_get_renewables_forecast_rtpd_latest(self):
        with caiso_vcr.use_cassette(
            "test_get_renewables_forecast_rtpd_latest.yaml",
        ):
            df = self.iso.get_renewables_forecast_rtpd("latest")
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Location",
                "Solar",
                "Wind",
            ]
            assert df["Interval Start"].min() >= self.local_start_of_today()

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2025-03-20", "2025-03-22"),
        ],
    )
    def test_get_renewables_forecast_rtpd_date_range(self, date, end):
        with caiso_vcr.use_cassette(
            f"test_get_renewables_forecast_rtpd_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_renewables_forecast_rtpd(date, end=end)
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Location",
                "Solar",
                "Wind",
            ]
            assert df["Interval Start"].min() >= pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            )

    """get_curtailment_legacy"""

    def _check_curtailment_legacy(self, df: pd.DataFrame):
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

    @pytest.mark.parametrize("date", ["2022-10-15"])
    def test_get_curtailment_legacy(self, date):
        with caiso_vcr.use_cassette(f"test_get_curtailment_legacy_{date}.yaml"):
            df = self.iso.get_curtailment_legacy(date)
            assert df.shape == (31, 8)
            self._check_curtailment_legacy(df)

    @pytest.mark.parametrize("date", ["2022-03-15"])
    def test_get_curtailment_legacy_2_pages(self, date):
        # test that the function can handle 2 pages of data
        with caiso_vcr.use_cassette(f"test_get_curtailment_legacy_2_pages_{date}.yaml"):
            df = self.iso.get_curtailment_legacy(date)
            assert df.shape == (55, 8)
            self._check_curtailment_legacy(df)

    @pytest.mark.parametrize("date", ["2022-03-16"])
    def test_get_curtailment_legacy_3_pages(self, date):
        # test that the function can handle 3 pages of data
        with caiso_vcr.use_cassette(f"test_get_curtailment_legacy_3_pages_{date}.yaml"):
            df = self.iso.get_curtailment_legacy(date)
            assert df.shape == (76, 8)
            self._check_curtailment_legacy(df)

    @pytest.mark.parametrize("date", ["2021-12-02", "2025-01-02"])
    def test_get_curtailment_legacy_special_dates(self, date):
        with caiso_vcr.use_cassette(
            f"test_get_curtailment_legacy_special_dates_{date}.yaml",
        ):
            df = self.iso.get_curtailment_legacy(date)
            self._check_curtailment_legacy(df)

    """get_curtailment"""

    def _check_curtailment(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Curtailment Type",
            "Curtailment Reason",
            "Fuel Type",
            "Curtailment MWH",
            "Curtailment MW",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

    def test_get_curtailment_specific_date(self):
        date = self.local_today() - pd.DateOffset(days=2)
        with caiso_vcr.use_cassette(f"test_get_curtailment_{date}.yaml"):
            df = self.iso.get_curtailment(date)

        self._check_curtailment(df)

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval Start"].max() == self.local_start_of_day(
            date,
        ) + pd.Timedelta(hours=23)

    def test_get_curtailment_date_range(self):
        start_date = self.local_start_of_today() - pd.DateOffset(days=5)
        end_date = start_date + pd.DateOffset(days=3)

        with caiso_vcr.use_cassette(
            f"test_get_curtailment_date_range_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_curtailment(start_date, end=end_date)

        self._check_curtailment(df)

        assert df["Interval Start"].min() == start_date
        assert df["Interval Start"].max() == end_date - pd.Timedelta(hours=1)

    # Some of the data structure changes in July 2025, so add tests making sure we
    # can cover existing data
    def test_get_curtailment_june_and_july_2025(self):
        start_date = pd.Timestamp("2025-06-29", tz=self.iso.default_timezone)
        end_date = pd.Timestamp("2025-07-02", tz=self.iso.default_timezone)

        with caiso_vcr.use_cassette(
            f"test_get_curtailment_june_and_july_2025_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_curtailment(start_date, end=end_date)

        self._check_curtailment(df)

        assert df["Interval Start"].min() == start_date
        assert df["Interval Start"].max() == end_date - pd.Timedelta(hours=1)

    """get_gas_prices"""

    @pytest.mark.parametrize("date", ["2022-10-15"])
    def test_get_gas_prices(self, date):
        with caiso_vcr.use_cassette(f"test_get_gas_prices_{date}.yaml"):
            # no fuel region
            df = self.iso.get_gas_prices(date=date)

            n_unique = 153
            assert df["Fuel Region Id"].nunique() == n_unique
            assert len(df) == n_unique * 24

    @pytest.mark.parametrize("date", ["2022-10-15"])
    def test_get_gas_prices_single_fuel_region(self, date):
        with caiso_vcr.use_cassette(
            f"test_get_gas_prices_single_fuel_region_{date}.yaml",
        ):
            test_region_1 = "FRPGE2GHG"
            df = self.iso.get_gas_prices(date=date, fuel_region_id=test_region_1)
            assert df["Fuel Region Id"].unique()[0] == test_region_1
            assert len(df) == 24

    @pytest.mark.parametrize("date", ["2022-10-15"])
    def test_get_gas_prices_list_of_fuel_regions(self, date):
        with caiso_vcr.use_cassette(
            f"test_get_gas_prices_list_of_fuel_regions_{date}.yaml",
        ):
            test_region_1 = "FRPGE2GHG"
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
        with caiso_vcr.use_cassette("test_get_fuel_regions.yaml"):
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

    @pytest.mark.parametrize("date", ["2022-10-15"])
    def test_get_ghg_allowance(self, date):
        with caiso_vcr.use_cassette(f"test_get_ghg_allowance_{date}.yaml"):
            df = self.iso.get_ghg_allowance(date)

            assert len(df) == 1
            assert df.columns.tolist() == [
                "Time",
                "Interval Start",
                "Interval End",
                "GHG Allowance Price",
            ]

    """get_lmp"""

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

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
    )
    def test_lmp_date_range(self, market):
        with caiso_vcr.use_cassette(f"test_lmp_date_range_{market.value.lower()}.yaml"):
            super().test_lmp_date_range(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_15_MIN,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_historical(self, market):
        with caiso_vcr.use_cassette(
            f"test_get_lmp_historical_{market.value.lower()}.yaml",
        ):
            super().test_get_lmp_historical(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_15_MIN,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_latest(self, market):
        with caiso_vcr.use_cassette(f"test_get_lmp_latest_{market.value.lower()}.yaml"):
            super().test_get_lmp_latest(market=market)

    @pytest.mark.parametrize("date", ["today"])
    def test_get_lmp_locations_must_be_list(self, date):
        with caiso_vcr.use_cassette(f"test_get_lmp_locations_must_be_list_{date}.yaml"):
            with pytest.raises(AssertionError):
                self.iso.get_lmp(date, locations="foo", market="REAL_TIME_5_MIN")

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_15_MIN,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_today(self, market):
        with caiso_vcr.use_cassette(f"test_get_lmp_today_{market.value.lower()}.yaml"):
            super().test_get_lmp_today(market=market)

    @pytest.mark.parametrize(
        "date, end",
        [
            (
                pd.Timestamp("today").normalize() - pd.Timedelta(days=3),
                pd.Timestamp("today").normalize(),
            ),
        ],
    )
    def test_get_lmp_with_locations_range_dam(self, date, end):
        with caiso_vcr.use_cassette(
            f"test_get_lmp_with_locations_range_dam_{date.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            locations = self.iso.trading_hub_locations
            df = self.iso.get_lmp(
                start=date,
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

    @pytest.mark.parametrize(
        "date",
        [pd.Timestamp("today").normalize() - pd.Timedelta(days=1)],
    )
    def test_get_lmp_all_ap_nodes_locations(self, date):
        with caiso_vcr.use_cassette(
            f"test_get_lmp_all_ap_nodes_locations_{date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_lmp(
                date=date,
                locations="ALL_AP_NODES",
                market="DAY_AHEAD_HOURLY",
            )
            # assert approx 2300 locations
            assert df["Location"].nunique() > 2300

    # NOTE(kladar): can't use self.iso.default_timezone because decorator is created before class is initialized
    @pytest.mark.parametrize(
        "end",
        [
            pd.Timestamp("today").tz_localize("US/Pacific").normalize()
            - pd.Timedelta(days=2),
        ],
    )
    def test_get_lmp_with_all_locations_range(self, end: pd.Timestamp) -> None:
        start = end - pd.Timedelta(days=3)
        with caiso_vcr.use_cassette(
            f"test_get_lmp_with_all_locations_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_lmp(
                start=start,
                end=end,
                locations="ALL_AP_NODES",
                market="DAY_AHEAD_HOURLY",
            )
            # assert all days are present
            assert df["Time"].dt.date.nunique() == 3

    @pytest.mark.parametrize(
        "start, end",
        [
            (
                pd.Timestamp("now").tz_localize("UTC").normalize()
                - pd.Timedelta(days=1),
                pd.Timestamp("now").tz_localize("UTC").normalize()
                - pd.Timedelta(days=1)
                + pd.Timedelta(hours=2),
            ),
        ],
    )
    def test_get_lmp_all_locations_real_time_2_hour(self, start, end):
        with caiso_vcr.use_cassette(
            f"test_get_lmp_all_locations_real_time_2_hour_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            # test two hours
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

    @pytest.mark.parametrize(
        "date",
        [pd.Timestamp.now().date() - pd.Timedelta(days=1201)],
    )
    def test_get_lmp_too_far_in_past_raises_custom_exception(self, date):
        with caiso_vcr.use_cassette(
            f"test_get_lmp_too_far_in_past_raises_custom_exception_{date}.yaml",
        ):
            with pytest.raises(NoDataFoundException):
                self.iso.get_lmp(
                    date=date,
                    locations=self.trading_hub_locations,
                    market="REAL_TIME_15_MIN",
                )

    @pytest.mark.parametrize(
        "date",
        [pd.Timestamp.now().date() - pd.Timedelta(days=1000)],
    )
    def test_get_lmp_valid_date(self, date):
        with caiso_vcr.use_cassette(f"test_get_lmp_valid_date_{date}.yaml"):
            df = self.iso.get_lmp(
                date=date,
                locations=self.trading_hub_locations,
                market="REAL_TIME_15_MIN",
            )

        assert not df.empty

    @pytest.mark.parametrize(
        "start",
        [pd.Timestamp("2021-04-01T03:00").tz_localize("UTC")],
    )
    def test_warning_no_end_date(self, start):
        with caiso_vcr.use_cassette(
            f"test_warning_no_end_date_{start.strftime('%Y-%m-%d')}.yaml",
        ):
            with pytest.warns(
                UserWarning,
                match="Only 1 hour of data will be returned for real time markets if end is not specified and all nodes are requested",  # noqa
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
    def _check_as_data(df: pd.DataFrame, market: str) -> None:
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

    CURTAILED_GENERATOR_COLUMNS = [
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

    @pytest.mark.parametrize("date", ["2021-06-17"])
    def test_get_curtailed_non_operational_generator_report(self, date):
        with caiso_vcr.use_cassette(
            f"test_get_curtailed_non_operational_generator_report_{date}.yaml",
        ):
            df = self.iso.get_curtailed_non_operational_generator_report(
                date=date,
            )
            assert df.shape[0] > 0
            assert df.columns.tolist() == self.CURTAILED_GENERATOR_COLUMNS

    @pytest.mark.parametrize("date", [pd.Timestamp("today") - pd.Timedelta(days=2)])
    def test_get_curtailed_non_operational_generator_report_two_days_ago(self, date):
        with caiso_vcr.use_cassette(
            f"test_get_curtailed_non_operational_generator_report_two_days_ago_{date}.yaml",
        ):
            df = self.iso.get_curtailed_non_operational_generator_report(
                date=date,
            )
            assert df.shape[0] > 0
            assert df.columns.tolist() == self.CURTAILED_GENERATOR_COLUMNS

    @pytest.mark.parametrize("date", ["2021-11-07"])
    def test_get_curtailed_non_operational_generator_report_duplicates(self, date):
        with caiso_vcr.use_cassette(
            f"test_get_curtailed_non_operational_generator_report_duplicates_{date}.yaml",
        ):
            df = self.iso.get_curtailed_non_operational_generator_report(
                date=date,
            )
            assert df.shape[0] > 0
            assert df.columns.tolist() == self.CURTAILED_GENERATOR_COLUMNS

    @pytest.mark.parametrize("date", ["2021-06-16"])
    def test_get_curtailed_non_operational_generator_report_before_2021_06_17(
        self,
        date,
    ):
        with caiso_vcr.use_cassette(
            f"test_get_curtailed_non_operational_generator_report_before_{date}.yaml",
        ):
            # errors for a date before 2021-06-17
            with pytest.raises(ValueError):
                df = self.iso.get_curtailed_non_operational_generator_report(
                    date=date,
                )

                assert df.shape[0] > 0

        # Change in url format on this date
        date_with_new_format = pd.Timestamp("2025-01-13")
        df = self.iso.get_curtailed_non_operational_generator_report(
            date=date_with_new_format,
        )
        assert df.shape[0] > 0
        assert df.columns.tolist() == self.CURTAILED_GENERATOR_COLUMNS

    """get_tie_flows_real_time"""

    def _check_tie_flows_real_time(self, df: pd.DataFrame):
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
        with caiso_vcr.use_cassette("test_get_tie_flows_real_time_latest.yaml"):
            df = self.iso.get_tie_flows_real_time("latest")
            self._check_tie_flows_real_time(df)

            assert df["Interval Start"].min() == pd.Timestamp.utcnow().round("5min")
            assert df["Interval End"].max() == pd.Timestamp.utcnow().round(
                "5min",
            ) + pd.Timedelta(minutes=5)

    def test_get_tie_flows_real_time_today(self):
        with caiso_vcr.use_cassette("test_get_tie_flows_real_time_today.yaml"):
            df = self.iso.get_tie_flows_real_time("today")
            self._check_tie_flows_real_time(df)

            assert df["Interval Start"].min() == self.local_start_of_today()

    def test_get_tie_flows_real_time_historical_date_range(self):
        start_of_local_today = self.local_start_of_today()
        start = start_of_local_today - pd.DateOffset(days=100)
        end = start + pd.DateOffset(days=2)
        with caiso_vcr.use_cassette(
            f"test_get_tie_flows_real_time_historical_date_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_tie_flows_real_time(start, end=end)
            self._check_tie_flows_real_time(df)

            assert df["Interval Start"].min() == start
            assert df["Interval End"].max() == end

    """other"""

    @pytest.mark.parametrize(
        "dataset, date",
        [("as_clearing_prices", pd.Timestamp.now() + pd.Timedelta(days=7))],
    )
    def test_oasis_no_data(self, dataset, date):
        with caiso_vcr.use_cassette(
            f"test_oasis_no_data_{dataset}_{date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_oasis_dataset(
                dataset=dataset,
                date=date,
            )

            assert df.empty

    def test_get_pnodes(self):
        with caiso_vcr.use_cassette("test_get_pnodes.yaml"):
            df = self.iso.get_pnodes()
            assert df.shape[0] > 0

    """get_lmp_scheduling_point_tie_combination"""

    def _check_lmp_scheduling_point_tie(self, df: pd.DataFrame):
        assert df.shape[0] > 0
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Location",
            "Market",
            "Node",
            "Tie",
            "LMP",
            "Energy",
            "Congestion",
            "Loss",
            "GHG",
        ]

        assert (df["Location"] == df["Node"] + " " + df["Tie"]).all()

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

    @pytest.mark.parametrize("date", ["2022-10-15"])
    def test_get_lmp_scheduling_point_tie_real_time_5_min(self, date):
        with caiso_vcr.use_cassette(
            f"test_get_lmp_scheduling_point_tie_real_time_5_min_{date}.yaml",
        ):
            df = self.iso.get_lmp_scheduling_point_tie_real_time_5_min(date)
            self._check_lmp_scheduling_point_tie(df)

            interval_minutes = (
                df["Interval End"] - df["Interval Start"]
            ).dt.total_seconds() / 60
            assert (interval_minutes == 5).all()

    @pytest.mark.parametrize("date", ["2022-10-15"])
    def test_get_lmp_scheduling_point_tie_real_time_15_min(self, date):
        with caiso_vcr.use_cassette(
            f"test_get_lmp_scheduling_point_tie_real_time_15_min_{date}.yaml",
        ):
            df = self.iso.get_lmp_scheduling_point_tie_real_time_15_min(date)
            self._check_lmp_scheduling_point_tie(df)

            interval_minutes = (
                df["Interval End"] - df["Interval Start"]
            ).dt.total_seconds() / 60
            assert (interval_minutes == 15).all()

    @pytest.mark.parametrize("date", ["2022-10-15"])
    def test_get_lmp_scheduling_point_tie_day_ahead_hourly(self, date):
        with caiso_vcr.use_cassette(
            f"test_get_lmp_scheduling_point_tie_day_ahead_hourly_{date}.yaml",
        ):
            df = self.iso.get_lmp_scheduling_point_tie_day_ahead_hourly(date)
            self._check_lmp_scheduling_point_tie(df)

            interval_minutes = (
                df["Interval End"] - df["Interval Start"]
            ).dt.total_seconds() / 60
            assert (interval_minutes == 60).all()

    @pytest.mark.parametrize(
        "start, end",
        [
            (
                pd.Timestamp("today").normalize() - pd.Timedelta(days=3),
                pd.Timestamp("today").normalize() - pd.Timedelta(days=1),
            ),
        ],
    )
    def test_get_lmp_scheduling_point_tie_day_ahead_hourly_min_date_range(
        self,
        start,
        end,
    ):
        with caiso_vcr.use_cassette(
            f"test_get_lmp_scheduling_point_tie_day_ahead_hourly_date_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_lmp_scheduling_point_tie_day_ahead_hourly(
                start,
                end=end,
            )
            self._check_lmp_scheduling_point_tie(df)

            assert df["Interval Start"].min() >= self.local_start_of_day(start)

    @pytest.mark.parametrize(
        "start, end",
        [
            (
                pd.Timestamp("today").normalize() - pd.Timedelta(days=3),
                pd.Timestamp("today").normalize() - pd.Timedelta(days=1),
            ),
        ],
    )
    def test_get_lmp_scheduling_point_tie_real_time_5_min_date_range(
        self,
        start,
        end,
    ):
        with caiso_vcr.use_cassette(
            f"test_get_lmp_scheduling_point_tie_real_time_5_min_date_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_lmp_scheduling_point_tie_real_time_5_min(
                start,
                end=end,
            )
            self._check_lmp_scheduling_point_tie(df)

            assert df["Interval Start"].min() >= self.local_start_of_day(start)

    @pytest.mark.parametrize(
        "start, end",
        [
            (
                pd.Timestamp("today").normalize() - pd.Timedelta(days=3),
                pd.Timestamp("today").normalize() - pd.Timedelta(days=1),
            ),
        ],
    )
    def test_get_lmp_scheduling_point_tie_real_time_15_min_date_range(
        self,
        start,
        end,
    ):
        with caiso_vcr.use_cassette(
            f"test_get_lmp_scheduling_point_tie_real_time_15_min_date_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_lmp_scheduling_point_tie_real_time_15_min(
                start,
                end=end,
            )
            self._check_lmp_scheduling_point_tie(df)

            assert df["Interval Start"].min() >= self.local_start_of_day(start)

    @pytest.mark.parametrize("date", ["2022-10-15"])
    def test_get_lmp_hasp_15_min(self, date):
        with caiso_vcr.use_cassette(f"test_get_lmp_hasp_15_min_{date}.yaml"):
            df = self.iso.get_lmp_hasp_15_min(date)
            self._check_lmp_hasp_15_min(df)

            interval_minutes = (
                df["Interval End"] - df["Interval Start"]
            ).dt.total_seconds() / 60
            assert (interval_minutes == 15).all()

    def _check_lmp_hasp_15_min(self, df: pd.DataFrame):
        assert df.shape[0] > 0
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Location",
            "LMP",
            "Energy",
            "Congestion",
            "Loss",
            "GHG",
        ]

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

    @pytest.mark.parametrize(
        "start, end",
        [
            (
                pd.Timestamp("today").normalize() - pd.Timedelta(days=3),
                pd.Timestamp("today").normalize() - pd.Timedelta(days=1),
            ),
        ],
    )
    def test_get_lmp_hasp_15_min_date_range(self, start, end):
        with caiso_vcr.use_cassette(
            f"test_get_lmp_hasp_15_min_date_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_lmp_hasp_15_min(start, end=end)
            self._check_lmp_hasp_15_min(df)

            assert df["Interval Start"].min() >= self.local_start_of_day(start)
            assert df["Interval End"].max() <= self.local_start_of_day(
                end,
            ) + pd.Timedelta(days=1)

    def test_get_tie_flows_real_time_15_min_latest(self):
        with caiso_vcr.use_cassette("test_get_tie_flows_real_time_15_min_latest.yaml"):
            df = self.iso.get_tie_flows_real_time_15_min("latest")
            assert df.shape[0] > 0
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

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2025-03-20", "2025-03-22"),
        ],
    )
    def test_get_tie_flows_real_time_15_min_date_range(self, date, end):
        with caiso_vcr.use_cassette(
            f"test_get_tie_flows_real_time_15_min_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_tie_flows_real_time_15_min(date, end=end)
            assert df.shape[0] > 0
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
            assert df["Interval Start"].min() >= pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            )

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2025-03-20", "2025-03-22"),
        ],
    )
    def test_get_nomogram_branch_shadow_prices_day_ahead_hourly(self, date, end):
        with caiso_vcr.use_cassette(
            f"test_get_nomogram_branch_shadow_prices_day_ahead_hourly_{date}_{end}.yaml",
        ):
            df = self.iso.get_nomogram_branch_shadow_prices_day_ahead_hourly(
                date,
                end=end,
            )
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location",
                "Price",
            ]
            assert df["Interval Start"].min() >= pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            )

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2025-03-20", "2025-03-22"),
        ],
    )
    def test_get_nomogram_branch_shadow_prices_hasp_hourly(self, date, end):
        with caiso_vcr.use_cassette(
            f"get_nomogram_branch_shadow_prices_hasp_hourly_{date}_{end}.yaml",
        ):
            df = self.iso.get_nomogram_branch_shadow_prices_hasp_hourly(date, end=end)
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location",
                "Price",
            ]
            assert df["Interval Start"].min() >= pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            )

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2025-03-20", "2025-03-22"),
        ],
    )
    def test_get_nomogram_branch_shadow_price_forecast_15_min(self, date, end):
        with caiso_vcr.use_cassette(
            f"get_nomogram_branch_shadow_price_forecast_15_min_{date}_{end}.yaml",
        ):
            df = self.iso.get_nomogram_branch_shadow_price_forecast_15_min(
                date,
                end=end,
            )
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location",
                "Price",
            ]
            assert df["Interval Start"].min() >= pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            )

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2025-03-20", "2025-03-22"),
        ],
    )
    def test_get_interval_nomogram_branch_shadow_prices_real_time_5_min(
        self,
        date,
        end,
    ):
        with caiso_vcr.use_cassette(
            f"get_interval_nomogram_branch_shadow_prices_real_time_5_min_{date}_{end}.yaml",
        ):
            df = self.iso.get_interval_nomogram_branch_shadow_prices_real_time_5_min(
                date,
                end=end,
            )
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location",
                "Price",
            ]
            assert df["Interval Start"].min() >= pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            )
