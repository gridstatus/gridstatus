import math

import numpy as np
import pandas as pd
import pytest

from gridstatus import CAISO, Markets
from gridstatus.base import NoDataFoundException, NotSupported
from gridstatus.caiso.caiso import _collapse_group_to_array
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

    """get_seven_day_resource_adequacy_outlook"""

    def _seven_day_resource_adequacy_outlook_columns(self) -> list[str]:
        return [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Demand",
            "Net Demand",
            "Day Ahead Demand Forecast",
            "Day Ahead Net Demand Forecast",
            "Resource Adequacy Capacity Forecast",
            "Net Resource Adequacy Capacity Forecast",
            "Reserve Requirement",
            "Reserve Requirement Forecast",
            "Resource Adequacy Credits",
        ]

    @pytest.mark.parametrize("date", ["2026-03-11"])
    def test_get_seven_day_resource_adequacy_outlook_historical(self, date: str):
        with caiso_vcr.use_cassette(
            f"test_get_seven_day_resource_adequacy_outlook_{date}.yaml",
            match_on=["method", "scheme", "host", "port", "path"],
        ):
            df = self.iso.get_seven_day_resource_adequacy_outlook(date)
        assert df.shape[0] > 0
        assert (
            df.columns.tolist() == self._seven_day_resource_adequacy_outlook_columns()
        )
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()
        assert df["Publish Time"].nunique() == 1
        expected_pub = pd.Timestamp(date, tz=self.iso.default_timezone).normalize()
        assert (df["Publish Time"] == expected_pub).all()
        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

    @pytest.mark.parametrize("start, end", [("2026-03-11", "2026-03-13")])
    def test_get_seven_day_resource_adequacy_outlook_date_range(
        self,
        start: str,
        end: str,
    ):
        with caiso_vcr.use_cassette(
            f"test_get_seven_day_resource_adequacy_outlook_{start}_{end}.yaml",
            match_on=["method", "scheme", "host", "port", "path"],
        ):
            df = self.iso.get_seven_day_resource_adequacy_outlook(start, end=end)
        assert df.shape[0] > 0
        assert (
            df.columns.tolist() == self._seven_day_resource_adequacy_outlook_columns()
        )
        assert df["Publish Time"].nunique() == 2
        assert df.columns[:3].tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
        ]
        for col in ["Interval Start", "Interval End", "Publish Time"]:
            assert isinstance(df.loc[0][col], pd.Timestamp)
            assert df.loc[0][col].tz is not None
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()
        sorted_df = df.sort_values(
            by=["Interval Start", "Publish Time"],
            kind="mergesort",
        )
        assert sorted_df["Interval Start"].is_monotonic_increasing

    def test_get_seven_day_resource_adequacy_outlook_latest_matches_today(self):
        with caiso_vcr.use_cassette(
            "test_get_seven_day_resource_adequacy_outlook_latest.yaml",
            match_on=["method", "scheme", "host", "port", "path"],
        ):
            latest_df = self.iso.get_seven_day_resource_adequacy_outlook("latest")
            today_df = self.iso.get_seven_day_resource_adequacy_outlook("today")
        assert latest_df.equals(today_df)
        assert (
            latest_df.columns.tolist()
            == self._seven_day_resource_adequacy_outlook_columns()
        )
        assert (latest_df["Publish Time"] == self.local_start_of_today()).all()

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

    def _check_edam_wind_solar_forecast(self, df: pd.DataFrame) -> None:
        assert df.shape[0] > 0

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "BAA",
            "Solar",
            "Wind",
        ]

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

        assert isinstance(df.loc[0]["Publish Time"], pd.Timestamp)
        assert df.loc[0]["Publish Time"].tz is not None

        assert pd.api.types.is_numeric_dtype(df["Solar"])
        assert pd.api.types.is_numeric_dtype(df["Wind"])

        assert df["BAA"].notna().all()
        assert not df.duplicated(subset=["Interval Start", "BAA"]).any()

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

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

    """get_edam_wind_solar_forecast"""

    def test_get_edam_wind_solar_forecast_today(self):
        with caiso_vcr.use_cassette(
            "test_get_edam_wind_solar_forecast_today.yaml",
        ):
            df = self.iso.get_edam_wind_solar_forecast("today")
        self._check_edam_wind_solar_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval Start"].max() == self.local_start_of_today() + pd.Timedelta(
            hours=23,
        )

    @pytest.mark.parametrize("date", ["2026-05-01"])
    def test_get_edam_wind_solar_forecast_historical_date(self, date):
        with caiso_vcr.use_cassette(
            f"test_get_edam_wind_solar_forecast_{date}.yaml",
        ):
            df = self.iso.get_edam_wind_solar_forecast(date)
        self._check_edam_wind_solar_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval Start"].max() == self.local_start_of_day(
            date,
        ) + pd.Timedelta(hours=23)

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
        "GHG",
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
                "Nomogram ID XML",
                "Market Run ID",
                "Constraint Cause",
                "Price",
                "Groups",
            ]
            assert df["Groups"].apply(type).eq(list).all()
            assert df["Interval Start"].min() >= pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            )

    def test_get_nomogram_branch_shadow_prices_day_ahead_hourly_latest(self):
        with caiso_vcr.use_cassette(
            "test_get_nomogram_branch_shadow_prices_day_ahead_hourly_latest.yaml",
        ):
            df = self.iso.get_nomogram_branch_shadow_prices_day_ahead_hourly("latest")
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location",
                "Nomogram ID XML",
                "Market Run ID",
                "Constraint Cause",
                "Price",
                "Groups",
            ]
            assert df["Groups"].apply(type).eq(list).all()
            assert df["Interval Start"].min() >= self.local_start_of_today()

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
                "Nomogram ID XML",
                "Market Run ID",
                "Constraint Cause",
                "Price",
                "Groups",
            ]
            assert df["Groups"].apply(type).eq(list).all()
            assert df["Interval Start"].min() >= pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            )

    def test_get_nomogram_branch_shadow_prices_hasp_hourly_latest(self):
        with caiso_vcr.use_cassette(
            "test_get_nomogram_branch_shadow_prices_hasp_hourly_latest.yaml",
        ):
            df = self.iso.get_nomogram_branch_shadow_prices_hasp_hourly("latest")
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location",
                "Nomogram ID XML",
                "Market Run ID",
                "Constraint Cause",
                "Price",
                "Groups",
            ]
            assert df["Groups"].apply(type).eq(list).all()
            assert df["Interval Start"].min() >= self.local_start_of_today()

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
                "Nomogram ID XML",
                "Market Run ID",
                "Constraint Cause",
                "Price",
                "Groups",
            ]
            assert df["Groups"].apply(type).eq(list).all()
            assert df["Interval Start"].min() >= pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            )

    def test_get_nomogram_branch_shadow_price_forecast_15_min_latest(self):
        with caiso_vcr.use_cassette(
            "test_get_nomogram_branch_shadow_price_forecast_15_min_latest.yaml",
        ):
            df = self.iso.get_nomogram_branch_shadow_price_forecast_15_min("latest")
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location",
                "Nomogram ID XML",
                "Market Run ID",
                "Constraint Cause",
                "Price",
                "Groups",
            ]
            assert df["Groups"].apply(type).eq(list).all()
            assert df["Interval Start"].min() >= self.local_start_of_today()

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
                "Market Run ID",
                "Constraint Cause",
                "Price",
                "Groups",
            ]
            assert df["Groups"].apply(type).eq(list).all()
            assert df["Interval Start"].min() >= pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            )

    def test_get_interval_nomogram_branch_shadow_prices_real_time_5_min_latest(self):
        with caiso_vcr.use_cassette(
            "test_get_interval_nomogram_branch_shadow_prices_real_time_5_min_latest.yaml",
        ):
            df = self.iso.get_interval_nomogram_branch_shadow_prices_real_time_5_min(
                "latest",
            )
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location",
                "Market Run ID",
                "Constraint Cause",
                "Price",
                "Groups",
            ]
            assert df["Groups"].apply(type).eq(list).all()
            assert df["Interval Start"].min() >= self.local_start_of_today()

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2025-03-20", "2025-03-22"),
        ],
    )
    def test_get_intertie_constraint_shadow_prices_real_time_5_min(
        self,
        date,
        end,
    ):
        with caiso_vcr.use_cassette(
            f"test_get_intertie_constraint_shadow_prices_real_time_5_min_{date}_{end}.yaml",
        ):
            df = self.iso.get_intertie_constraint_shadow_prices_real_time_5_min(
                date,
                end=end,
            )
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "TI ID",
                "TI Direction",
                "Market Run ID",
                "Constraint Cause",
                "Shadow Price",
                "Groups",
            ]
            assert df["Groups"].apply(type).eq(list).all()
            assert df["Interval Start"].min() >= pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            )

    def test_get_intertie_constraint_shadow_prices_real_time_5_min_latest(self):
        with caiso_vcr.use_cassette(
            "test_get_intertie_constraint_shadow_prices_real_time_5_min_latest.yaml",
        ):
            df = self.iso.get_intertie_constraint_shadow_prices_real_time_5_min(
                "latest",
            )
            assert df.shape[0] > 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "TI ID",
                "TI Direction",
                "Market Run ID",
                "Constraint Cause",
                "Shadow Price",
                "Groups",
            ]
            assert df["Groups"].apply(type).eq(list).all()
            assert df["Interval Start"].min() >= self.local_start_of_today()

    """get_system_load_and_resource_schedules"""

    def _check_system_load_and_resource_schedules(
        self,
        df: pd.DataFrame,
        interval_minutes: int,
        schedule_columns: list[str],
    ):
        """Helper to check system load and resource schedules dataframe."""
        assert (
            list(df.columns)
            == [
                "Interval Start",
                "Interval End",
                "TAC Name",
            ]
            + schedule_columns
        )

        # Check that interval timestamps are valid
        assert pd.api.types.is_datetime64_any_dtype(df["Interval Start"])
        assert pd.api.types.is_datetime64_any_dtype(df["Interval End"])

        assert (
            (df["Interval End"] - df["Interval Start"])
            == pd.Timedelta(minutes=interval_minutes)
        ).all()

        # Check TAC Name is string
        assert pd.api.types.is_string_dtype(df["TAC Name"])

        # Check that schedule columns contain numeric data
        for col in schedule_columns:
            assert pd.api.types.is_numeric_dtype(df[col]), (
                f"Column {col} should be numeric"
            )

    def test_get_system_load_and_resource_schedules_day_ahead_latest(self):
        with caiso_vcr.use_cassette(
            "test_get_system_load_and_resource_schedules_day_ahead_latest.yaml",
        ):
            df = self.iso.get_system_load_and_resource_schedules_day_ahead(
                "latest",
            )
            self._check_system_load_and_resource_schedules(
                df,
                60,
                schedule_columns=["Export", "Generation", "Import", "Load"],
            )

            # For day-ahead, should have future data
            assert df["Interval Start"].max() > self.local_now()

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2024-01-15", "2024-01-17"),
            ("2024-06-01", "2024-06-03"),
        ],
    )
    def test_get_system_load_and_resource_schedules_day_ahead_date_range(
        self,
        date,
        end,
    ):
        with caiso_vcr.use_cassette(
            f"test_get_system_load_and_resource_schedules_day_ahead_{date}_{end}.yaml",
        ):
            df = self.iso.get_system_load_and_resource_schedules_day_ahead(
                date,
                end=end,
            )
            self._check_system_load_and_resource_schedules(
                df,
                60,
                schedule_columns=["Export", "Generation", "Import", "Load"],
            )

            # Check date range
            assert df["Interval Start"].min() == pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval Start"].max() == pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            ) - pd.Timedelta(minutes=60)

    def test_get_system_load_and_resource_schedules_hasp_latest(self):
        with caiso_vcr.use_cassette(
            "test_get_system_load_and_resource_schedules_hasp_latest.yaml",
        ):
            df = self.iso.get_system_load_and_resource_schedules_hasp("latest")
            self._check_system_load_and_resource_schedules(
                df,
                60,
                schedule_columns=["Export", "Import"],
            )

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2024-01-15", "2024-01-17"),
            ("2024-06-01", "2024-06-03"),
        ],
    )
    def test_get_system_load_and_resource_schedules_hasp_date_range(
        self,
        date,
        end,
    ):
        with caiso_vcr.use_cassette(
            f"test_get_system_load_and_resource_schedules_hasp_{date}_{end}.yaml",
        ):
            df = self.iso.get_system_load_and_resource_schedules_hasp(
                date,
                end=end,
            )
            self._check_system_load_and_resource_schedules(
                df,
                60,
                schedule_columns=["Export", "Import"],
            )

            # Check date range
            assert df["Interval Start"].min() == pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval Start"].max() == pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            ) - pd.Timedelta(minutes=60)

    def test_get_system_load_and_resource_schedules_real_time_5_min_latest(self):
        with caiso_vcr.use_cassette(
            "test_get_system_load_and_resource_schedules_real_time_5_min_latest.yaml",
        ):
            df = self.iso.get_system_load_and_resource_schedules_real_time_5_min(
                "latest",
            )
            self._check_system_load_and_resource_schedules(
                df,
                5,
                schedule_columns=["Export", "Generation", "Import"],
            )

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2024-01-15", "2024-01-17"),
            ("2024-06-01", "2024-06-03"),
        ],
    )
    def test_get_system_load_and_resource_schedules_real_time_5_min_date_range(
        self,
        date,
        end,
    ):
        with caiso_vcr.use_cassette(
            f"test_get_system_load_and_resource_schedules_real_time_5_min_{date}_{end}.yaml",
        ):
            df = self.iso.get_system_load_and_resource_schedules_real_time_5_min(
                date,
                end=end,
            )
            self._check_system_load_and_resource_schedules(
                df,
                5,
                schedule_columns=["Export", "Generation", "Import"],
            )

            # Check date range
            assert df["Interval Start"].min() == pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval Start"].max() == pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            ) - pd.Timedelta(minutes=5)

    def test_get_system_load_and_resource_schedules_ruc_latest(self):
        with caiso_vcr.use_cassette(
            "test_get_system_load_and_resource_schedules_ruc_latest.yaml",
        ):
            df = self.iso.get_system_load_and_resource_schedules_ruc("latest")
            self._check_system_load_and_resource_schedules(
                df,
                60,
                schedule_columns=["Generation", "Import"],
            )

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2024-01-15", "2024-01-17"),
            ("2024-06-01", "2024-06-03"),
        ],
    )
    def test_get_system_load_and_resource_schedules_ruc_date_range(
        self,
        date,
        end,
    ):
        with caiso_vcr.use_cassette(
            f"test_get_system_load_and_resource_schedules_ruc_{date}_{end}.yaml",
        ):
            df = self.iso.get_system_load_and_resource_schedules_ruc(
                date,
                end=end,
            )
            self._check_system_load_and_resource_schedules(
                df,
                60,
                schedule_columns=["Generation", "Import"],
            )

            # Check date range
            assert df["Interval Start"].min() == pd.Timestamp(
                date,
                tz=self.iso.default_timezone,
            )
            assert df["Interval Start"].max() == pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            ) - pd.Timedelta(minutes=60)

    def test_get_lmp_hasp_15_min_no_data_exception(self):
        """Test that NoDataFoundException includes start and end dates in the message."""
        future_date = "2050-01-01"

        with pytest.raises(NoDataFoundException) as exc_info:
            self.iso.get_lmp_hasp_15_min(future_date)

        assert "start date:" in str(exc_info.value)
        assert "end date:" in str(exc_info.value)
        assert future_date in str(exc_info.value)

    def test_get_lmp_hasp_15_min_no_data_exception_with_end_date(self):
        """Test that NoDataFoundException includes both start and end dates when both are provided."""
        future_start = "2050-01-01T00:00:00Z"
        future_end = "2050-01-01T00:00:05Z"

        with pytest.raises(NoDataFoundException) as exc_info:
            self.iso.get_lmp_hasp_15_min(future_start, future_end)

        assert "start date:" in str(exc_info.value)
        assert "end date:" in str(exc_info.value)
        assert future_start in str(exc_info.value)
        assert future_end in str(exc_info.value)

    def test_get_lmp_scheduling_point_tie_real_time_5_min_no_data_exception(self):
        """Test that NoDataFoundException includes start and end dates in the message."""
        old_start = "2000-01-01T00:00:00Z"
        old_end = "2000-01-01T00:00:05Z"

        with pytest.raises(NoDataFoundException) as exc_info:
            self.iso.get_lmp_scheduling_point_tie_real_time_5_min(old_start, old_end)

        assert "start date:" in str(exc_info.value)
        assert "end date:" in str(exc_info.value)
        assert old_start in str(exc_info.value)
        assert old_end in str(exc_info.value)
        assert "Real Time 5 Min" in str(exc_info.value)

    def test_get_lmp_scheduling_point_tie_real_time_15_min_no_data_exception(self):
        """Test that NoDataFoundException includes start and end dates in the message."""
        old_start = "2000-01-01T00:00:00Z"
        old_end = "2000-01-01T00:00:15Z"

        with pytest.raises(NoDataFoundException) as exc_info:
            self.iso.get_lmp_scheduling_point_tie_real_time_15_min(old_start, old_end)

        assert "start date:" in str(exc_info.value)
        assert "end date:" in str(exc_info.value)
        assert old_start in str(exc_info.value)
        assert old_end in str(exc_info.value)
        assert "Real Time 15 Min" in str(exc_info.value)

    def test_get_lmp_scheduling_point_tie_day_ahead_hourly_no_data_exception(self):
        """Test that NoDataFoundException includes start and end dates in the message."""
        old_start = "2000-01-01T00:00:00Z"
        old_end = "2000-01-01T01:00:00Z"

        with pytest.raises(NoDataFoundException) as exc_info:
            self.iso.get_lmp_scheduling_point_tie_day_ahead_hourly(old_start, old_end)

        assert "start date:" in str(exc_info.value)
        assert "end date:" in str(exc_info.value)
        assert old_start in str(exc_info.value)
        assert old_end in str(exc_info.value)
        assert "Day Ahead Hourly" in str(exc_info.value)

    _DAILY_ENERGY_STORAGE_CASSETTE = "test_daily_energy_storage_report_2026_04_06.yaml"
    _DAILY_ENERGY_STORAGE_HISTORICAL_DATE = "2026-04-06"

    _DAILY_ENERGY_STORAGE_METHODS = (
        "get_storage_awards_fmm",
        "get_storage_awards_ifm",
        "get_storage_awards_rtd",
        "get_storage_energy_awards_ruc",
        "get_storage_energy_bids_fmm",
        "get_storage_energy_bids_ifm",
        "get_storage_soc_fmm",
        "get_storage_soc_hourly",
        "get_storage_soc_rtd",
    )

    @staticmethod
    def _assert_daily_energy_storage_frame(method_name: str, df: pd.DataFrame) -> None:
        if method_name == "get_storage_awards_fmm":
            assert df.shape == (960, 5)
            assert set(df.columns) == {
                "Interval Start",
                "Interval End",
                "Product",
                "Type",
                "MW",
            }
        elif method_name == "get_storage_awards_ifm":
            assert df.shape == (240, 5)
            assert set(df.columns) == {
                "Interval Start",
                "Interval End",
                "Product",
                "Type",
                "MW",
            }
        elif method_name == "get_storage_awards_rtd":
            assert df.shape == (576, 4)
            assert set(df.columns) == {
                "Interval Start",
                "Interval End",
                "Type",
                "MW",
            }
        elif method_name == "get_storage_energy_awards_ruc":
            assert df.shape == (576, 4)
            assert set(df.columns) == {
                "Interval Start",
                "Interval End",
                "Type",
                "MW",
            }
        elif method_name == "get_storage_energy_bids_fmm":
            assert df.shape == (4608, 6)
            assert set(df.columns) == {
                "Interval Start",
                "Interval End",
                "Bid Range",
                "Operation",
                "Type",
                "MW",
            }
        elif method_name == "get_storage_energy_bids_ifm":
            assert df.shape == (1152, 6)
            assert set(df.columns) == {
                "Interval Start",
                "Interval End",
                "Bid Range",
                "Operation",
                "Type",
                "MW",
            }
        elif method_name == "get_storage_soc_fmm":
            assert df.shape == (288, 3)
            assert set(df.columns) == {
                "Interval Start",
                "Interval End",
                "SOC",
            }
        elif method_name == "get_storage_soc_hourly":
            assert df.shape == (48, 4)
            assert list(df.columns) == [
                "Interval Start",
                "Interval End",
                "SOC",
                "Schedule",
            ]
        elif method_name == "get_storage_soc_rtd":
            assert df.shape == (288, 3)
            assert set(df.columns) == {
                "Interval Start",
                "Interval End",
                "SOC",
            }
        else:
            raise AssertionError(f"unknown method {method_name!r}")

    @pytest.mark.parametrize("method_name", _DAILY_ENERGY_STORAGE_METHODS)
    def test_daily_energy_storage_reports(self, method_name: str) -> None:
        with caiso_vcr.use_cassette(self._DAILY_ENERGY_STORAGE_CASSETTE):
            df = getattr(self.iso, method_name)(
                self._DAILY_ENERGY_STORAGE_HISTORICAL_DATE,
            )
        self._assert_daily_energy_storage_frame(method_name, df)

    def test_daily_energy_storage_latest_not_supported(self) -> None:
        with pytest.raises(NotSupported):
            self.iso.get_storage_awards_fmm("latest")

    def test_daily_energy_storage_fetch_uses_legacy_slug_fallback(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from gridstatus.caiso import daily_energy_storage

        requested_urls: list[str] = []

        class FakeResponse:
            def __init__(self, status_code: int, content: bytes) -> None:
                self.status_code = status_code
                self.content = content

        def fake_get(url: str, timeout: int) -> FakeResponse:
            requested_urls.append(url)
            if "daily-energy-storage-report-may-302024.html" in url:
                return FakeResponse(
                    200,
                    b"<html><script>var tot_charge_rtd = [1];</script></html>",
                )
            return FakeResponse(404, b"")

        monkeypatch.setattr(daily_energy_storage.requests, "get", fake_get)
        html = daily_energy_storage._fetch_daily_energy_storage_html(
            "2024-05-30",
            tz="US/Pacific",
            verbose=False,
        )
        assert "tot_charge_rtd" in html
        assert requested_urls[:3] == [
            "https://www.caiso.com/documents/daily-energy-storage-report-may-30-2024.html",
            "https://www.caiso.com/documents/daily-energy-storage-report-may-30-2024-corrected.html",
            "https://www.caiso.com/documents/daily-energy-storage-report-may-302024.html",
        ]

    def test_daily_energy_storage_fetch_uses_no_zero_day_slug_fallback(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from gridstatus.caiso import daily_energy_storage

        requested_urls: list[str] = []

        class FakeResponse:
            def __init__(self, status_code: int, content: bytes) -> None:
                self.status_code = status_code
                self.content = content

        def fake_get(url: str, timeout: int) -> FakeResponse:
            requested_urls.append(url)
            if "daily-energy-storage-report-may-8-2025.html" in url:
                return FakeResponse(
                    200,
                    b"<html><script>var tot_charge_rtd = [1];</script></html>",
                )
            return FakeResponse(404, b"")

        monkeypatch.setattr(daily_energy_storage.requests, "get", fake_get)
        html = daily_energy_storage._fetch_daily_energy_storage_html(
            "2025-05-08",
            tz="US/Pacific",
            verbose=False,
        )
        assert "tot_charge_rtd" in html
        assert requested_urls[:3] == [
            "https://www.caiso.com/documents/daily-energy-storage-report-may-08-2025.html",
            "https://www.caiso.com/documents/daily-energy-storage-report-may-08-2025-corrected.html",
            "https://www.caiso.com/documents/daily-energy-storage-report-may-8-2025.html",
        ]

    def test_daily_energy_storage_fetch_uses_legacy_slug_no_leading_zero_on_day(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from gridstatus.caiso import daily_energy_storage

        requested_urls: list[str] = []

        class FakeResponse:
            def __init__(self, status_code: int, content: bytes) -> None:
                self.status_code = status_code
                self.content = content

        def fake_get(url: str, timeout: int) -> FakeResponse:
            requested_urls.append(url)
            if "daily-energy-storage-report-may-82024.html" in url:
                return FakeResponse(
                    200,
                    b"<html><script>var tot_charge_rtd = [1];</script></html>",
                )
            return FakeResponse(404, b"")

        monkeypatch.setattr(daily_energy_storage.requests, "get", fake_get)
        html = daily_energy_storage._fetch_daily_energy_storage_html(
            "2024-05-08",
            tz="US/Pacific",
            verbose=False,
        )
        assert "tot_charge_rtd" in html
        assert (
            "https://www.caiso.com/documents/daily-energy-storage-report-may-82024.html"
            in requested_urls
        )

    @pytest.mark.parametrize(
        ("report_date", "compact_document_name"),
        [
            ("2024-01-31", "dailyenergystoragereportjan31-2024.html"),
            ("2022-08-31", "dailyenergystoragereportaug31-2022.html"),
            ("2022-08-01", "dailyenergystoragereportaug01-2022.html"),
        ],
    )
    def test_daily_energy_storage_fetch_uses_compact_dailyenergystoragereport_slug(
        self,
        report_date: str,
        compact_document_name: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from gridstatus.caiso import daily_energy_storage

        requested_urls: list[str] = []

        class FakeResponse:
            def __init__(self, status_code: int, content: bytes) -> None:
                self.status_code = status_code
                self.content = content

        def fake_get(url: str, timeout: int) -> FakeResponse:
            requested_urls.append(url)
            if compact_document_name in url:
                return FakeResponse(
                    200,
                    b"<html><script>var tot_charge_rtd = [1];</script></html>",
                )
            return FakeResponse(404, b"")

        monkeypatch.setattr(daily_energy_storage.requests, "get", fake_get)
        html = daily_energy_storage._fetch_daily_energy_storage_html(
            report_date,
            tz="US/Pacific",
            verbose=False,
        )
        assert "tot_charge_rtd" in html
        assert (
            f"https://www.caiso.com/documents/{compact_document_name}" in requested_urls
        )

    def test_daily_energy_storage_parse_and_downsample_coerce_na_strings(self) -> None:
        from gridstatus.caiso import daily_energy_storage

        html = (
            '<html><script>var tot_energy_rtpd = [1, 2, "NA", 4, 5, 6];</script></html>'
        )
        parsed = daily_energy_storage._parse_js_array(html, "tot_energy_rtpd")
        assert len(parsed) == 6
        assert parsed[0] == 1.0
        assert parsed[1] == 2.0
        assert pd.isna(parsed[2])
        assert parsed[3] == 4.0
        down = daily_energy_storage._downsample_5min_to_15min(parsed)
        assert len(down) == 2
        assert down[0] == (1.0 + 2.0) / 2.0
        assert down[1] == (4.0 + 5.0 + 6.0) / 3.0

    def test_build_storage_soc_hourly_collapses_forward_filled_five_minute_arrays(
        self,
    ) -> None:
        from gridstatus.caiso import daily_energy_storage

        ifm: list[float] = []
        ruc: list[float] = []
        for h in range(24):
            ifm.extend([float(h * 100)] * 12)
            ruc.extend([float(h * 100 + 1)] * 12)
        html = (
            "<html><script>var tot_charge_ifm = "
            + str(ifm)
            + "; var tot_charge_ruc = "
            + str(ruc)
            + ";</script></html>"
        )
        report_start = pd.Timestamp("2026-04-06", tz="US/Pacific")
        df = daily_energy_storage.build_storage_soc_hourly(html, report_start)
        assert df.shape == (48, 4)
        ifm_df = df.loc[df["Schedule"] == "IFM"].sort_values("Interval Start")
        assert len(ifm_df) == 24
        deltas = ifm_df["Interval Start"].diff().dropna()
        assert (deltas == pd.Timedelta(hours=1)).all()


NOMOGRAM_GROUP_COLS = [
    "Interval Start",
    "Interval End",
    "Location",
    "Nomogram ID XML",
    "Market Run ID",
    "Constraint Cause",
    "Price",
]

INTERTIE_GROUP_COLS = [
    "Interval Start",
    "Interval End",
    "TI ID",
    "TI Direction",
    "Market Run ID",
    "Constraint Cause",
    "Shadow Price",
]


def _make_nomogram_rows(location, price, groups, ts="2025-01-01 08:00"):
    """Create rows mimicking CAISO nomogram shadow price data before collapse."""
    rows = []
    for g in groups:
        rows.append(
            {
                "Interval Start": pd.Timestamp(ts, tz="US/Pacific"),
                "Interval End": pd.Timestamp(ts, tz="US/Pacific")
                + pd.Timedelta(hours=1),
                "Location": location,
                "Nomogram ID XML": "NOM_1234",
                "Market Run ID": "DAM",
                "Constraint Cause": "Thermal",
                "Price": price,
                "Group": g,
            },
        )
    return rows


def _make_intertie_rows(ti_id, direction, shadow_price, groups, ts="2025-01-01 08:00"):
    """Create rows mimicking CAISO intertie constraint shadow price data."""
    rows = []
    for g in groups:
        rows.append(
            {
                "Interval Start": pd.Timestamp(ts, tz="US/Pacific"),
                "Interval End": pd.Timestamp(ts, tz="US/Pacific")
                + pd.Timedelta(minutes=5),
                "TI ID": ti_id,
                "TI Direction": direction,
                "Market Run ID": "RTD",
                "Constraint Cause": "Thermal",
                "Shadow Price": shadow_price,
                "Group": g,
            },
        )
    return rows


class TestCollapseGroupToArray:
    """Unit tests for _collapse_group_to_array helper."""

    def test_nomogram_multiple_groups_collapsed_and_sorted(self):
        """Multiple Group rows for a single constraint collapse into a sorted list."""
        rows = _make_nomogram_rows(
            "24723_CONTROL_115_24865_TAP188_115_BR_2_1",
            133.52,
            [3, 1, 5],
        )
        df = pd.DataFrame(rows)
        result = _collapse_group_to_array(df, NOMOGRAM_GROUP_COLS)

        assert len(result) == 1
        assert result["Groups"].iloc[0] == [1, 3, 5]
        assert "Group" not in result.columns

    def test_intertie_multiple_groups_collapsed(self):
        """Intertie constraint data collapses groups correctly."""
        rows = _make_intertie_rows("EPE_NET_ITC", "E", -51.39, [2, 1])
        df = pd.DataFrame(rows)
        result = _collapse_group_to_array(df, INTERTIE_GROUP_COLS)

        assert len(result) == 1
        assert result["Groups"].iloc[0] == [1, 2]
        assert result["TI ID"].iloc[0] == "EPE_NET_ITC"
        assert result["TI Direction"].iloc[0] == "E"

    def test_single_group_returns_single_element_list(self):
        rows = _make_nomogram_rows(
            "24723_CONTROL_115_24865_TAP188_115_BR_2_1",
            100.0,
            [1],
        )
        df = pd.DataFrame(rows)
        result = _collapse_group_to_array(df, NOMOGRAM_GROUP_COLS)

        assert len(result) == 1
        assert result["Groups"].iloc[0] == [1]

    def test_nan_groups_dropped(self):
        rows = _make_nomogram_rows(
            "24723_CONTROL_115_24865_TAP188_115_BR_2_1",
            588.84,
            [1, np.nan, 3],
        )
        df = pd.DataFrame(rows)
        result = _collapse_group_to_array(df, NOMOGRAM_GROUP_COLS)

        assert len(result) == 1
        assert result["Groups"].iloc[0] == [1, 3]

    def test_all_nan_groups_returns_empty_list(self):
        rows = _make_nomogram_rows(
            "24723_CONTROL_115_24865_TAP188_115_BR_2_1",
            100.0,
            [np.nan, np.nan],
        )
        df = pd.DataFrame(rows)
        result = _collapse_group_to_array(df, NOMOGRAM_GROUP_COLS)

        assert len(result) == 1
        assert result["Groups"].iloc[0] == []

    def test_float_groups_converted_to_int(self):
        rows = _make_nomogram_rows(
            "24723_CONTROL_115_24865_TAP188_115_BR_2_1",
            100.0,
            [1.0, 2.0],
        )
        df = pd.DataFrame(rows)
        result = _collapse_group_to_array(df, NOMOGRAM_GROUP_COLS)

        groups = result["Groups"].iloc[0]
        assert groups == [1, 2]
        assert all(isinstance(v, int) for v in groups)

    def test_distinct_constraints_stay_separate(self):
        """Different locations produce separate rows after collapse."""
        rows = _make_nomogram_rows(
            "24723_CONTROL_115_24865_TAP188_115_BR_2_1",
            133.52,
            [2, 3, 5],
        ) + _make_nomogram_rows(
            "30900_DELANEY_500_24156_N.GILA_500_BR_1_1",
            45.0,
            [1],
        )
        df = pd.DataFrame(rows)
        result = _collapse_group_to_array(df, NOMOGRAM_GROUP_COLS)

        assert len(result) == 2
        row_a = result[
            result["Location"] == "24723_CONTROL_115_24865_TAP188_115_BR_2_1"
        ].iloc[0]
        row_b = result[
            result["Location"] == "30900_DELANEY_500_24156_N.GILA_500_BR_1_1"
        ].iloc[0]
        assert row_a["Groups"] == [2, 3, 5]
        assert row_b["Groups"] == [1]

    def test_same_location_different_intervals_stay_separate(self):
        """Same constraint at different intervals keeps separate rows."""
        rows = _make_nomogram_rows(
            "24723_CONTROL_115_24865_TAP188_115_BR_2_1",
            133.52,
            [1, 2],
            ts="2025-01-01 08:00",
        ) + _make_nomogram_rows(
            "24723_CONTROL_115_24865_TAP188_115_BR_2_1",
            133.52,
            [3, 4],
            ts="2025-01-01 09:00",
        )
        df = pd.DataFrame(rows)
        result = _collapse_group_to_array(df, NOMOGRAM_GROUP_COLS)

        assert len(result) == 2
        row_08 = result[
            result["Interval Start"]
            == pd.Timestamp("2025-01-01 08:00", tz="US/Pacific")
        ].iloc[0]
        row_09 = result[
            result["Interval Start"]
            == pd.Timestamp("2025-01-01 09:00", tz="US/Pacific")
        ].iloc[0]
        assert row_08["Groups"] == [1, 2]
        assert row_09["Groups"] == [3, 4]
