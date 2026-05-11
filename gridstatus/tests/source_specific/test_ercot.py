import datetime
from io import StringIO
from typing import Dict
from unittest import mock

import numpy as np
import pandas as pd
import pytest

from gridstatus import Markets, NoDataFoundException, NotSupported
from gridstatus.ercot import (
    ELECTRICAL_BUS_LOCATION_TYPE,
    Ercot,
    ERCOTSevenDayLoadForecastReport,
    parse_timestamp_from_friendly_name,
)
from gridstatus.ercot_60d_utils import (
    DAM_AS_ONLY_AWARDS_COLUMNS,
    DAM_AS_ONLY_AWARDS_KEY,
    DAM_AS_ONLY_OFFERS_COLUMNS,
    DAM_AS_ONLY_OFFERS_KEY,
    DAM_ENERGY_BID_AWARDS_COLUMNS,
    DAM_ENERGY_BID_AWARDS_KEY,
    DAM_ENERGY_BIDS_COLUMNS,
    DAM_ENERGY_BIDS_KEY,
    DAM_ENERGY_ONLY_OFFER_AWARDS_COLUMNS,
    DAM_ENERGY_ONLY_OFFER_AWARDS_KEY,
    DAM_ENERGY_ONLY_OFFERS_COLUMNS,
    DAM_ENERGY_ONLY_OFFERS_KEY,
    DAM_ESR_AS_OFFERS_COLUMNS,
    DAM_ESR_AS_OFFERS_KEY,
    DAM_ESR_COLUMNS,
    DAM_ESR_KEY,
    DAM_GEN_RESOURCE_AS_OFFERS_KEY,
    DAM_GEN_RESOURCE_COLUMNS,
    DAM_GEN_RESOURCE_KEY,
    DAM_LOAD_RESOURCE_AS_OFFERS_KEY,
    DAM_LOAD_RESOURCE_COLUMNS,
    DAM_LOAD_RESOURCE_KEY,
    DAM_PTP_OBLIGATION_BID_AWARDS_COLUMNS,
    DAM_PTP_OBLIGATION_BID_AWARDS_KEY,
    DAM_PTP_OBLIGATION_BIDS_COLUMNS,
    DAM_PTP_OBLIGATION_BIDS_KEY,
    DAM_PTP_OBLIGATION_OPTION_AWARDS_COLUMNS,
    DAM_PTP_OBLIGATION_OPTION_AWARDS_KEY,
    DAM_PTP_OBLIGATION_OPTION_COLUMNS,
    DAM_PTP_OBLIGATION_OPTION_KEY,
    DAM_RESOURCE_AS_OFFERS_COLUMNS,
    SCED_AS_OFFER_UPDATES_IN_OP_HOUR_COLUMNS,
    SCED_AS_OFFER_UPDATES_IN_OP_HOUR_KEY,
    SCED_ESR_COLUMNS,
    SCED_ESR_KEY,
    SCED_GEN_RESOURCE_COLUMNS,
    SCED_GEN_RESOURCE_KEY,
    SCED_LOAD_RESOURCE_COLUMNS,
    SCED_LOAD_RESOURCE_KEY,
    SCED_RESOURCE_AS_OFFERS_COLUMNS,
    SCED_RESOURCE_AS_OFFERS_KEY,
    SCED_SMNE_COLUMNS,
    SCED_SMNE_KEY,
    CurveOutputFormat,
    _categorize_strings,
    extract_curve,
    process_as_offer_curves,
    process_sced_resource_as_offers,
)
from gridstatus.ercot_constants import (
    LOAD_FORECAST_BY_MODEL_COLUMNS,
    SOLAR_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS,
    SOLAR_ACTUAL_AND_FORECAST_COLUMNS,
    SYSTEM_AS_CAPACITY_MONITOR_COLUMNS,
    WIND_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS,
    WIND_ACTUAL_AND_FORECAST_COLUMNS,
)
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

api_vcr = setup_vcr(
    source="ercot",
    record_mode=RECORD_MODE,
)

INTERVALS_PER_HOUR_AT_FIVE_MINUTE_RESOLUTION = 12


class TestErcot(BaseTestISO):
    iso = Ercot()

    # These are the weather zones in ERCOT in the order we want them.
    weather_zone_columns = [
        "Coast",
        "East",
        "Far West",
        "North",
        "North Central",
        "South Central",
        "Southern",
        "West",
    ]

    """dam_system_lambda"""

    @pytest.mark.integration
    def test_get_dam_system_lambda_latest(self):
        df = self.iso.get_dam_system_lambda("latest", verbose=True)
        self._check_dam_system_lambda(df)
        # We don't know the exact publish date because it could be yesterday
        # or today depending on when this test is run
        assert df["Publish Time"].dt.date.nunique() == 1

    @pytest.mark.integration
    def test_get_dam_system_lambda_today(self):
        df = self.iso.get_dam_system_lambda("today", verbose=True)
        self._check_dam_system_lambda(df)
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        # Published yesterday
        assert df["Publish Time"].dt.date.unique() == [today - pd.Timedelta(days=1)]
        assert df["Interval Start"].dt.date.unique() == [today]

    @pytest.mark.integration
    def test_get_dam_system_lambda_historical(self):
        two_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(days=2)
        df = self.iso.get_dam_system_lambda(two_days_ago)
        self._check_dam_system_lambda(df)
        assert list(df["Publish Time"].dt.date.unique()) == [
            two_days_ago - pd.Timedelta(days=1),
        ]

    @pytest.mark.integration
    def test_get_dam_system_lambda_historical_range(self):
        three_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(days=3)
        two_days_ago = three_days_ago + pd.Timedelta(days=1)
        df = self.iso.get_dam_system_lambda(
            start=three_days_ago,
            end=two_days_ago + pd.Timedelta(days=1),
            verbose=True,
        )
        self._check_dam_system_lambda(df)
        assert list(df["Publish Time"].dt.date.unique()) == [
            three_days_ago - pd.Timedelta(days=1),
            two_days_ago - pd.Timedelta(days=1),
        ]

    """sced_system_lambda"""

    @pytest.mark.integration
    def test_get_sced_system_lambda(self):
        for i in ["latest", "today"]:
            df = self.iso.get_sced_system_lambda(i, verbose=True)
            assert df.shape[0] >= 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "SCED Timestamp",
                "System Lambda",
            ]
            today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
            assert df["SCED Timestamp"].unique()[0].date() == today
            assert isinstance(df["System Lambda"].unique()[0], float)

    """as_prices"""

    @pytest.mark.integration
    def test_get_as_prices(self):
        as_cols = [
            "Time",
            "Interval Start",
            "Interval End",
            "Market",
            "Non-Spinning Reserves",
            "Regulation Down",
            "Regulation Up",
            "Responsive Reserves",
            "ERCOT Contingency Reserve Service",
        ]

        # today
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        df = self.iso.get_as_prices(today)
        assert df.shape[0] >= 0
        assert df.columns.tolist() == as_cols
        assert df["Time"].unique()[0].date() == today

        date = today - pd.Timedelta(days=3)
        df = self.iso.get_as_prices(date)
        assert df.shape[0] >= 0
        assert df.columns.tolist() == as_cols
        assert df["Time"].unique()[0].date() == date

    """get_as_plan"""

    def _check_as_plan(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "NSPIN",
            "REGDN",
            "REGUP",
            "RRS",
            "ECRS",
        ]

    @pytest.mark.integration
    def test_get_as_plan_today_or_latest(self):
        df = self.iso.get_as_plan("today")
        self._check_as_plan(df)
        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.DateOffset(
            days=7,
        )
        assert df["Publish Time"].dt.date.unique().tolist() == [self.local_today()]
        assert self.iso.get_as_plan("latest").equals(df)

    @pytest.mark.integration
    def test_get_as_plan_historical_date(self):
        date = self.local_today() - pd.Timedelta(days=30)
        df = self.iso.get_as_plan(date)
        self._check_as_plan(df)
        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval End"].max() == self.local_start_of_day(
            date,
        ) + pd.DateOffset(days=7)
        assert df["Publish Time"].dt.date.unique().tolist() == [date]

    @pytest.mark.integration
    def test_get_as_plan_historical_date_range(self):
        start_date = self.local_today() - pd.Timedelta(days=30)
        end_date = start_date + pd.Timedelta(days=2)
        df = self.iso.get_as_plan(start_date, end_date)
        self._check_as_plan(df)
        assert df["Interval Start"].min() == self.local_start_of_day(start_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            end_date,
            # Not inclusive of end date
        ) + pd.DateOffset(days=6)
        assert df["Publish Time"].dt.date.unique().tolist() == [
            start_date,
            (start_date + pd.DateOffset(days=1)).date(),
        ]

    @pytest.mark.integration
    def test_get_as_monitor(self):
        df = self.iso.get_as_monitor()
        # asset length is 1, 49 columns
        assert df.shape == (1, 49)
        # assert every colunn but the first is int dtype
        assert df.iloc[:, 1:].dtypes.unique() == "int64"
        assert df.columns[0] == "Time"

    @pytest.mark.integration
    def test_get_real_time_system_conditions(self):
        df = self.iso.get_real_time_system_conditions()
        assert df.shape == (1, 15)
        assert df.columns[0] == "Time"

    """get_operations_messages"""

    expected_operations_messages_cols = [
        "Time",
        "Notice",
        "Type",
        "Status",
    ]

    SAMPLE_OPS_MESSAGES_DF = pd.DataFrame(
        {
            "Date & Time": [
                "Apr 14, 2026 2:23:50 AM",
                "Apr 14, 2026 12:04:02 AM",
            ],
            "Notice": [
                "ERCOT has cancelled the following notice: Railroad DC Tie derated.",
                "No sudden loss of generation greater than 450 MW occurred.",
            ],
            "Type": [
                "Operational Information",
                "Operational Information",
            ],
            "Status": [
                "Cancelled",
                "Active",
            ],
        },
    )

    def test_get_operations_messages(self):
        with mock.patch(
            "gridstatus.ercot.pd.read_html",
            return_value=[self.SAMPLE_OPS_MESSAGES_DF.copy()],
        ):
            df = self.iso.get_operations_messages()

        assert df.columns.tolist() == self.expected_operations_messages_cols
        assert len(df) == 2
        assert isinstance(df["Time"].dtype, pd.DatetimeTZDtype)
        assert str(df["Time"].dt.tz) == str(self.iso.default_timezone)
        assert df["Notice"].iloc[0] is not None
        assert df["Type"].iloc[0] == "Operational Information"
        assert set(df["Status"]) == {"Active", "Cancelled"}

    def test_get_operations_messages_sorted_by_time(self):
        with mock.patch(
            "gridstatus.ercot.pd.read_html",
            return_value=[self.SAMPLE_OPS_MESSAGES_DF.copy()],
        ):
            df = self.iso.get_operations_messages()

        assert df["Time"].is_monotonic_increasing

    def test_get_operations_messages_single_row(self):
        single_row_df = pd.DataFrame(
            {
                "Date & Time": ["Mar 10, 2026 9:00:00 AM"],
                "Notice": ["Advisory issued due to tool unavailability."],
                "Type": ["Advisory"],
                "Status": ["Active"],
            },
        )
        with mock.patch(
            "gridstatus.ercot.pd.read_html",
            return_value=[single_row_df],
        ):
            df = self.iso.get_operations_messages()

        assert len(df) == 1
        assert df.columns.tolist() == self.expected_operations_messages_cols
        assert df["Type"].iloc[0] == "Advisory"

    def test_get_operations_messages_historical_deduplicates(self):
        snap1 = pd.DataFrame(
            {
                "Date & Time": [
                    "Jan 10, 2026 2:00:00 PM",
                    "Jan 9, 2026 12:00:00 AM",
                ],
                "Notice": ["Msg A", "Msg B"],
                "Type": ["Operational Information", "Operational Information"],
                "Status": ["Active", "Active"],
            },
        )
        snap2 = pd.DataFrame(
            {
                "Date & Time": [
                    "Jan 15, 2026 8:00:00 AM",
                    "Jan 10, 2026 2:00:00 PM",
                ],
                "Notice": ["Msg C", "Msg A"],
                "Type": ["Advisory", "Operational Information"],
                "Status": ["Active", "Active"],
            },
        )

        with mock.patch(
            "gridstatus.ercot.requests.get",
        ) as mock_requests_get:
            mock_cdx_resp = mock.Mock()
            mock_cdx_resp.json.return_value = [
                ["timestamp", "statuscode"],
                ["20260110000000", "200"],
                ["20260115000000", "200"],
            ]
            mock_cdx_resp.raise_for_status = mock.Mock()
            mock_requests_get.return_value = mock_cdx_resp

            with mock.patch(
                "gridstatus.ercot.pd.read_html",
                side_effect=[[snap1], [snap2]],
            ):
                df = self.iso.get_operations_messages(
                    date="2026-01-01",
                    end="2026-02-01",
                )

        assert len(df) == 3
        assert df["Notice"].tolist() == ["Msg B", "Msg A", "Msg C"]
        assert df["Time"].is_monotonic_increasing

    def test_get_operations_messages_historical_filters_to_range(self):
        snap = pd.DataFrame(
            {
                "Date & Time": [
                    "Jan 15, 2026 8:00:00 AM",
                    "Dec 28, 2025 3:00:00 PM",
                ],
                "Notice": ["In range", "Out of range"],
                "Type": ["Operational Information", "Operational Information"],
                "Status": ["Active", "Active"],
            },
        )

        with mock.patch(
            "gridstatus.ercot.requests.get",
        ) as mock_requests_get:
            mock_cdx_resp = mock.Mock()
            mock_cdx_resp.json.return_value = [
                ["timestamp", "statuscode"],
                ["20260115000000", "200"],
            ]
            mock_cdx_resp.raise_for_status = mock.Mock()
            mock_requests_get.return_value = mock_cdx_resp

            with mock.patch(
                "gridstatus.ercot.pd.read_html",
                return_value=[snap],
            ):
                df = self.iso.get_operations_messages(
                    date="2026-01-01",
                    end="2026-02-01",
                )

        assert len(df) == 1
        assert df["Notice"].iloc[0] == "In range"

    def test_get_operations_messages_historical_wayback(self):
        with api_vcr.use_cassette(
            "test_get_operations_messages_historical_wayback.yaml",
        ):
            df = self.iso.get_operations_messages(
                date="2026-01-01",
                end="2026-01-15",
            )
        assert df.columns.tolist() == self.expected_operations_messages_cols
        assert len(df) > 0
        assert isinstance(df["Time"].dtype, pd.DatetimeTZDtype)
        assert df["Time"].min() >= pd.Timestamp("2026-01-01", tz="US/Central")
        assert df["Time"].max() < pd.Timestamp("2026-01-15", tz="US/Central")
        assert df["Time"].is_monotonic_increasing
        assert df.duplicated(subset=["Time", "Notice"]).sum() == 0

    @pytest.mark.integration
    def test_get_energy_storage_resources(self):
        df = self.iso.get_energy_storage_resources()
        assert df.columns.tolist() == [
            "Time",
            "Total Charging",
            "Total Discharging",
            "Net Output",
        ]

    """get_fuel_mix"""

    fuel_mix_cols = [
        "Time",
        "Coal and Lignite",
        "Hydro",
        "Nuclear",
        "Power Storage",
        "Solar",
        "Wind",
        "Natural Gas",
        "Other",
    ]

    def test_get_fuel_mix_today(self):
        with api_vcr.use_cassette("test_get_fuel_mix_today.yaml"):
            df = self.iso.get_fuel_mix("today")
        self._check_fuel_mix(df)
        assert df.shape[0] >= 0
        assert df.columns.tolist() == self.fuel_mix_cols

    def test_get_fuel_mix_latest(self):
        with api_vcr.use_cassette("test_get_fuel_mix_latest.yaml"):
            df = self.iso.get_fuel_mix("latest")
        self._check_fuel_mix(df)
        # returns two days of data
        assert df["Time"].dt.date.nunique() == 2
        assert df.shape[0] >= 0
        assert df.columns.tolist() == self.fuel_mix_cols

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_date_or_start(self):
        pass

    def test_get_fuel_mix_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_historical_with_date_range(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_range_two_days_with_day_start_endpoint(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_start_end_same_day(self):
        pass

    """get_fuel_mix_detailed"""
    fuel_mix_detailed_columns = [
        "Time",
        "Coal and Lignite Gen",
        "Coal and Lignite HSL",
        "Coal and Lignite Seasonal Capacity",
        "Hydro Gen",
        "Hydro HSL",
        "Hydro Seasonal Capacity",
        "Nuclear Gen",
        "Nuclear HSL",
        "Nuclear Seasonal Capacity",
        "Power Storage Gen",
        "Power Storage HSL",
        "Power Storage Seasonal Capacity",
        "Solar Gen",
        "Solar HSL",
        "Solar Seasonal Capacity",
        "Wind Gen",
        "Wind HSL",
        "Wind Seasonal Capacity",
        "Natural Gas Gen",
        "Natural Gas HSL",
        "Natural Gas Seasonal Capacity",
        "Other Gen",
        "Other HSL",
        "Other Seasonal Capacity",
    ]

    def test_get_fuel_mix_detailed_latest(self):
        with api_vcr.use_cassette("test_get_fuel_mix_detailed_latest.yaml"):
            df = self.iso.get_fuel_mix_detailed("latest")
        assert df.columns.tolist() == self.fuel_mix_detailed_columns
        assert df["Time"].dt.date.nunique() == 2

    def test_get_fuel_mix_detailed_today(self):
        with api_vcr.use_cassette("test_get_fuel_mix_detailed_today.yaml"):
            df = self.iso.get_fuel_mix_detailed("today")
        assert df.columns.tolist() == self.fuel_mix_detailed_columns
        assert df["Time"].dt.date.nunique() == 1

    """get_lmp"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_lmp_date_range(self, markets=None):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_lmp_historical(self, markets=None):
        pass

    @pytest.mark.integration
    def test_get_load_3_days_ago(self):
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        three_days_ago = today - pd.Timedelta(days=3)
        df = self.iso.get_load(three_days_ago)
        self._check_load(df)
        assert df["Time"].unique()[0].date() == three_days_ago

    @pytest.mark.integration
    def test_get_load_by_weather_zone(self):
        df = self.iso.get_load_by_weather_zone("today")
        self._check_time_columns(df, instant_or_interval="interval")
        cols = (
            [
                "Time",
                "Interval Start",
                "Interval End",
            ]
            + self.weather_zone_columns
            + ["System Total"]
        )

        assert df.columns.tolist() == cols

        # test 5 days ago
        five_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(days=5)
        df = self.iso.get_load_by_weather_zone(five_days_ago)
        self._check_time_columns(df, instant_or_interval="interval")
        assert df["Time"].unique()[0].date() == five_days_ago

        assert df.columns.tolist() == cols

    @pytest.mark.integration
    def test_get_load_by_forecast_zone_today(self):
        df = self.iso.get_load_by_forecast_zone("today")
        self._check_time_columns(df, instant_or_interval="interval")
        columns = [
            "Time",
            "Interval Start",
            "Interval End",
            "NORTH",
            "SOUTH",
            "WEST",
            "HOUSTON",
            "TOTAL",
        ]
        assert df.columns.tolist() == columns

        five_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(days=5)
        df = self.iso.get_load_by_forecast_zone(five_days_ago)
        self._check_time_columns(df, instant_or_interval="interval")
        assert df["Time"].unique()[0].date() == five_days_ago

    """get_load_forecast"""

    @pytest.mark.integration
    def test_get_load_forecast_range(self):
        end = pd.Timestamp.now(tz=self.iso.default_timezone)
        start = end - pd.Timedelta(hours=3)
        df = self.iso.get_load_forecast(start=start, end=end)

        unique_load_forecast_time = df["Publish Time"].unique()
        # make sure each is between start and end
        assert (unique_load_forecast_time >= start).all()
        assert (unique_load_forecast_time <= end).all()

    expected_load_forecast_columns = [
        "Time",
        "Interval Start",
        "Interval End",
        "Publish Time",
        "North",
        "South",
        "West",
        "Houston",
        "System Total",
    ]

    @pytest.mark.integration
    def test_get_load_forecast_historical(self):
        test_date = (pd.Timestamp.now() - pd.Timedelta(days=2)).date()
        forecast = self.iso.get_load_forecast(date=test_date)
        self._check_forecast(
            forecast,
            expected_columns=self.expected_load_forecast_columns,
        )

    @pytest.mark.integration
    def test_get_load_forecast_today(self):
        forecast = self.iso.get_load_forecast("today")
        self._check_forecast(
            forecast,
            expected_columns=self.expected_load_forecast_columns,
        )

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical_with_date_range(self):
        pass

    @pytest.mark.integration
    def test_get_load_forecast_by_weather_zone(self):
        df = self.iso.get_load_forecast(
            "today",
            forecast_type=ERCOTSevenDayLoadForecastReport.BY_WEATHER_ZONE,
        )

        cols = (
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Publish Time",
            ]
            + self.weather_zone_columns
            + ["System Total"]
        )

        self._check_forecast(df, expected_columns=cols)

        five_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(days=5)
        df = self.iso.get_load_forecast(
            five_days_ago,
            forecast_type=ERCOTSevenDayLoadForecastReport.BY_WEATHER_ZONE,
        )

        self._check_forecast(df, expected_columns=cols)

    """get_load_forecast_by_model"""

    def _check_load_forecast_by_model(self, df):
        check_load_forecast_by_model(df)

    def test_get_load_forecast_by_model_date_range(self):
        start = self.local_today() - pd.Timedelta(days=3)
        end = self.local_today() - pd.Timedelta(days=2)

        with api_vcr.use_cassette(
            f"test_get_load_forecast_by_model_date_range_{start}_{end}.yaml",
        ):
            df = self.iso.get_load_forecast_by_model(start, end, verbose=True)

        self._check_load_forecast_by_model(df)

        # One day of data
        assert df["Publish Time"].nunique() == 24

    """get_capacity_committed"""

    @pytest.mark.integration
    def test_get_capacity_committed(self):
        df = self.iso.get_capacity_committed("latest")

        assert df.columns.tolist() == ["Interval Start", "Interval End", "Capacity"]

        assert df["Interval Start"].min() == self.local_start_of_today()
        # The end time is approximately now
        assert (
            self.local_now() - pd.Timedelta(minutes=5)
            < df["Interval End"].max()
            < self.local_now() + pd.Timedelta(minutes=5)
        )

        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            minutes=5,
        )

    """get_capacity_forecast"""

    @pytest.mark.integration
    def test_get_capacity_forecast(self):
        df = self.iso.get_capacity_forecast("latest")

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Committed Capacity",
            "Available Capacity",
        ]

        # The start time is approximately now
        assert (
            self.local_now() - pd.Timedelta(minutes=5)
            < df["Interval Start"].min()
            < self.local_now() + pd.Timedelta(minutes=5)
        )

        assert df["Interval End"].max() >= self.local_start_of_day(
            self.local_today() + pd.Timedelta(days=1),
        )

        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            minutes=5,
        )

    """get_available_seasonal_capacity_forecast"""

    @pytest.mark.integration
    def test_get_available_seasonal_capacity_forecast(self):
        df = self.iso.get_available_seasonal_capacity_forecast("latest")

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Available Capacity",
            "Load Forecast",
        ]

        # Use DateOffset for comparisons because it takes into account DST
        assert df[
            "Interval Start"
        ].min() == self.local_start_of_today() + pd.DateOffset(
            days=1,
        )
        assert df["Interval End"].max() == self.local_start_of_today() + pd.DateOffset(
            days=7,
        )

        # This can use a timedelta because it doesn't span a day
        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            hours=1,
        )

    """get_spp"""

    @pytest.mark.integration
    def test_get_spp_dam_today_day_ahead_hourly_hub(self):
        df = self.iso.get_spp(
            date="today",
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="Trading Hub",
        )
        # minimum interval start is beginning of day
        assert df["Interval Start"].min().hour == 0
        assert df["Interval Start"].min().minute == 0
        self._check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Trading Hub")

    @pytest.mark.integration
    def test_get_spp_dam_today_day_ahead_hourly_node(self):
        df = self.iso.get_spp(
            date="today",
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="Resource Node",
        )
        # minimum interval start is beginning of day
        assert df["Interval Start"].min().hour == 0
        assert df["Interval Start"].min().minute == 0
        self._check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Resource Node")

    @pytest.mark.integration
    def test_get_spp_dam_today_day_ahead_hourly_zone(self):
        df = self.iso.get_spp(
            date="today",
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="Load Zone",
        )
        # minimum interval start is beginning of day
        assert df["Interval Start"].min().hour == 0
        assert df["Interval Start"].min().minute == 0
        self._check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Load Zone")

    @pytest.mark.integration
    def test_get_spp_dam_range(self):
        today = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize()

        two_days_ago = today - pd.Timedelta(
            days=2,
        )

        df = self.iso.get_spp(
            start=two_days_ago,
            end=today,
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="Load Zone",
        )

        # two unique days
        # should be today and yesterday since published one day ahead
        assert set(df["Interval Start"].dt.date.unique()) == {
            today.date(),
            today.date() - pd.Timedelta(days=1),
        }
        self._check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Load Zone")

    @pytest.mark.integration
    def test_get_spp_real_time_range(self):
        today = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize()

        one_hour_earlier = today - pd.Timedelta(
            hours=1,
        )

        df = self.iso.get_spp(
            start=one_hour_earlier,
            end=today,
            market=Markets.REAL_TIME_15_MIN,
            location_type="Load Zone",
        )

        # should be 4 intervals in last hour
        assert (df.groupby("Location")["Interval Start"].count() == 4).all()
        assert df["Interval End"].min() > one_hour_earlier
        assert df["Interval End"].max() <= today

        self._check_ercot_spp(df, Markets.REAL_TIME_15_MIN, "Load Zone")

    @pytest.mark.integration
    def test_get_spp_real_time_yesterday(self):
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        yesterday = today - pd.Timedelta(days=1)

        df = self.iso.get_spp(
            date=yesterday,
            market=Markets.REAL_TIME_15_MIN,
            location_type="Trading Hub",
            verbose=True,
        )

        # assert Interval End max is today
        assert df["Interval End"].max().date() == today
        assert df["Interval Start"].min().date() == yesterday

    @pytest.mark.integration
    def test_get_spp_real_time_handles_all_location_types(self):
        df = self.iso.get_spp(
            date="latest",
            market=Markets.REAL_TIME_15_MIN,
            verbose=True,
        )

        assert set(df["Location Type"].unique()) == {
            "Resource Node",
            "Load Zone DC Tie",
            "Load Zone DC Tie Energy Weighted",
            "Trading Hub",
            "Load Zone Energy Weighted",
            "Load Zone",
        }

    @pytest.mark.integration
    def test_get_spp_day_ahead_handles_all_location_types(self):
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        yesterday = today - pd.Timedelta(days=1)
        df = self.iso.get_spp(
            date=yesterday,
            market=Markets.DAY_AHEAD_HOURLY,
            verbose=True,
        )

        assert set(df["Location Type"].unique()) == {
            "Resource Node",
            "Load Zone DC Tie",
            "Trading Hub",
            "Load Zone",
        }

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_spp_rtm_historical(self):
        rtm = self.iso.get_rtm_spp(2020)
        assert isinstance(rtm, pd.DataFrame)
        assert len(rtm) > 0

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_spp_today_real_time_15_minutes_zone(self):
        df = self.iso.get_spp(
            date="today",
            market=Markets.REAL_TIME_15_MIN,
            location_type="Load Zone",
        )
        # minimum interval start is beginning of day
        assert df["Interval Start"].min().hour == 0
        assert df["Interval Start"].min().minute == 0
        self._check_ercot_spp(df, Markets.REAL_TIME_15_MIN, "Load Zone")

    @pytest.mark.integration
    def test_get_spp_two_days_ago_day_ahead_hourly_zone(self):
        two_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=2,
        )
        df = self.iso.get_spp(
            date=two_days_ago,
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="Load Zone",
        )
        # minimum interval start is beginning of day
        assert df["Interval Start"].min().hour == 0
        assert df["Interval Start"].min().minute == 0
        self._check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Load Zone")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_spp_two_days_ago_real_time_15_minutes_zone(self):
        two_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=2,
        )
        df = self.iso.get_spp(
            date=two_days_ago,
            market=Markets.REAL_TIME_15_MIN,
            location_type="Load Zone",
        )
        # minimum interval start is beginning of day
        assert df["Interval Start"].min().hour == 0
        assert df["Interval Start"].min().minute == 0
        self._check_ercot_spp(df, Markets.REAL_TIME_15_MIN, "Load Zone")

    """get_60_day_sced_disclosure"""

    def test_get_60_day_sced_disclosure_historical(self):
        days_ago_65 = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=65,
        )

        with api_vcr.use_cassette(
            f"test_get_60_day_sced_disclosure_historical_{days_ago_65}",
        ):
            df_dict = self.iso.get_60_day_sced_disclosure(
                date=days_ago_65,
                process=True,
            )

        load_resource = df_dict[SCED_LOAD_RESOURCE_KEY]
        gen_resource = df_dict[SCED_GEN_RESOURCE_KEY]
        smne = df_dict[SCED_SMNE_KEY]

        assert load_resource["SCED Timestamp"].dt.date.unique()[0] == days_ago_65
        assert gen_resource["SCED Timestamp"].dt.date.unique()[0] == days_ago_65
        assert smne["Interval Time"].dt.date.unique()[0] == days_ago_65

        check_60_day_sced_disclosure(df_dict)

    def test_get_60_day_sced_disclosure_range(self):
        days_ago_65 = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=65,
        )

        days_ago_66 = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=66,
        )

        with api_vcr.use_cassette(
            f"test_get_60_day_sced_disclosure_range_{days_ago_66}_{days_ago_65}",
        ):
            df_dict = self.iso.get_60_day_sced_disclosure(
                start=days_ago_66,
                end=days_ago_65
                + pd.Timedelta(days=1),  # add one day to end date since exclusive
                process=True,
                verbose=True,
            )

        load_resource = df_dict[SCED_LOAD_RESOURCE_KEY]
        gen_resource = df_dict[SCED_GEN_RESOURCE_KEY]
        smne = df_dict[SCED_SMNE_KEY]

        check_60_day_sced_disclosure(df_dict)

        assert load_resource["SCED Timestamp"].dt.date.unique().tolist() == [
            days_ago_66,
            days_ago_65,
        ]

        assert gen_resource["SCED Timestamp"].dt.date.unique().tolist() == [
            days_ago_66,
            days_ago_65,
        ]

        assert smne["Interval Time"].dt.date.unique().tolist() == [
            days_ago_66,
            days_ago_65,
        ]

    @pytest.mark.integration
    def test_get_60_day_sced_disclosure_esr(self):
        # ESR data is available starting 2025-12-05
        esr_start = pd.Timestamp("2025-12-05").date()
        days_ago_65 = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=65,
        )

        # Use the later of 65 days ago or ESR start date
        date = max(days_ago_65, esr_start)

        with api_vcr.use_cassette(
            f"test_get_60_day_sced_disclosure_esr_{date}",
        ):
            try:
                df_dict = self.iso.get_60_day_sced_disclosure(
                    date=date,
                    process=True,
                )
            except NoDataFoundException:
                pytest.skip(
                    f"No data found for date {date} - "
                    "ESR report may not be published yet",
                )

        assert SCED_ESR_KEY in df_dict
        esr = df_dict[SCED_ESR_KEY]

        assert esr.columns.tolist() == SCED_ESR_COLUMNS
        assert len(esr) > 0
        assert esr["Resource Type"].unique().tolist() == ["ESR"]
        assert esr["SCED Timestamp"].dt.date.unique()[0] == date

        # Verify offer curves are parsed
        assert esr["SCED1 Offer Curve"].apply(lambda x: isinstance(x, list)).any()
        assert esr["SCED2 Offer Curve"].apply(lambda x: isinstance(x, list)).any()
        assert esr["SCED TPO Offer Curve"].apply(lambda x: isinstance(x, list)).any()

        # Also check the other datasets are still present
        check_60_day_sced_disclosure(df_dict)

    def test_get_60_day_sced_disclosure_supplemental_correction(self):
        # Data dates Dec 5-20, 2025 (report dates Feb 3-18, 2026) need
        # supplemental correction for ESR, Gen Resource, and Load Resource.
        # Data dates Dec 5, 2025 - Feb 2, 2026 (report dates Feb 3 -
        # April 3, 2026) need supplemental correction for Resource AS Offers.
        # 2025-12-10 falls in both ranges.
        date = pd.Timestamp("2025-12-10").date()

        with api_vcr.use_cassette(
            "test_get_60_day_sced_disclosure_supplemental_correction",
        ):
            df_dict = self.iso.get_60_day_sced_disclosure(
                date=date,
                process=True,
            )

        check_60_day_sced_disclosure(df_dict)

        # All four corrected datasets should be present
        assert SCED_ESR_KEY in df_dict
        assert SCED_GEN_RESOURCE_KEY in df_dict
        assert SCED_LOAD_RESOURCE_KEY in df_dict
        assert SCED_RESOURCE_AS_OFFERS_KEY in df_dict

        # Verify data is for the correct date
        esr = df_dict[SCED_ESR_KEY]
        gen = df_dict[SCED_GEN_RESOURCE_KEY]
        load = df_dict[SCED_LOAD_RESOURCE_KEY]
        resource_as_offers = df_dict[SCED_RESOURCE_AS_OFFERS_KEY]

        assert esr["SCED Timestamp"].dt.date.unique()[0] == date
        assert gen["SCED Timestamp"].dt.date.unique()[0] == date
        assert load["SCED Timestamp"].dt.date.unique()[0] == date
        assert resource_as_offers["SCED Timestamp"].dt.date.unique()[0] == date
        assert resource_as_offers.columns.tolist() == SCED_RESOURCE_AS_OFFERS_COLUMNS

        # SMNE should still come from the normal disclosure
        smne = df_dict[SCED_SMNE_KEY]
        assert len(smne) > 0

    @pytest.mark.integration
    def test_get_60_day_sced_disclosure_telemetered_net_output(self):
        """Test that Telemetered Net Output contains real data, not NaN.

        On 2025-12-28 the raw data column is named 'Telemetered Net Output'
        (no trailing space), unlike earlier dates which had a trailing space.
        Without stripping whitespace from column names, the processing code
        fails to match the column and fills it with NaN.
        """
        date = pd.Timestamp("2025-12-28").date()

        with api_vcr.use_cassette(
            "test_get_60_day_sced_disclosure_telemetered_net_output",
        ):
            df_dict = self.iso.get_60_day_sced_disclosure(
                date=date,
                process=True,
            )

        gen_resource = df_dict[SCED_GEN_RESOURCE_KEY]

        assert len(gen_resource) > 0
        assert gen_resource.columns.tolist() == SCED_GEN_RESOURCE_COLUMNS

        # The critical assertion: Telemetered Net Output must contain real
        # data, not all NaN. On main, the column name mismatch causes
        # this to be filled with NaN for dates where the raw data has no
        # trailing space.
        assert gen_resource["Telemetered Net Output"].notna().any(), (
            "Telemetered Net Output is all NaN - column name mismatch"
        )

    """get_60_day_dam_disclosure"""

    @pytest.mark.integration
    def test_get_60_day_dam_disclosure_historical(self):
        days_ago_65 = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=65,
        )

        df_dict = self.iso.get_60_day_dam_disclosure(date=days_ago_65, process=True)

        check_60_day_dam_disclosure(df_dict)

    @pytest.mark.integration
    def test_get_60_day_dam_disclosure_esr(self):
        # ESR data is available starting 2025-12-06
        esr_start = pd.Timestamp("2025-12-06").date()
        days_ago_65 = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=65,
        )

        # Use the later of 65 days ago or ESR start date
        date = max(days_ago_65, esr_start)

        with api_vcr.use_cassette(
            f"test_get_60_day_dam_disclosure_esr_{date}",
        ):
            try:
                df_dict = self.iso.get_60_day_dam_disclosure(
                    date=date,
                    process=True,
                )
            except NoDataFoundException:
                pytest.skip(
                    f"No data found for date {date} - "
                    "ESR report may not be published yet",
                )

        assert DAM_ESR_KEY in df_dict
        dam_esr = df_dict[DAM_ESR_KEY]

        assert dam_esr.columns.tolist() == DAM_ESR_COLUMNS
        assert len(dam_esr) > 0
        assert dam_esr["Resource Type"].unique().tolist() == ["ESR"]

        # Verify offer curves are parsed
        assert (
            dam_esr["QSE submitted Curve"]
            .apply(
                lambda x: isinstance(x, list),
            )
            .any()
        )

        assert DAM_ESR_AS_OFFERS_KEY in df_dict
        dam_esr_as_offers = df_dict[DAM_ESR_AS_OFFERS_KEY]

        assert dam_esr_as_offers.columns.tolist() == DAM_ESR_AS_OFFERS_COLUMNS
        assert len(dam_esr_as_offers) > 0

        # Verify no duplicates
        assert not dam_esr_as_offers.duplicated(
            subset=[
                "Interval Start",
                "Interval End",
                "QSE",
                "DME",
                "Resource Name",
            ],
        ).any()

        # AS Only Awards/Offers (ENG-3684/ENG-3688) landed in the bundle on the
        # same 2025-12-06 operating day as ESR, so we check them here too.
        assert DAM_AS_ONLY_AWARDS_KEY in df_dict
        assert DAM_AS_ONLY_OFFERS_KEY in df_dict

        _check_dam_as_only_awards(df_dict[DAM_AS_ONLY_AWARDS_KEY])
        _check_dam_as_only_offers(df_dict[DAM_AS_ONLY_OFFERS_KEY])

        # Also check the other datasets are still present
        check_60_day_dam_disclosure(df_dict)

    def _check_nonspin_offer_curve(self, df, column_name, dataset_name):
        """Verify a NONSPIN offer curve column has valid data."""
        assert df[column_name].notna().any(), (
            f"{column_name} should have non-null values in {dataset_name}"
        )
        curve = df[column_name].dropna().iloc[0]
        assert isinstance(curve, list)
        assert len(curve) > 0
        assert len(curve[0]) == 2

    def test_get_60_day_dam_disclosure_online_nonspin_offer_curves(self):
        """Test that ONLINE NONSPIN offer curves are correctly parsed.

        2025-12-12 data contains ONLINE NONSPIN price columns. Before the fix,
        split(" ")[1] would extract "ONLINE" instead of "ONLINE NONSPIN",
        causing the curves to be all NA. Checks all three AS offer datasets:
        gen, load, and ESR.
        """
        date = pd.Timestamp("2025-12-12").date()

        with api_vcr.use_cassette(
            "test_get_60_day_dam_disclosure_online_nonspin_offer_curves.yaml",
        ):
            df_dict = self.iso.get_60_day_dam_disclosure(
                date=date,
                process=True,
            )

        col = "ONLINE NONSPIN Offer Curve"

        gen_as_offers = df_dict[DAM_GEN_RESOURCE_AS_OFFERS_KEY]
        assert gen_as_offers.columns.tolist() == DAM_RESOURCE_AS_OFFERS_COLUMNS
        self._check_nonspin_offer_curve(
            gen_as_offers,
            col,
            "dam_gen_resource_as_offers",
        )

        load_as_offers = df_dict[DAM_LOAD_RESOURCE_AS_OFFERS_KEY]
        assert load_as_offers.columns.tolist() == DAM_RESOURCE_AS_OFFERS_COLUMNS
        self._check_nonspin_offer_curve(
            load_as_offers,
            col,
            "dam_load_resource_as_offers",
        )

        esr_as_offers = df_dict[DAM_ESR_AS_OFFERS_KEY]
        assert esr_as_offers.columns.tolist() == DAM_ESR_AS_OFFERS_COLUMNS
        self._check_nonspin_offer_curve(
            esr_as_offers,
            col,
            "dam_esr_as_offers",
        )

    def test_get_60_day_dam_disclosure_offline_nonspin_offer_curves(self):
        """Test that OFFLINE NONSPIN offer curves are correctly parsed.

        2025-12-12 data contains OFFLINE NONSPIN price columns. Before the fix,
        split(" ")[1] would extract "OFFLINE" instead of "OFFLINE NONSPIN",
        causing the curves to be all NA. Checks dam_gen_resource_as_offers
        which has OFFLINE NONSPIN data.
        """
        date = pd.Timestamp("2025-12-12").date()

        with api_vcr.use_cassette(
            "test_get_60_day_dam_disclosure_offline_nonspin_offer_curves.yaml",
        ):
            df_dict = self.iso.get_60_day_dam_disclosure(
                date=date,
                process=True,
            )

        col = "OFFLINE NONSPIN Offer Curve"

        gen_as_offers = df_dict[DAM_GEN_RESOURCE_AS_OFFERS_KEY]
        assert gen_as_offers.columns.tolist() == DAM_RESOURCE_AS_OFFERS_COLUMNS
        self._check_nonspin_offer_curve(
            gen_as_offers,
            col,
            "dam_gen_resource_as_offers",
        )

    @pytest.mark.integration
    def test_get_sara(self):
        columns = [
            "Unit Name",
            "Generation Interconnection Project Code",
            "Unit Code",
            "County",
            "Fuel",
            "Zone",
            "In Service Year",
            "Installed Capacity Rating",
            "Summer Capacity (MW)",
            "New Planned Project Additions to Report",
        ]
        df = self.iso.get_sara(verbose=True)
        assert df.shape[0] > 0
        assert df.columns.tolist() == columns

    @pytest.mark.integration
    def test_spp_real_time_parse_retry_file_name(self):
        assert parse_timestamp_from_friendly_name(
            "SPPHLZNP6905_retry_20230608_1545_csv",
        ) == pd.Timestamp("2023-06-08 15:45:00-0500", tz="US/Central")

        assert parse_timestamp_from_friendly_name(
            "SPPHLZNP6905_20230608_1545_csv",
        ) == pd.Timestamp("2023-06-08 15:45:00-0500", tz="US/Central")

    """get_unplanned_resource_outages"""

    def _check_unplanned_resource_outages(self, df):
        assert df.shape[0] >= 0

        assert df.columns.tolist() == [
            "Current As Of",
            "Publish Time",
            "Actual Outage Start",
            "Planned End Date",
            "Actual End Date",
            "Resource Name",
            "Resource Unit Code",
            "Fuel Type",
            "Outage Type",
            "Nature Of Work",
            "Available MW Maximum",
            "Available MW During Outage",
            "Effective MW Reduction Due to Outage",
        ]

        time_cols = [
            "Current As Of",
            "Publish Time",
            "Actual Outage Start",
            "Planned End Date",
            "Actual End Date",
        ]

        for col in time_cols:
            assert df[col].dt.tz.zone == self.iso.default_timezone

    @pytest.mark.integration
    def test_get_unplanned_resource_outages_historical_date(self):
        five_days_ago = self.local_start_of_today() - pd.DateOffset(days=5)
        df = self.iso.get_unplanned_resource_outages(date=five_days_ago)

        self._check_unplanned_resource_outages(df)

        assert df["Current As Of"].dt.date.unique() == [
            (five_days_ago - pd.DateOffset(days=3)).date(),
        ]
        assert df["Publish Time"].dt.date.unique() == [five_days_ago.date()]

    @pytest.mark.integration
    def test_get_unplanned_resource_outages_historical_range(self):
        start = self.local_start_of_today() - pd.DateOffset(6)

        df_2_days = self.iso.get_unplanned_resource_outages(
            start=start,
            end=start + pd.DateOffset(2),
        )

        self._check_unplanned_resource_outages(df_2_days)

        assert df_2_days["Current As Of"].dt.date.nunique() == 2
        assert (
            df_2_days["Current As Of"].min().date()
            == (start - pd.DateOffset(days=3)).date()
        )
        assert (
            df_2_days["Current As Of"].max().date()
            == (start - pd.DateOffset(days=2)).date()
        )

        assert df_2_days["Publish Time"].dt.date.nunique() == 2
        assert df_2_days["Publish Time"].min().date() == start.date()
        assert (
            df_2_days["Publish Time"].max().date() == (start + pd.DateOffset(1)).date()
        )

    """test get_highest_price_as_offer_selected"""

    def _check_highest_price_as_offer_selected(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "Market",
            "QSE",
            "DME",
            "Resource Name",
            "AS Type",
            "Block Indicator",
            "Offered Price",
            "Total Offered Quantity",
            "Offered Quantities",
        ]

    def test_get_highest_price_as_offer_selected_date_range(self):
        # This dataset ends on 2025-12-05 so pin the date
        date = pd.Timestamp("2025-12-05", tz=self.iso.default_timezone)

        with api_vcr.use_cassette(
            f"test_get_highest_price_as_offer_selected_date_range_{date}.yaml",
        ):
            df = self.iso.get_highest_price_as_offer_selected(
                start=date,
            )

        assert (df["Interval Start"].dt.date.unique() == [date.date()]).all()

        self._check_highest_price_as_offer_selected(df)

    @pytest.mark.skip("This test no longer works because the file has rolled off.")
    def test_get_highest_price_as_offer_selected_dst_end(self):
        dst_end_date = "2025-11-02"

        with api_vcr.use_cassette(
            f"test_get_highest_price_as_offer_selected_dst_end_{dst_end_date}.yaml",
        ):
            df = self.iso.get_highest_price_as_offer_selected(dst_end_date)

        assert df["Interval Start"].nunique() == 25
        assert "2025-11-02 01:00:00-05:00" in df["Interval Start"].astype(str).values
        assert "2025-11-02 01:00:00-06:00" in df["Interval Start"].astype(str).values

        self._check_highest_price_as_offer_selected(df)

    """get_highest_price_as_offer_selected_dam"""

    def _check_highest_price_as_offer_selected_dam(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "QSE",
            "DME",
            "Resource Name",
            "AS Type",
            "Block Indicator",
            "Offered Price",
            "Total Offered Quantity",
            "Offered Quantities",
        ]

        for col in ["Interval Start", "Interval End"]:
            assert df.dtypes[col] == "datetime64[ns, US/Central]"

        for col in [
            "QSE",
            "DME",
            "Resource Name",
            "AS Type",
            "Block Indicator",
            "Offered Quantities",
        ]:
            assert df.dtypes[col] == "object"

        for col in ["Offered Price", "Total Offered Quantity"]:
            assert df.dtypes[col] == "float64"

    def test_get_highest_price_as_offer_selected_dam(self):
        # Test the new DAM-specific method
        date = self.local_start_of_today() - pd.DateOffset(days=4)

        with api_vcr.use_cassette(
            f"test_get_highest_price_as_offer_selected_dam_{date}.yaml",
        ):
            df = self.iso.get_highest_price_as_offer_selected_dam(date)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.DateOffset(days=1, hours=-1)
        assert df["Interval Start"].nunique() == 24

        self._check_highest_price_as_offer_selected_dam(df)

    """get_highest_price_as_offer_selected_sced"""

    def _check_highest_price_as_offer_selected_sced(self, df):
        assert df.columns.tolist() == [
            "SCED Timestamp",
            "QSE",
            "DME",
            "Resource Name",
            "AS Type",
            "Offered Price",
            "Total Offered Quantity",
            "Offered Quantities",
        ]

        assert df.dtypes["SCED Timestamp"] == "datetime64[ns, US/Central]"

        for col in [
            "QSE",
            "DME",
            "Resource Name",
            "AS Type",
            "Offered Quantities",
        ]:
            assert df.dtypes[col] == "object"

        for col in ["Offered Price", "Total Offered Quantity"]:
            assert df.dtypes[col] == "float64"

    def test_get_highest_price_as_offer_selected_sced(self):
        date = self.local_start_of_today() - pd.DateOffset(days=4)

        with api_vcr.use_cassette(
            f"test_get_highest_price_as_offer_selected_sced_{date}.yaml",
        ):
            df = self.iso.get_highest_price_as_offer_selected_sced(date)

        # This is a SCED dataset so data points are not on 5 minute intervals
        assert df["SCED Timestamp"].nunique() >= 288
        assert df["SCED Timestamp"].dt.date.unique() == [date.date()]

        self._check_highest_price_as_offer_selected_sced(df)

    """get_3_day_highest_price_bids_selected_sced"""

    def _check_3_day_highest_price_bids_selected_sced(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "SCED Timestamp",
            "QSE",
            "DME",
            "Load Resource",
            "Highest Price Dispatched by SCED",
            "Proxy Extension",
        ]
        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"
        assert df.dtypes["SCED Timestamp"] == "datetime64[ns, US/Central]"
        for col in ["QSE", "DME", "Load Resource", "Proxy Extension"]:
            assert df.dtypes[col] == "object"
        assert df.dtypes["Highest Price Dispatched by SCED"] == "float64"
        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(minutes=5)
        ).all()
        assert set(df["Proxy Extension"].unique()).issubset({"Yes", "No"})

    def test_get_3_day_highest_price_bids_selected_sced(self):
        date = self.local_start_of_today() - pd.DateOffset(days=4)

        with api_vcr.use_cassette(
            f"test_get_3_day_highest_price_bids_selected_sced_{date}.yaml",
        ):
            df = self.iso.get_3_day_highest_price_bids_selected_sced(date)

        self._check_3_day_highest_price_bids_selected_sced(df)
        assert df["SCED Timestamp"].dt.date.unique() == [date.date()]

    """get_3_day_highest_price_offered_sced"""

    def _check_3_day_highest_price_offered_sced(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "SCED Timestamp",
            "QSE",
            "DME",
            "Generation Resource",
            "LMP",
            "Proxy Extension",
            "Power Balance Penalty Flag",
        ]
        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"
        assert df.dtypes["SCED Timestamp"] == "datetime64[ns, US/Central]"
        for col in [
            "QSE",
            "DME",
            "Generation Resource",
            "Proxy Extension",
            "Power Balance Penalty Flag",
        ]:
            assert df.dtypes[col] == "object"
        assert df.dtypes["LMP"] == "float64"
        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(minutes=5)
        ).all()
        assert set(df["Proxy Extension"].unique()).issubset({"Yes", "No"})
        assert set(df["Power Balance Penalty Flag"].unique()).issubset({"Yes", "No"})

    def test_get_3_day_highest_price_offered_sced(self):
        date = self.local_start_of_today() - pd.DateOffset(days=4)

        with api_vcr.use_cassette(
            f"test_get_3_day_highest_price_offered_sced_{date}.yaml",
        ):
            df = self.iso.get_3_day_highest_price_offered_sced(date)

        self._check_3_day_highest_price_offered_sced(df)
        assert df["SCED Timestamp"].dt.date.unique() == [date.date()]

    """test get_as_reports"""

    def test_get_as_reports(self):
        # This dataset stops on 2025-12-05 so we have to pin the date
        date = pd.Timestamp(
            "2025-12-05",
            tz=self.iso.default_timezone,
        )

        with api_vcr.use_cassette(
            f"test_get_as_reports_{date}.yaml",
        ):
            df = self.iso.get_as_reports(start=date)

        assert (df["Interval Start"].dt.date.unique() == [date.date()]).all()

        bid_curve_columns = [
            "Bid Curve - RRSPFR",
            "Bid Curve - RRSUFR",
            "Bid Curve - RRSFFR",
            "Bid Curve - ECRSM",
            "Bid Curve - ECRSS",
            "Bid Curve - REGUP",
            "Bid Curve - REGDN",
            "Bid Curve - ONNS",
            "Bid Curve - OFFNS",
        ]

        cols = [
            "Time",
            "Interval Start",
            "Interval End",
            "Total Cleared AS - RRSPFR",
            "Total Cleared AS - RRSUFR",
            "Total Cleared AS - RRSFFR",
            "Total Cleared AS - ECRSM",
            "Total Cleared AS - ECRSS",
            "Total Cleared AS - RegUp",
            "Total Cleared AS - RegDown",
            "Total Cleared AS - NonSpin",
            "Total Self-Arranged AS - RRSPFR",
            "Total Self-Arranged AS - RRSUFR",
            "Total Self-Arranged AS - RRSFFR",
            "Total Self-Arranged AS - ECRSM",
            "Total Self-Arranged AS - ECRSS",
            "Total Self-Arranged AS - RegUp",
            "Total Self-Arranged AS - RegDown",
            "Total Self-Arranged AS - NonSpin",
            "Total Self-Arranged AS - NSPNM",
        ] + bid_curve_columns

        assert df.columns.tolist() == cols

        for col in bid_curve_columns:
            # Check that the first non-null value is a list of lists
            first_non_null_value = df[col].dropna().iloc[0]
            assert isinstance(first_non_null_value, list)
            assert all(isinstance(x, list) for x in first_non_null_value)

    """get_as_reports_dam"""

    def _check_as_reports_dam(self, df):
        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"
        assert df.dtypes["AS Type"] == "object"
        assert df.dtypes["Cleared"] == "float64"
        assert df.dtypes["Self Arranged"] == "float64"
        assert df.dtypes["Offer Curve"] == "object"

        # Check that AS Type contains expected products
        expected_products = {
            "REGUP",
            "REGDN",
            "NSPIN",
            "ECRSS",
            "ECRSM",
            "NSPNM",
            "RRSFFR",
            "RRSPFR",
            "RRSUFR",
        }

        actual_products = set(df["AS Type"])
        assert expected_products == actual_products

        # Check that offer curves are lists of [MW, Price] pairs
        offer_curves = df["Offer Curve"].dropna()
        if len(offer_curves) > 0:
            first_curve = offer_curves.iloc[0]
            assert isinstance(first_curve, list)
            if first_curve:
                assert all(isinstance(x, list) and len(x) == 2 for x in first_curve)

    def test_get_as_reports_dam(self):
        """Test get_as_reports_dam method - long format with AS Type column"""
        start = self.local_start_of_today() - pd.Timedelta(days=4)

        with api_vcr.use_cassette(
            f"test_get_as_reports_dam_{start}.yaml",
        ):
            df = self.iso.get_as_reports_dam(start=start)

        self._check_as_reports_dam(df)

        assert df["Interval Start"].dt.date.unique() == start.date()

    """get_as_reports_sced"""

    def _check_as_reports_sced(self, df):
        assert df.dtypes["SCED Timestamp"] == "datetime64[ns, US/Central]"
        assert df.dtypes["AS Type"] == "object"
        assert df.dtypes["Offer Curve"] == "object"

        # Check that AS Type contains expected products
        expected_products = {
            "REGUP",
            "REGDN",
            "NSPIN",
            "ECRSS",
            "ECRSM",
            "NSPNM",
            "RRSFFR",
            "RRSPFR",
            "RRSUFR",
        }
        actual_products = set(df["AS Type"])
        assert expected_products == actual_products

        # Check that offer curves are lists of [MW, Price] pairs
        first_curve = df["Offer Curve"].iloc[0]
        assert isinstance(first_curve, list)
        if first_curve:
            assert all(isinstance(x, list) and len(x) == 2 for x in first_curve)

    def test_get_as_reports_sced(self):
        """Test get_as_reports_sced method for SCED ancillary service offers"""
        # SCED AS reports started on December 5, 2025
        # Use a date after that with the 2-day delay
        test_date = self.local_start_of_today() - pd.Timedelta(days=2)

        with api_vcr.use_cassette(
            f"test_get_as_reports_sced_{test_date}.yaml",
        ):
            df = self.iso.get_as_reports_sced(date=test_date)

        self._check_as_reports_sced(df)

        assert df["SCED Timestamp"].dt.date.unique() == test_date.date()

    """get_reported_outages"""

    @pytest.mark.integration
    def test_get_reported_outages(self):
        df = self.iso.get_reported_outages()

        assert df.columns.tolist() == [
            "Time",
            "Combined Unplanned",
            "Combined Planned",
            "Combined Total",
            "Dispatchable Unplanned",
            "Dispatchable Planned",
            "Dispatchable Total",
            "Renewable Unplanned",
            "Renewable Planned",
            "Renewable Total",
        ]

        assert df["Time"].min() <= self.local_start_of_today() - pd.Timedelta(
            # Add the minutes because the times do not line up exactly on the hour
            days=6,
            minutes=-5,
        )

        assert df["Time"].max() >= self.local_start_of_today()

        assert (
            df["Combined Total"] == (df["Combined Unplanned"] + df["Combined Planned"])
        ).all()

        assert (
            df["Dispatchable Total"]
            == (df["Dispatchable Unplanned"] + df["Dispatchable Planned"])
        ).all()

        assert (
            df["Renewable Total"]
            == (df["Renewable Unplanned"] + df["Renewable Planned"])
        ).all()

    """get_hourly_resource_outage_capacity"""

    @pytest.mark.integration
    def test_get_hourly_resource_outage_capacity(self):
        cols = [
            "Publish Time",
            "Time",
            "Interval Start",
            "Interval End",
            "Total Resource MW Zone South",
            "Total Resource MW Zone North",
            "Total Resource MW Zone West",
            "Total Resource MW Zone Houston",
            "Total Resource MW",
            "Total IRR MW Zone South",
            "Total IRR MW Zone North",
            "Total IRR MW Zone West",
            "Total IRR MW Zone Houston",
            "Total IRR MW",
            "Total New Equip Resource MW Zone South",
            "Total New Equip Resource MW Zone North",
            "Total New Equip Resource MW Zone West",
            "Total New Equip Resource MW Zone Houston",
            "Total New Equip Resource MW",
        ]

        # test specific hour
        date = pd.Timestamp.now(tz=self.iso.default_timezone) - pd.Timedelta(
            days=1,
        )
        df = self.iso.get_hourly_resource_outage_capacity(date)

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        # test latest and confirm published in last 2 hours
        df = self.iso.get_hourly_resource_outage_capacity("latest")
        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        assert df["Publish Time"].min() >= pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ) - pd.Timedelta(hours=2)

        # test date range
        end = date.floor("h")
        start = end - pd.Timedelta(
            hours=3,
        )
        df = self.iso.get_hourly_resource_outage_capacity(
            start=start,
            end=end,
            verbose=True,
        )

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols
        assert df["Publish Time"].nunique() == 3

    """get_wind_actual_and_forecast_hourly"""

    def _check_hourly_wind_report(self, df, geographic_data=False):
        assert (
            df.columns.tolist() == WIND_ACTUAL_AND_FORECAST_COLUMNS
            if not geographic_data
            else WIND_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS
        )

        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()

    @pytest.mark.integration
    def test_get_wind_actual_and_forecast_hourly_today(self):
        df = self.iso.get_wind_actual_and_forecast_hourly("today", verbose=True)

        self._check_hourly_wind_report(df)

        hours_since_local_midnight = (
            self.local_now() - self.local_start_of_today()
        ) // pd.Timedelta(hours=1)

        assert df["Publish Time"].nunique() == hours_since_local_midnight

    @pytest.mark.integration
    def test_get_wind_actual_and_forecast_hourly_latest(self):
        df = self.iso.get_wind_actual_and_forecast_hourly("latest", verbose=True)

        self._check_hourly_wind_report(df)

        assert df["Publish Time"].nunique() == 1

    @pytest.mark.integration
    def test_get_wind_actual_and_forecast_hourly_historical_date(self):
        date = self.local_today() - pd.Timedelta(days=1)
        df = self.iso.get_wind_actual_and_forecast_hourly(date, verbose=True)

        self._check_hourly_wind_report(df)

        assert df["Publish Time"].nunique() == 24  # One for each hour
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    @pytest.mark.integration
    def test_get_wind_actual_and_forecast_hourly_historical_date_range(self):
        start = self.local_today() - pd.Timedelta(days=3)
        end = self.local_today() - pd.Timedelta(days=1)
        df = self.iso.get_wind_actual_and_forecast_hourly(start, end, verbose=True)

        self._check_hourly_wind_report(df)

        assert df["Publish Time"].nunique() == 48
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    """get_wind_actual_and_forecast_by_geographical_region_hourly"""

    def test_get_wind_actual_and_forecast_by_geographical_region_hourly_today(self):
        with api_vcr.use_cassette(
            "test_get_wind_actual_and_forecast_by_geographical_region_hourly_today.yaml",
        ):
            df = self.iso.get_wind_actual_and_forecast_by_geographical_region_hourly(
                "today",
                verbose=True,
            )

        self._check_hourly_wind_report(df, geographic_data=True)

        hours_since_local_midnight = (
            self.local_now() - self.local_start_of_today()
        ) // pd.Timedelta(hours=1)

        assert df["Publish Time"].nunique() == hours_since_local_midnight

    def test_get_wind_actual_and_forecast_by_geographical_region_hourly_historical_date_range(  # noqa: E501
        self,
    ):
        start = self.local_today() - pd.Timedelta(days=3)
        end = self.local_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(
            f"test_get_wind_actual_and_forecast_by_geographical_region_hourly_historical_date_range_{start}_{end}.yaml",  # noqa: E501
        ):
            df = self.iso.get_wind_actual_and_forecast_by_geographical_region_hourly(
                start,
                end,
                verbose=True,
            )

        self._check_hourly_wind_report(df, geographic_data=True)

        assert df["Publish Time"].nunique() == 48
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    """get_solar_actual_and_forecast_hourly"""

    def test_get_solar_actual_and_forecast_hourly_today(self):
        with api_vcr.use_cassette(
            "test_get_solar_actual_and_forecast_hourly_today.yaml",
        ):
            df = self.iso.get_solar_actual_and_forecast_hourly("today", verbose=True)

        self._check_hourly_solar_report(df)

        hours_since_local_midnight = (
            self.local_now() - self.local_start_of_today()
        ) // pd.Timedelta(hours=1)

        assert df["Publish Time"].nunique() == hours_since_local_midnight

    def test_get_solar_actual_and_forecast_hourly_historical_date_range(self):
        start = self.local_today() - pd.Timedelta(days=3)
        end = self.local_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(
            f"test_get_solar_actual_and_forecast_hourly_historical_date_range_{start}_{end}.yaml",  # noqa: E501
        ):
            df = self.iso.get_solar_actual_and_forecast_hourly(start, end, verbose=True)

        self._check_hourly_solar_report(df)

        assert df["Publish Time"].nunique() == 48
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    """get_solar_actual_and_forecast_by_geographical_region_hourly"""

    def _check_hourly_solar_report(self, df, geographic_data=False):
        assert (
            df.columns.tolist() == SOLAR_ACTUAL_AND_FORECAST_COLUMNS
            if not geographic_data
            else SOLAR_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS
        )
        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()

    @pytest.mark.integration
    def test_get_solar_actual_and_forecast_by_geographical_region_hourly_today(self):
        df = self.iso.get_solar_actual_and_forecast_by_geographical_region_hourly(
            "today",
            verbose=True,
        )

        self._check_hourly_solar_report(df, geographic_data=True)

        hours_since_local_midnight = (
            self.local_now() - self.local_start_of_today()
        ) // pd.Timedelta(hours=1)

        assert df["Publish Time"].nunique() == hours_since_local_midnight

    @pytest.mark.integration
    def test_get_solar_actual_and_forecast_by_geographical_region_hourly_latest(self):
        df = self.iso.get_solar_actual_and_forecast_by_geographical_region_hourly(
            "latest",
            verbose=True,
        )

        self._check_hourly_solar_report(df, geographic_data=True)

        assert df["Publish Time"].nunique() == 1

    @pytest.mark.integration
    def test_get_solar_actual_and_forecast_by_geographical_region_hourly_historical_date(  # noqa: E501
        self,
    ):
        date = self.local_today() - pd.Timedelta(days=1)
        df = self.iso.get_solar_actual_and_forecast_by_geographical_region_hourly(
            date,
            verbose=True,
        )

        self._check_hourly_solar_report(df, geographic_data=True)

        assert df["Publish Time"].nunique() == 24  # One for each hour
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    @pytest.mark.integration
    def test_get_solar_actual_and_forecast_by_geographical_region_hourly_historical_date_range(
        self,
    ):
        start = self.local_today() - pd.Timedelta(days=3)
        end = self.local_today() - pd.Timedelta(days=1)
        df = self.iso.get_solar_actual_and_forecast_by_geographical_region_hourly(
            start,
            end,
            verbose=True,
        )

        self._check_hourly_solar_report(df, geographic_data=True)

        assert df["Publish Time"].nunique() == 48
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    """get_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

    """get_price_corrections"""

    @pytest.mark.integration
    def test_get_rtm_price_corrections(self):
        df = self.iso.get_rtm_price_corrections(rtm_type="RTM_SPP")

        cols = [
            "Price Correction Time",
            "Interval Start",
            "Interval End",
            "Location",
            "Location Type",
            "SPP Original",
            "SPP Corrected",
        ]

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

    # TODO: this url has no DocumentList
    # https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=13044
    @pytest.mark.skip(reason="Failing")
    @pytest.mark.integration
    def test_get_dam_price_corrections(self):
        df = self.iso.get_dam_price_corrections(dam_type="DAM_SPP")

        cols = [
            "Price Correction Time",
            "Interval Start",
            "Interval End",
            "Location",
            "Location Type",
            "SPP Original",
            "SPP Corrected",
        ]

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

    @pytest.mark.integration
    def test_get_mcpc_dam_price_corrections(self):
        """Test DAM AS Price Corrections (MCPC)."""
        df = self.iso.get_mcpc_dam_price_corrections()

        cols = [
            "Price Correction Time",
            "Interval Start",
            "Interval End",
            "AS Type",
            "MCPC Original",
            "MCPC Corrected",
        ]

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        assert pd.api.types.is_datetime64_any_dtype(df["Price Correction Time"])
        assert pd.api.types.is_datetime64_any_dtype(df["Interval Start"])
        assert pd.api.types.is_datetime64_any_dtype(df["Interval End"])
        assert pd.api.types.is_object_dtype(df["AS Type"])
        assert pd.api.types.is_float_dtype(df["MCPC Original"])
        assert pd.api.types.is_float_dtype(df["MCPC Corrected"])

    """get_system_wide_actuals"""

    @pytest.mark.integration
    def test_get_system_wide_actual_load_for_date(self):
        yesterday = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=1,
        )
        df = self.iso.get_system_wide_actual_load(yesterday)

        # 1 Hour of data
        assert df.shape[0] == 4
        assert df["Interval Start"].min() == pd.Timestamp(
            yesterday,
            tz=self.iso.default_timezone,
        )

        cols = ["Time", "Interval Start", "Interval End", "Demand"]
        assert df.columns.tolist() == cols

    @pytest.mark.integration
    def test_get_system_wide_actual_load_date_range(self):
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        two_days_ago = today - pd.Timedelta(days=2)

        df = self.iso.get_system_wide_actual_load(
            start=two_days_ago,
            end=today,
            verbose=True,
        )

        cols = ["Time", "Interval Start", "Interval End", "Demand"]

        assert df["Interval Start"].min() == pd.Timestamp(
            two_days_ago,
            tz=self.iso.default_timezone,
        )
        assert df["Interval Start"].max() == pd.Timestamp(
            today,
            tz=self.iso.default_timezone,
        ) - pd.Timedelta(minutes=15)
        assert df.columns.tolist() == cols

    @pytest.mark.integration
    def test_get_system_wide_actual_load_today(self):
        df = self.iso.get_system_wide_actual_load("today")

        cols = ["Time", "Interval Start", "Interval End", "Demand"]

        assert df["Interval Start"].min() == pd.Timestamp(
            pd.Timestamp.now(tz=self.iso.default_timezone).date(),
            tz=self.iso.default_timezone,
        )
        # 1 Hour of data
        assert df.shape[0] == 4
        assert df.columns.tolist() == cols

    @pytest.mark.integration
    def test_get_system_wide_actual_load_latest(self):
        df = self.iso.get_system_wide_actual_load("latest")

        cols = ["Time", "Interval Start", "Interval End", "Demand"]

        assert df["Interval Start"].min() == pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).floor("h") - pd.Timedelta(hours=1)

        # 1 Hour of data
        assert df.shape[0] == 4
        assert df.columns.tolist() == cols

    """get_short_term_system_adequacy"""

    def _check_short_term_system_adequacy(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Capacity Generation Resource South",
            "Capacity Generation Resource North",
            "Capacity Generation Resource West",
            "Capacity Generation Resource Houston",
            "Capacity Load Resource South",
            "Capacity Load Resource North",
            "Capacity Load Resource West",
            "Capacity Load Resource Houston",
            "Offline Available MW South",
            "Offline Available MW North",
            "Offline Available MW West",
            "Offline Available MW Houston",
            "Available Capacity Generation",
            "Available Capacity Reserve",
            "Capacity Generation Resource Total",
            "Capacity Load Resource Total",
            "Offline Available MW Total",
            "Capacity Reg Up Total",
            "Capacity Reg Down Total",
            "Capacity RRS Total",
            "Capacity ECRS Total",
            "Capacity NSPIN Total",
            "Capacity Reg Up RRS Total",
            "Capacity Reg Up RRS ECRS Total",
            "Capacity Reg Up RRS ECRS NSPIN Total",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

    @pytest.mark.integration
    def test_get_short_term_system_adequacy_today(self):
        df = self.iso.get_short_term_system_adequacy("today")

        self._check_short_term_system_adequacy(df)

        # At least one published per hour
        assert (
            df["Publish Time"].nunique()
            >= (self.local_now() - self.local_start_of_today()).total_seconds() // 3600
        )
        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.DateOffset(
            days=7,
        )

    @pytest.mark.integration
    def test_get_short_term_system_adequacy_latest(self):
        df = self.iso.get_short_term_system_adequacy("latest")

        self._check_short_term_system_adequacy(df)

        assert df["Publish Time"].nunique() == 1

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.DateOffset(
            days=7,
        )

    @pytest.mark.integration
    def test_get_short_term_system_adequacy_historical_date(self):
        date = self.local_today() - pd.DateOffset(days=15)
        df = self.iso.get_short_term_system_adequacy(date)

        assert df["Publish Time"].nunique() >= 24

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval End"].max() == self.local_start_of_day(
            date,
        ) + pd.DateOffset(days=7)

        self._check_short_term_system_adequacy(df)

    @pytest.mark.integration
    def test_get_short_term_system_adequacy_historical_range(self):
        start = self.local_today() - pd.DateOffset(days=15)
        end = self.local_today() - pd.DateOffset(days=14)
        df = self.iso.get_short_term_system_adequacy(
            start=start,
            end=end,
        )

        assert df["Publish Time"].nunique() >= 24
        assert df["Interval Start"].min() == self.local_start_of_day(start)
        assert df["Interval End"].max() == self.local_start_of_day(end) + pd.DateOffset(
            days=6,
        )

        self._check_short_term_system_adequacy(df)

    """get_real_time_adders_and_reserves"""

    def _check_real_time_adders_and_reserves(self, df):
        assert df.columns.tolist() == [
            "SCED Timestamp",
            "Interval Start",
            "Interval End",
            "BatchID",
            "System Lambda",
            "PRC",
            "RTORPA",
            "RTOFFPA",
            "RTOLCAP",
            "RTOFFCAP",
            "RTOLHSL",
            "RTBP",
            "RTCLRCAP",
            "RTCLRREG",
            "RTCLRBP",
            "RTCLRLSL",
            "RTCLRNS",
            "RTNCLRRRS",
            "RTOLNSRS",
            "RTCST30HSL",
            "RTOFFNSHSL",
            "RTRUCCST30HSL",
            "RTORDPA",
            "RTRRUC",
            "RTRRMR",
            "RTDNCLR",
            "RTDERS",
            "RTDCTIEIMPORT",
            "RTDCTIEEXPORT",
            "RTBLTIMPORT",
            "RTBLTEXPORT",
            "RTOLLASL",
            "RTOLHASL",
            "RTNCLRNSCAP",
            "RTNCLRECRS",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()

    @pytest.mark.integration
    def test_get_real_time_adders_and_reserves_today(self):
        df = self.iso.get_real_time_adders_and_reserves("today")

        self._check_real_time_adders_and_reserves(df)

        hours_since_start_of_day = (
            self.local_now() - self.local_start_of_today()
            # Integer division
        ) // pd.Timedelta(hours=1)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert (
            len(df)
            >= hours_since_start_of_day * INTERVALS_PER_HOUR_AT_FIVE_MINUTE_RESOLUTION
        )

    @pytest.mark.integration
    def test_get_real_time_adders_and_reserves_latest(self):
        df = self.iso.get_real_time_adders_and_reserves("latest")

        self._check_real_time_adders_and_reserves(df)

        assert len(df) == 1

    @pytest.mark.integration
    def test_get_real_time_adders_and_reserves_historical(self):
        date = self.local_today() - pd.DateOffset(days=3)
        df = self.iso.get_real_time_adders_and_reserves(date)

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval End"].max() == self.local_start_of_day(
            date,
        ) + pd.DateOffset(days=1)

        self._check_real_time_adders_and_reserves(df)

        assert len(df) >= 24 * INTERVALS_PER_HOUR_AT_FIVE_MINUTE_RESOLUTION

    @pytest.mark.integration
    def test_get_real_time_adders_and_reserves_historical_range(self):
        start = self.local_today() - pd.DateOffset(days=4)
        end = self.local_today() - pd.DateOffset(days=2)
        df = self.iso.get_real_time_adders_and_reserves(
            start=start,
            end=end,
        )

        assert df["Interval Start"].min() == self.local_start_of_day(start)
        assert df["Interval End"].max() == self.local_start_of_day(end)

        self._check_real_time_adders_and_reserves(df)

        assert len(df) >= 24 * INTERVALS_PER_HOUR_AT_FIVE_MINUTE_RESOLUTION * 2

    """get_temperature_forecast_by_weather_zone"""

    def _check_temperature_forecast_by_weather_zone(self, df):
        assert (
            df.columns.tolist()
            == [
                "Interval Start",
                "Interval End",
                "Publish Time",
            ]
            + self.weather_zone_columns
        )

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

    temperature_forecast_start_offset = -pd.DateOffset(days=3)
    temperature_forecast_end_offset = pd.DateOffset(days=9)

    @pytest.mark.integration
    def test_get_temperature_forecast_by_weather_zone_today_and_latest(self):
        df = self.iso.get_temperature_forecast_by_weather_zone("today")
        self._check_temperature_forecast_by_weather_zone(df)

        # One publish time
        assert df["Publish Time"].nunique() == 1

        # Data goes into the past 3 days.
        assert (
            df["Interval Start"].min()
            == self.local_start_of_today() + self.temperature_forecast_start_offset
        )

        assert (
            df["Interval End"].max()
            == self.local_start_of_today() + self.temperature_forecast_end_offset
        )

        assert self.iso.get_temperature_forecast_by_weather_zone("latest").equals(df)

    @pytest.mark.integration
    def test_get_temperature_forecast_by_weather_zone_historical_date(self):
        date = self.local_today() - pd.DateOffset(days=22)
        df = self.iso.get_temperature_forecast_by_weather_zone(date)

        assert df["Publish Time"].nunique() == 1

        assert (
            df["Interval Start"].min()
            == self.local_start_of_day(date) + self.temperature_forecast_start_offset
        )

        assert (
            df["Interval End"].max()
            == self.local_start_of_day(
                date,
            )
            + self.temperature_forecast_end_offset
        )

        self._check_temperature_forecast_by_weather_zone(df)

    @pytest.mark.integration
    def test_get_temperature_forecast_by_weather_zone_historical_range(self):
        start = self.local_today() - pd.DateOffset(days=24)
        end = self.local_today() - pd.DateOffset(days=21)

        df = self.iso.get_temperature_forecast_by_weather_zone(
            start=start,
            end=end,
        )

        assert df["Publish Time"].nunique() == 3
        assert (
            df["Interval Start"].min()
            == self.local_start_of_day(start) + self.temperature_forecast_start_offset
        )

        assert df["Interval End"].max() == self.local_start_of_day(
            end,
            # Non-inclusive end date
        ) + self.temperature_forecast_end_offset - pd.DateOffset(days=1)

        self._check_temperature_forecast_by_weather_zone(df)

    def test_get_temperature_forecast_by_weather_zone_dst_end_2025(self):
        # This forecast date includes 2025-11-02, DST end
        with api_vcr.use_cassette(
            "test_get_temperature_forecast_by_weather_zone_dst_end_2025.yaml",
        ):
            df = self.iso.get_temperature_forecast_by_weather_zone("2025-10-26")

        self._check_temperature_forecast_by_weather_zone(df)

        # Check for the presence of the repeated hour
        assert (
            pd.Timestamp("2025-11-02 01:00:00-0500", tz="US/Central")
            == df["Interval Start"].iloc[-48]
        )

        assert (
            pd.Timestamp("2025-11-02 01:00:00-0600", tz="US/Central")
            == df["Interval Start"].iloc[-47]
        )

    """parse_doc"""

    def test_parse_doc_works_on_dst_data(self):
        data_string = """DeliveryDate,TimeEnding,Demand,DSTFlag
        03/13/2016,01:15,26362.1563,N
        03/13/2016,01:30,26123.679,N
        03/13/2016,01:45,25879.7454,N
        03/13/2016,03:00,25668.166,N
        """
        # Read the data into a DataFrame
        df = pd.read_csv(StringIO(data_string))

        df = self.iso.parse_doc(df)

        assert df["Interval Start"].min() == pd.Timestamp(
            "2016-03-13 01:00:00-0600",
            tz="US/Central",
        )
        assert df["Interval Start"].max() == pd.Timestamp(
            "2016-03-13 01:45:00-0600",
            tz="US/Central",
        )

        assert df["Interval End"].min() == pd.Timestamp(
            "2016-03-13 01:15:00-0600",
            tz="US/Central",
        )
        # Note the hour jump due to DST
        assert df["Interval End"].max() == pd.Timestamp(
            "2016-03-13 03:00:00-0500",
            tz="US/Central",
        )

    def test_parse_doc_works_on_dst_end(self):
        data_string = """DeliveryDate,TimeEnding,Demand,DSTFlag
        11/06/2016,01:15,28907.1315,N
        11/06/2016,01:30,28595.5918,N
        11/06/2016,01:45,28266.6354,N
        11/06/2016,01:00,28057.502,N
        11/06/2016,01:15,27707.4798,Y
        11/06/2016,01:30,27396.1973,Y
        11/06/2016,01:45,27157.3464,Y
        11/06/2016,02:00,26981.778,Y
        """

        df = pd.read_csv(StringIO(data_string))

        df = self.iso.parse_doc(df)

        assert df["Interval Start"].min() == pd.Timestamp(
            "2016-11-06 00:45:00-0500",
            tz="US/Central",
        )

        assert df["Interval Start"].max() == pd.Timestamp(
            "2016-11-06 01:45:00-0600",
            tz="US/Central",
        )

        assert df["Interval End"].min() == pd.Timestamp(
            "2016-11-06 01:00:00-0500",
            tz="US/Central",
        )

        assert df["Interval End"].max() == pd.Timestamp(
            "2016-11-06 02:00:00-0600",
            tz="US/Central",
        )

    def test_parse_doc_delivery_interval_timedelta(self):
        """Regression test for #227: parse_doc must handle DeliveryInterval
        data without using timedelta64[h] (unsupported in pandas >=2.0)."""
        data_string = """DeliveryDate,DeliveryHour,DeliveryInterval,SettlementPointName,SettlementPointType,SettlementPointPrice,DSTFlag
01/15/2023,1,1,HB_HOUSTON,HU,25.50,N
01/15/2023,1,2,HB_HOUSTON,HU,26.00,N
01/15/2023,1,3,HB_HOUSTON,HU,24.75,N
01/15/2023,1,4,HB_HOUSTON,HU,25.25,N
01/15/2023,2,1,HB_HOUSTON,HU,23.00,N
"""
        df = pd.read_csv(StringIO(data_string))
        df = self.iso.parse_doc(df)

        assert "Interval Start" in df.columns
        assert len(df) == 5

        # First interval: hour 0 (HourBeginning = DeliveryHour - 1 = 0),
        # interval 1 -> 00:00 CT
        assert df["Interval Start"].iloc[0] == pd.Timestamp(
            "2023-01-15 00:00:00-0600",
            tz="US/Central",
        )
        # Second interval of hour 1: 00:15 CT
        assert df["Interval Start"].iloc[1] == pd.Timestamp(
            "2023-01-15 00:15:00-0600",
            tz="US/Central",
        )
        # First interval of hour 2: 01:00 CT
        assert df["Interval Start"].iloc[4] == pd.Timestamp(
            "2023-01-15 01:00:00-0600",
            tz="US/Central",
        )

    """get_lmp"""

    @pytest.mark.integration
    def test_get_lmp_electrical_bus(self):
        cols = [
            "Interval Start",
            "Interval End",
            "SCED Timestamp",
            "Market",
            "Location",
            "Location Type",
            "LMP",
        ]

        df = self.iso.get_lmp(
            date="latest",
            location_type=ELECTRICAL_BUS_LOCATION_TYPE,
        )

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        now = pd.Timestamp.now(tz=self.iso.default_timezone)
        start = now - pd.Timedelta(hours=1)
        df = self.iso.get_lmp(
            location_type=ELECTRICAL_BUS_LOCATION_TYPE,
            start=start,
            end=now,
            verbose=True,
        )

        # There should be at least 12 intervals in the last hour
        # sometimes there are more if sced is run more frequently
        # subtracting 1 to allow for some flexibility
        assert df["SCED Timestamp"].nunique() >= 12 - 1

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols
        assert df["SCED Timestamp"].min() >= start
        assert df["SCED Timestamp"].max() <= now

    @pytest.mark.integration
    def test_get_lmp_settlement_point(self):
        df = self.iso.get_lmp(
            date="latest",
            location_type="Settlement Point",
        )

        cols = [
            "Interval Start",
            "Interval End",
            "SCED Timestamp",
            "Market",
            "Location",
            "Location Type",
            "LMP",
        ]

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        assert (df["Interval Start"] == df["SCED Timestamp"].dt.floor("5min")).all()
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()

    def test_get_lmp_settlement_point_uses_mapping(self):
        with api_vcr.use_cassette("test_get_lmp_settlement_point_uses_mapping.yaml"):
            df = self.iso.get_lmp(
                date="today",
                location_type="Settlement Point",
                verbose=True,
            )
        cols = [
            "Interval Start",
            "Interval End",
            "SCED Timestamp",
            "Market",
            "Location",
            "Location Type",
            "LMP",
        ]
        assert df.columns.tolist() == cols
        assert df.shape[0] >= 0
        assert df["Location Type"].notna().all()
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()

    """get_lmp_by_bus_dam"""

    expected_lmp_by_bus_dam_columns = [
        "Interval Start",
        "Interval End",
        "Market",
        "Location",
        "Location Type",
        "LMP",
    ]

    def _check_lmp_by_bus_dam(self, df):
        assert df.columns.tolist() == self.expected_lmp_by_bus_dam_columns
        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"
        assert (df["Market"] == Markets.DAY_AHEAD_HOURLY.value).all()
        assert (df["Location Type"] == ELECTRICAL_BUS_LOCATION_TYPE).all()
        assert df.dtypes["LMP"] == "float64"
        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()

    def test_get_lmp_by_bus_dam_today(self):
        with api_vcr.use_cassette("test_get_lmp_by_bus_dam_today.yaml"):
            df = self.iso.get_lmp_by_bus_dam("today", verbose=True)
        self._check_lmp_by_bus_dam(df)
        assert df.shape[0] > 0

    def test_get_lmp_by_bus_dam_latest(self):
        with api_vcr.use_cassette("test_get_lmp_by_bus_dam_latest.yaml"):
            df = self.iso.get_lmp_by_bus_dam("latest", verbose=True)
        self._check_lmp_by_bus_dam(df)
        assert df.shape[0] > 0

    def test_get_lmp_by_bus_dam_historical(self):
        date = pd.Timestamp("2026-03-05", tz=self.iso.default_timezone)
        with api_vcr.use_cassette("test_get_lmp_by_bus_dam_historical.yaml"):
            df = self.iso.get_lmp_by_bus_dam(date, verbose=True)
        self._check_lmp_by_bus_dam(df)
        assert df["Interval Start"].min() == date
        assert df["Interval End"].max() == date + pd.DateOffset(days=1)

    def test_get_lmp_by_bus_dam_date_range(self):
        start = pd.Timestamp("2026-03-04", tz=self.iso.default_timezone)
        end = pd.Timestamp("2026-03-06", tz=self.iso.default_timezone)
        with api_vcr.use_cassette("test_get_lmp_by_bus_dam_date_range.yaml"):
            df = self.iso.get_lmp_by_bus_dam(start, end=end, verbose=True)
        self._check_lmp_by_bus_dam(df)
        assert df["Interval Start"].min() == start
        assert df["Interval End"].max() == end

    def test_read_docs_return_empty_df(self):
        df = self.iso.read_docs(docs=[], empty_df=pd.DataFrame(columns=["test"]))

        assert df.shape[0] == 0
        assert df.columns.tolist() == ["test"]

    @staticmethod
    def _check_ercot_spp(df, market, location_type):
        """Common checks for SPP data:
        - Columns
        - One Market
        - One Location Type
        """
        cols = [
            "Time",
            "Interval Start",
            "Interval End",
            "Location",
            "Location Type",
            "Market",
            "SPP",
        ]
        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols
        markets = df["Market"].unique()
        assert len(markets) == 1
        assert markets[0] == market.value

        location_types = df["Location Type"].unique()
        assert len(location_types) == 1
        assert location_types[0] == location_type

    def _check_dam_system_lambda(self, df):
        cols = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Market",
            "System Lambda",
        ]
        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        assert df["System Lambda"].dtype == float

    def test_get_documents_raises_exception_when_no_docs(self):
        with pytest.raises(NoDataFoundException):
            self.iso.get_load_forecast("2010-01-01")

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "date, end",
        [
            (
                pd.Timestamp.now().normalize() - pd.Timedelta(hours=1),
                pd.Timestamp.now().normalize(),
            ),
        ],
    )
    def test_get_indicative_lmp_by_settlement_point(self, date, end):
        with api_vcr.use_cassette(
            f"test_get_indicative_lmp_historical_{date}_{end}.yaml",
            record_mode="all",  # NOTE(kladar) Relative parameters and fixtures don't play nicely together yet,
            # so always record new interactions
        ):
            df = self.iso.get_indicative_lmp_by_settlement_point(date, end)

            assert df.columns.tolist() == [
                "RTD Timestamp",
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "LMP",
            ]

            assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
            assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"
            assert df.dtypes["LMP"] == "float64"
            assert (
                (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(minutes=5)
            ).all()
            assert df["Interval Start"].min() == date.tz_localize(
                self.iso.default_timezone,
            )
            assert df["Interval End"].max() == end.tz_localize(
                self.iso.default_timezone,
            ) + pd.Timedelta(minutes=50)

    """get_dam_total_energy_purchased"""

    def _check_dam_total_energy_purchased(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Location",
            "Total",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()
        assert df["Total"].dtype == float
        assert df["Location"].dtype == object

    def test_get_dam_total_energy_purchased_today(self):
        with api_vcr.use_cassette(
            "test_get_dam_total_energy_purchased_today.yaml",
        ):
            df = self.iso.get_dam_total_energy_purchased("today")

        self._check_dam_total_energy_purchased(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df[
            "Interval Start"
        ].max() == self.local_start_of_today() + pd.DateOffset(days=1, hours=-1)

    def test_get_dam_total_energy_purchased_historical_date_range(self):
        start = self.local_today() - pd.DateOffset(days=8)
        end = start + pd.DateOffset(days=2)

        with api_vcr.use_cassette(
            f"test_get_dam_total_energy_purchased_historical_date_range_{start}_{end}.yaml",
        ):
            df = self.iso.get_dam_total_energy_purchased(start, end)

        self._check_dam_total_energy_purchased(df)

        assert df["Interval Start"].min() == self.local_start_of_day(start)
        assert df["Interval Start"].max() == self.local_start_of_day(
            end,
        ) - pd.DateOffset(
            hours=1,
        )

    """get_dam_total_energy_sold"""

    def _check_dam_total_energy_sold(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Location",
            "Total",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()
        assert df["Total"].dtype == float
        assert df["Location"].dtype == object

    def test_get_dam_total_energy_sold_today(self):
        with api_vcr.use_cassette(
            "test_get_dam_total_energy_sold_today.yaml",
        ):
            df = self.iso.get_dam_total_energy_sold("today")

        self._check_dam_total_energy_sold(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df[
            "Interval Start"
        ].max() == self.local_start_of_today() + pd.DateOffset(days=1, hours=-1)

    def test_get_dam_total_energy_sold_historical_date_range(self):
        start = self.local_today() - pd.DateOffset(days=15)
        end = start + pd.DateOffset(days=2)

        with api_vcr.use_cassette(
            f"test_get_dam_total_energy_sold_historical_date_range_{start}_{end}.yaml",
        ):
            df = self.iso.get_dam_total_energy_sold(start, end)

        self._check_dam_total_energy_sold(df)

        assert df["Interval Start"].min() == self.local_start_of_day(start)
        assert df["Interval Start"].max() == self.local_start_of_day(
            end,
        ) - pd.DateOffset(hours=1)

    """get_cop_adjustment_period_snapshot_60_day"""

    def _check_cop_adjustment_period_snapshot_60_day(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Resource Name",
            "QSE",
            "Status",
            "High Sustained Limit",
            "Low Sustained Limit",
            "High Emergency Limit",
            "Low Emergency Limit",
            "Reg Up",
            "Reg Down",
            "RRS",
            "RRSPFR",
            "RRSFFR",
            "RRSUFR",
            "NSPIN",
            "ECRS",
            "Minimum SOC",
            "Maximum SOC",
            "Hour Beginning Planned SOC",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

        assert df["Resource Name"].dtype == object
        assert df["QSE"].dtype == object

        # Column not in newer data so it's added as null
        assert df["RRS"].isnull().all()

        # Columns not in older data but should be present in newer data
        for col in [
            "High Sustained Limit",
            "Low Sustained Limit",
            "High Emergency Limit",
            "Low Emergency Limit",
            "Reg Up",
            "Reg Down",
            "RRSPFR",
            "RRSFFR",
            "RRSUFR",
            "NSPIN",
            "ECRS",
        ]:
            assert df[col].notnull().all()

    def test_get_cop_adjustment_period_snapshot_60_day_raises_error(self):
        with pytest.raises(ValueError):
            self.iso.get_cop_adjustment_period_snapshot_60_day(
                start=self.local_today() - pd.DateOffset(days=59),
                end=self.local_today(),
            )

    def test_get_cop_adjustment_period_snapshot_60_day_historical_date_range(self):
        # Must be at least 60 days in the past
        start = self.local_today() - pd.DateOffset(days=63)
        end = start + pd.DateOffset(days=2)

        with api_vcr.use_cassette(
            f"test_get_cop_adjustment_period_snapshot_60_day_historical_date_range_{start}_{end}.yaml",
        ):
            df = self.iso.get_cop_adjustment_period_snapshot_60_day(start, end)

        self._check_cop_adjustment_period_snapshot_60_day(df)

        assert df["Interval Start"].min() == self.local_start_of_day(start)
        assert df["Interval Start"].max() == self.local_start_of_day(
            end,
        ) - pd.DateOffset(hours=1)

    """get_crr_*_monthly"""

    CRR_TEST_MONTH_START = "2026-04-01"
    CRR_TEST_MONTH_END = "2026-05-01"

    crr_auction_bids_offers_cols = [
        "Interval Start",
        "Interval End",
        "Path",
        "Source",
        "Sink",
        "Bid Type",
        "Hedge Type",
        "Time of Use",
        "MW",
        "Bid Price Per MWh",
        "Shadow Price Per MWh",
    ]

    crr_base_loading_cols = [
        "Interval Start",
        "Interval End",
        "CRR ID",
        "Account Holder",
        "Source",
        "Sink",
        "Hedge Type",
        "Time of Use",
        "MW",
        "Shadow Price Per MWh",
        "Path",
    ]

    crr_binding_constraints_cols = [
        "Interval Start",
        "Interval End",
        "Device Name",
        "Device Type",
        "Direction",
        "Flow",
        "Limit",
        "Description",
        "Contingency",
        "Time of Use",
        "Shadow Price",
    ]

    crr_market_results_cols = [
        "Interval Start",
        "Interval End",
        "CRR ID",
        "Original CRR ID",
        "Account Holder",
        "Hedge Type",
        "Bid Type",
        "CRR Type",
        "Source",
        "Sink",
        "Time of Use",
        "Bid 24 Hour",
        "MW",
        "Shadow Price Per MWh",
        "Path",
    ]

    crr_source_sink_shadow_prices_cols = [
        "Interval Start",
        "Interval End",
        "Source Sink",
        "Time of Use",
        "Shadow Price Per MWh",
    ]

    def _check_crr_monthly_frame(
        self,
        df: pd.DataFrame,
        expected_cols: list[str],
        expected_end: datetime.date | None = None,
    ) -> None:
        assert df.columns.tolist() == expected_cols
        assert len(df) > 0
        assert pd.api.types.is_datetime64_any_dtype(df["Interval Start"])
        assert pd.api.types.is_datetime64_any_dtype(df["Interval End"])
        assert df["Interval Start"].dt.tz is not None
        assert df["Interval End"].dt.tz is not None
        tz = self.iso.default_timezone
        expected_start = pd.Timestamp(self.CRR_TEST_MONTH_START, tz=tz)
        if expected_end is None:
            expected_end_ts = expected_start + pd.offsets.MonthEnd(0)
        else:
            expected_end_ts = pd.Timestamp(expected_end, tz=tz)
        assert (df["Interval Start"] == expected_start).all()
        assert (df["Interval End"] == expected_end_ts).all()

    def test_get_crr_auction_bids_offers_monthly_historical(self):
        with api_vcr.use_cassette(
            "test_get_crr_auction_bids_offers_monthly_historical.yaml",
        ):
            df = self.iso.get_crr_auction_bids_offers_monthly(
                date=self.CRR_TEST_MONTH_START,
                end=self.CRR_TEST_MONTH_END,
            )

        self._check_crr_monthly_frame(
            df,
            self.crr_auction_bids_offers_cols,
            expected_end=datetime.date(2026, 4, 30),
        )
        assert (df["Path"] == df["Source"] + "-" + df["Sink"]).all()
        assert df["Bid Type"].isin(["BUY", "SELL"]).all()

    def test_get_crr_base_loading_monthly_historical(self):
        with api_vcr.use_cassette(
            "test_get_crr_base_loading_monthly_historical.yaml",
        ):
            df = self.iso.get_crr_base_loading_monthly(
                date=self.CRR_TEST_MONTH_START,
                end=self.CRR_TEST_MONTH_END,
            )

        self._check_crr_monthly_frame(df, self.crr_base_loading_cols)
        assert (df["Path"] == df["Source"] + "-" + df["Sink"]).all()

    def test_get_crr_binding_constraints_monthly_historical(self):
        with api_vcr.use_cassette(
            "test_get_crr_binding_constraints_monthly_historical.yaml",
        ):
            df = self.iso.get_crr_binding_constraints_monthly(
                date=self.CRR_TEST_MONTH_START,
                end=self.CRR_TEST_MONTH_END,
            )

        self._check_crr_monthly_frame(df, self.crr_binding_constraints_cols)
        assert df["Direction"].notna().all()
        assert df["Device Type"].notna().all()

    def test_get_crr_market_results_monthly_historical(self):
        with api_vcr.use_cassette(
            "test_get_crr_market_results_monthly_historical.yaml",
        ):
            df = self.iso.get_crr_market_results_monthly(
                date=self.CRR_TEST_MONTH_START,
                end=self.CRR_TEST_MONTH_END,
            )

        self._check_crr_monthly_frame(
            df,
            self.crr_market_results_cols,
            expected_end=datetime.date(2026, 4, 30),
        )
        assert (df["Path"] == df["Source"] + "-" + df["Sink"]).all()

    def test_get_crr_source_sink_shadow_prices_monthly_historical(self):
        with api_vcr.use_cassette(
            "test_get_crr_source_sink_shadow_prices_monthly_historical.yaml",
        ):
            df = self.iso.get_crr_source_sink_shadow_prices_monthly(
                date=self.CRR_TEST_MONTH_START,
                end=self.CRR_TEST_MONTH_END,
            )

        self._check_crr_monthly_frame(df, self.crr_source_sink_shadow_prices_cols)
        pk = ["Source Sink", "Interval Start", "Time of Use"]
        assert not df.duplicated(subset=pk).any()

    def test_get_crr_market_results_monthly_multi_month_range(self):
        with api_vcr.use_cassette(
            "test_get_crr_market_results_monthly_multi_month_range.yaml",
        ):
            df = self.iso.get_crr_market_results_monthly(
                date="2026-02-01",
                end="2026-04-01",
            )

        assert df.columns.tolist() == self.crr_market_results_cols
        tz = self.iso.default_timezone
        months = sorted(df["Interval Start"].unique())
        assert months == [
            pd.Timestamp("2026-02-01", tz=tz),
            pd.Timestamp("2026-03-01", tz=tz),
        ]
        end_dates = sorted(df["Interval End"].unique())
        assert end_dates == [
            pd.Timestamp("2026-02-28", tz=tz),
            pd.Timestamp("2026-03-31", tz=tz),
        ]

    """get_crr_*_annual"""

    CRR_TEST_AUCTION_YEAR_START = "2026-01-01"
    CRR_TEST_AUCTION_YEAR_END = "2027-01-01"

    crr_auction_bids_offers_annual_cols = [
        "Interval Start",
        "Interval End",
        "Path",
        "Source",
        "Sink",
        "Bid Type",
        "Hedge Type",
        "Time of Use",
        "MW",
        "Bid Price Per MWh",
        "Shadow Price Per MWh",
        "Sequence",
        "Strip",
    ]

    crr_base_loading_annual_cols = [
        "Interval Start",
        "Interval End",
        "CRR ID",
        "Account Holder",
        "Source",
        "Sink",
        "Hedge Type",
        "Time of Use",
        "MW",
        "Shadow Price Per MWh",
        "Path",
        "Sequence",
        "Strip",
    ]

    crr_binding_constraints_annual_cols = [
        "Interval Start",
        "Interval End",
        "Device Name",
        "Device Type",
        "Direction",
        "Flow",
        "Limit",
        "Description",
        "Contingency",
        "Time of Use",
        "Shadow Price",
        "Sequence",
        "Strip",
    ]

    crr_market_results_annual_cols = [
        "Interval Start",
        "Interval End",
        "CRR ID",
        "Original CRR ID",
        "Account Holder",
        "Hedge Type",
        "Bid Type",
        "CRR Type",
        "Source",
        "Sink",
        "Time of Use",
        "Bid 24 Hour",
        "MW",
        "Shadow Price Per MWh",
        "Path",
        "Sequence",
        "Strip",
    ]

    crr_source_sink_shadow_prices_annual_cols = [
        "Interval Start",
        "Interval End",
        "Source Sink",
        "Time of Use",
        "Shadow Price Per MWh",
        "Sequence",
        "Strip",
    ]

    def _check_crr_annual_frame(
        self,
        df: pd.DataFrame,
        expected_cols: list[str],
        expected_year: int = 2026,
    ) -> None:
        assert df.columns.tolist() == expected_cols
        assert len(df) > 0
        assert pd.api.types.is_datetime64_any_dtype(df["Interval Start"])
        assert pd.api.types.is_datetime64_any_dtype(df["Interval End"])
        assert df["Interval Start"].dt.tz is not None
        assert df["Interval End"].dt.tz is not None
        tz = self.iso.default_timezone
        assert (
            df["Interval Start"] >= pd.Timestamp(f"{expected_year}-01-01", tz=tz)
        ).all()
        assert (
            df["Interval Start"] < pd.Timestamp(f"{expected_year + 1}-01-01", tz=tz)
        ).all()
        assert df["Sequence"].between(1, 6).all()
        assert df["Strip"].isin([1, 2]).all()

    def test_get_crr_auction_bids_offers_annual_historical(self):
        with api_vcr.use_cassette(
            "test_get_crr_auction_bids_offers_annual_historical.yaml",
        ):
            df = self.iso.get_crr_auction_bids_offers_annual(
                date=self.CRR_TEST_AUCTION_YEAR_START,
                end=self.CRR_TEST_AUCTION_YEAR_END,
            )

        self._check_crr_annual_frame(df, self.crr_auction_bids_offers_annual_cols)
        assert (df["Path"] == df["Source"] + "-" + df["Sink"]).all()
        assert df["Bid Type"].isin(["BUY", "SELL"]).all()
        observed_combos = sorted(
            map(
                tuple,
                df[["Sequence", "Strip"]].drop_duplicates().to_numpy().tolist(),
            ),
        )
        expected_combos = sorted(
            (seq, strip) for strip in (1, 2) for seq in range(1, 7)
        )
        assert observed_combos == expected_combos

    def test_get_crr_base_loading_annual_historical(self):
        with api_vcr.use_cassette(
            "test_get_crr_base_loading_annual_historical.yaml",
        ):
            df = self.iso.get_crr_base_loading_annual(
                date=self.CRR_TEST_AUCTION_YEAR_START,
                end=self.CRR_TEST_AUCTION_YEAR_END,
            )

        self._check_crr_annual_frame(df, self.crr_base_loading_annual_cols)
        assert (df["Path"] == df["Source"] + "-" + df["Sink"]).all()
        pk = ["Interval Start", "Sequence", "Strip", "CRR ID"]
        assert not df.duplicated(subset=pk).any()

    def test_get_crr_binding_constraints_annual_historical(self):
        with api_vcr.use_cassette(
            "test_get_crr_binding_constraints_annual_historical.yaml",
        ):
            df = self.iso.get_crr_binding_constraints_annual(
                date=self.CRR_TEST_AUCTION_YEAR_START,
                end=self.CRR_TEST_AUCTION_YEAR_END,
            )

        self._check_crr_annual_frame(df, self.crr_binding_constraints_annual_cols)
        assert df["Direction"].notna().all()
        assert df["Device Type"].notna().all()
        pk = [
            "Interval Start",
            "Sequence",
            "Strip",
            "Device Name",
            "Contingency",
            "Direction",
            "Time of Use",
        ]
        assert not df.duplicated(subset=pk).any()

    def test_get_crr_market_results_annual_historical(self):
        with api_vcr.use_cassette(
            "test_get_crr_market_results_annual_historical.yaml",
        ):
            df = self.iso.get_crr_market_results_annual(
                date=self.CRR_TEST_AUCTION_YEAR_START,
                end=self.CRR_TEST_AUCTION_YEAR_END,
            )

        self._check_crr_annual_frame(df, self.crr_market_results_annual_cols)
        assert (df["Path"] == df["Source"] + "-" + df["Sink"]).all()
        pk = ["Interval Start", "Sequence", "Strip", "CRR ID"]
        assert not df.duplicated(subset=pk).any()

    def test_get_crr_source_sink_shadow_prices_annual_historical(self):
        with api_vcr.use_cassette(
            "test_get_crr_source_sink_shadow_prices_annual_historical.yaml",
        ):
            df = self.iso.get_crr_source_sink_shadow_prices_annual(
                date=self.CRR_TEST_AUCTION_YEAR_START,
                end=self.CRR_TEST_AUCTION_YEAR_END,
            )

        self._check_crr_annual_frame(
            df,
            self.crr_source_sink_shadow_prices_annual_cols,
        )
        pk = ["Interval Start", "Sequence", "Strip", "Source Sink", "Time of Use"]
        assert not df.duplicated(subset=pk).any()

    def test_get_crr_market_results_annual_multi_year_range(self):
        with api_vcr.use_cassette(
            "test_get_crr_market_results_annual_multi_year_range.yaml",
        ):
            df = self.iso.get_crr_market_results_annual(
                date="2026-01-01",
                end="2028-01-01",
            )

        assert df.columns.tolist() == self.crr_market_results_annual_cols
        tz = self.iso.default_timezone
        years_covered = sorted(df["Interval Start"].dt.year.unique())
        assert set(years_covered).issubset({2026, 2027})
        assert (df["Interval Start"] >= pd.Timestamp("2026-01-01", tz=tz)).all()
        assert (df["Interval Start"] < pd.Timestamp("2028-01-01", tz=tz)).all()
        assert df["Sequence"].between(1, 6).all()
        assert df["Strip"].isin([1, 2]).all()

    """get_hourly_load_post_settlements"""

    def _check_hourly_load_post_settlements(self, df):
        """Common checks for hourly load post settlements data."""
        expected_columns = [
            "Interval Start",
            "Interval End",
            "Coast",
            "East",
            "Far West",
            "North",
            "North Central",
            "South",
            "South Central",
            "West",
            "ERCOT",
        ]

        assert df.columns.tolist() == expected_columns
        assert df.shape[0] > 0
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

        # Check timezone
        assert df["Interval Start"].dt.tz.zone == self.iso.default_timezone
        assert df["Interval End"].dt.tz.zone == self.iso.default_timezone

        # Check numeric columns are numeric
        numeric_columns = [
            col
            for col in expected_columns
            if col not in ["Interval Start", "Interval End"]
        ]
        for col in numeric_columns:
            if col in df.columns:
                assert pd.api.types.is_numeric_dtype(df[col])

    def test_get_hourly_load_post_settlements_latest(self):
        """Test getting the latest year's data."""
        with api_vcr.use_cassette(
            "test_get_hourly_load_post_settlements_latest.yaml",
        ):
            df = self.iso.get_hourly_load_post_settlements("latest")
        self._check_hourly_load_post_settlements(df)

        # Should be current year data
        current_year = pd.Timestamp.now().year
        assert df["Interval Start"].dt.year.unique() == [current_year]

    @pytest.mark.parametrize("date, end", [("2010-03-01", "2010-08-02")])
    def test_get_hourly_load_post_settlements_xls(self, date, end):
        """Test getting historical data from the 2004-2016 era."""
        with api_vcr.use_cassette(
            "test_get_hourly_load_post_settlements_historical_2004_2016.yaml",
        ):
            df = self.iso.get_hourly_load_post_settlements(date, end)
        self._check_hourly_load_post_settlements(df)

        assert df["Interval Start"].min() == pd.Timestamp(date, tz="US/Central")
        assert df["Interval End"].max() == pd.Timestamp(end, tz="US/Central")

    @pytest.mark.parametrize("date, end", [("2023-07-01", "2023-08-02")])
    def test_get_hourly_load_post_settlements_zip(self, date, end):
        """Test getting modern data from the 2017-2025 era."""
        with api_vcr.use_cassette(
            "test_get_hourly_load_post_settlements_modern_2017_2025.yaml",
        ):
            df = self.iso.get_hourly_load_post_settlements(date, end)
        self._check_hourly_load_post_settlements(df)

        assert df["Interval Start"].min() == pd.Timestamp(date, tz="US/Central")
        assert df["Interval End"].max() == pd.Timestamp(end, tz="US/Central")

    """get_mcpc_dam"""

    def _check_get_mcpc_dam(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "AS Type",
            "MCPC",
        ]
        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"
        assert df.dtypes["MCPC"] == "float64"
        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()
        # 5 AS types * 24 hours = 120 rows per day
        assert set(df["AS Type"].unique()) == {"ECRS", "NSPIN", "REGDN", "REGUP", "RRS"}

    def test_get_mcpc_dam_today(self):
        with api_vcr.use_cassette("test_get_mcpc_dam_today.yaml"):
            df = self.iso.get_mcpc_dam("today", verbose=True)
        self._check_get_mcpc_dam(df)

    def test_get_mcpc_dam_latest(self):
        with api_vcr.use_cassette("test_get_mcpc_dam_latest.yaml"):
            df = self.iso.get_mcpc_dam("latest")
        self._check_get_mcpc_dam(df)

    def test_get_mcpc_dam_historical(self):
        date = pd.Timestamp("2026-03-05", tz=self.iso.default_timezone)
        with api_vcr.use_cassette("test_get_mcpc_dam_historical.yaml"):
            df = self.iso.get_mcpc_dam(date, verbose=True)
        self._check_get_mcpc_dam(df)
        assert df["Interval Start"].min() == date
        assert df["Interval End"].max() == date + pd.DateOffset(days=1)

    def test_get_mcpc_dam_date_range(self):
        start = pd.Timestamp("2026-03-04", tz=self.iso.default_timezone)
        end = pd.Timestamp("2026-03-06", tz=self.iso.default_timezone)
        with api_vcr.use_cassette("test_get_mcpc_dam_date_range.yaml"):
            df = self.iso.get_mcpc_dam(start, end=end, verbose=True)
        self._check_get_mcpc_dam(df)
        assert df["Interval Start"].min() == start
        assert df["Interval End"].max() == end

    """get_shadow_prices_dam"""

    expected_shadow_prices_dam_columns = [
        "Interval Start",
        "Interval End",
        "Constraint ID",
        "Constraint Name",
        "Contingency Name",
        "Limiting Facility",
        "Constraint Limit",
        "Constraint Value",
        "Violation Amount",
        "Shadow Price",
        "From Station",
        "To Station",
        "From Station kV",
        "To Station kV",
    ]

    def _check_shadow_prices_dam(self, df):
        assert df.columns.tolist() == self.expected_shadow_prices_dam_columns
        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"
        assert (
            df.loc[
                df["Contingency Name"] == "BASE CASE",
                "Limiting Facility",
            ]
            .isna()
            .all()
        )
        for col in [
            "Constraint Name",
            "Contingency Name",
            "From Station",
            "To Station",
        ]:
            assert df[col].dropna().str.strip().equals(df[col].dropna())

    def test_get_shadow_prices_dam_today(self):
        with api_vcr.use_cassette("test_get_shadow_prices_dam_today.yaml"):
            df = self.iso.get_shadow_prices_dam("today", verbose=True)
        self._check_shadow_prices_dam(df)
        assert df.shape[0] > 0

    def test_get_shadow_prices_dam_latest(self):
        with api_vcr.use_cassette("test_get_shadow_prices_dam_latest.yaml"):
            df = self.iso.get_shadow_prices_dam("latest")
        self._check_shadow_prices_dam(df)
        assert df.shape[0] > 0

    def test_get_shadow_prices_dam_historical(self):
        date = pd.Timestamp("2026-03-05", tz=self.iso.default_timezone)
        with api_vcr.use_cassette("test_get_shadow_prices_dam_historical.yaml"):
            df = self.iso.get_shadow_prices_dam(date, verbose=True)
        self._check_shadow_prices_dam(df)
        assert df["Interval Start"].min() == date
        assert df["Interval End"].max() == date + pd.DateOffset(days=1)

    def test_get_shadow_prices_dam_date_range(self):
        start = pd.Timestamp("2026-03-04", tz=self.iso.default_timezone)
        end = pd.Timestamp("2026-03-06", tz=self.iso.default_timezone)
        with api_vcr.use_cassette("test_get_shadow_prices_dam_date_range.yaml"):
            df = self.iso.get_shadow_prices_dam(start, end=end, verbose=True)
        self._check_shadow_prices_dam(df)
        assert df["Interval Start"].min() == start
        assert df["Interval End"].max() == end

    """get_mcpc_sced"""

    def _check_get_mcpc_sced(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "SCED Timestamp",
            "AS Type",
            "MCPC",
        ]
        assert df.dtypes["SCED Timestamp"] == "datetime64[ns, US/Central]"
        assert df.dtypes["AS Type"] == "object"
        assert df.dtypes["MCPC"] == "float64"

    def test_get_mcpc_sced_latest(self):
        with api_vcr.use_cassette(
            "test_get_mcpc_sced_date_range_latest.yaml",
        ):
            df = self.iso.get_mcpc_sced("latest")

        self._check_get_mcpc_sced(df)

        assert df["SCED Timestamp"].nunique() == 1
        assert df["SCED Timestamp"].min() <= self.local_now()
        assert df["SCED Timestamp"].min() >= self.local_now() - pd.Timedelta(minutes=10)

    def test_get_mcpc_sced_date_range(self):
        # Choose a date range that spans two days to test we handle day transitions
        date = self.local_start_of_today() - pd.Timedelta(hours=1)
        end = date + pd.Timedelta(hours=2)

        assert date.date() != end.date()

        with api_vcr.use_cassette(
            f"test_get_mcpc_sced_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_mcpc_sced(date, end)

        self._check_get_mcpc_sced(df)

        assert df["SCED Timestamp"].min().date() == date.date()
        assert df["SCED Timestamp"].max().date() == end.date()

        # 2 hours / 15 minutes/interval = 24 intervals
        assert df["SCED Timestamp"].nunique() == 24

    """get_mcpc_real_time_15_min"""

    def _check_get_mcpc_real_time_15_min(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "AS Type",
            "MCPC",
        ]
        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"
        assert df.dtypes["AS Type"] == "object"
        assert df.dtypes["MCPC"] == "float64"

    def test_get_mcpc_real_time_15_min_latest(self):
        with api_vcr.use_cassette(
            "test_get_mcpc_real_time_15_min_date_range_latest.yaml",
        ):
            df = self.iso.get_mcpc_real_time_15_min("latest")

        self._check_get_mcpc_real_time_15_min(df)

        assert df["Interval Start"].nunique() == 1
        assert df["Interval Start"].min() <= self.local_now()
        assert df["Interval Start"].min() >= self.local_now() - pd.Timedelta(
            minutes=60,
        )

    def test_get_mcpc_real_time_15_min_date_range(self):
        # Choose a date range that spans two days to test we handle day transitions
        date = self.local_start_of_today() - pd.Timedelta(hours=1)
        end = date + pd.Timedelta(hours=2)

        assert date.date() != end.date()

        with api_vcr.use_cassette(
            f"test_get_mcpc_real_time_15_min_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_mcpc_real_time_15_min(date, end)

        self._check_get_mcpc_real_time_15_min(df)

        assert df["Interval Start"].min().date() == date.date()
        assert df["Interval Start"].max().date() == end.date()

        # 2 hours / 15 minutes/interval = 8 intervals
        assert df["Interval Start"].nunique() == 8

    """get_as_demand_curves_dam_and_sced"""

    def _check_get_as_demand_curves_dam_and_sced(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "AS Type",
            "Demand Curve Point",
            "Quantity",
            "Price",
        ]
        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Publish Time"] == "datetime64[s, US/Central]"
        assert df.dtypes["AS Type"] == "object"
        assert df.dtypes["Demand Curve Point"] == "int64"
        assert df.dtypes["Quantity"] == "int64"
        assert df.dtypes["Price"] == "float64"

    def test_get_as_demand_curves_dam_and_sced_latest(self):
        with api_vcr.use_cassette(
            "test_get_as_demand_curves_dam_and_sced_date_range_latest.yaml",
        ):
            df = self.iso.get_as_demand_curves_dam_and_sced("latest")

        self._check_get_as_demand_curves_dam_and_sced(df)

        # The "latest" method will still get us two days of data
        assert df["Interval Start"].min() == self.local_now().normalize()
        assert df[
            "Interval Start"
        ].max() == self.local_now().normalize() + pd.DateOffset(days=1, hours=23)

    def test_get_as_demand_curves_dam_and_sced_date_range(self):
        date = pd.Timestamp.now().normalize() - pd.Timedelta(days=2)
        end = date + pd.Timedelta(days=1)

        with api_vcr.use_cassette(
            f"test_get_as_demand_curves_dam_and_sced_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_as_demand_curves_dam_and_sced(date, end)

        self._check_get_as_demand_curves_dam_and_sced(df)

        assert df["Interval Start"].min() == date.tz_localize(
            self.iso.default_timezone,
        )
        # Data extends into the future one day
        assert df["Interval Start"].max() == (
            end + pd.DateOffset(days=1) - pd.Timedelta(hours=1)
        ).tz_localize(self.iso.default_timezone)

    """get_dam_asdc_aggregated"""

    # Per NP4-19-CD documentation, the dataset advertises REGDN, REGUP, RRSPF,
    # RRSFF, RRSUF, ECRSS, and ECRSM; in practice the published files also
    # include the pre-ECRS NSPIN and NSPNM products.
    allowed_dam_asdc_aggregated_as_types = {
        "REGDN",
        "REGUP",
        "RRSPF",
        "RRSFF",
        "RRSUF",
        "ECRSS",
        "ECRSM",
        "NSPIN",
        "NSPNM",
    }

    def _check_get_dam_asdc_aggregated(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "AS Type",
            "Price",
            "Quantity",
        ]
        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"
        assert df.dtypes["AS Type"] == "object"
        assert df.dtypes["Price"] == "float64"
        assert df.dtypes["Quantity"] == "float64"
        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()
        assert set(df["AS Type"].unique()).issubset(
            self.allowed_dam_asdc_aggregated_as_types,
        )

    def test_get_dam_asdc_aggregated_latest(self):
        with api_vcr.use_cassette("test_get_dam_asdc_aggregated_latest.yaml"):
            df = self.iso.get_dam_asdc_aggregated("latest")
        self._check_get_dam_asdc_aggregated(df)

    def test_get_dam_asdc_aggregated_date_range(self):
        date = pd.Timestamp.now().normalize() - pd.Timedelta(days=2)
        end = date + pd.Timedelta(days=1)

        with api_vcr.use_cassette(
            f"test_get_dam_asdc_aggregated_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_dam_asdc_aggregated(date, end)

        self._check_get_dam_asdc_aggregated(df)

        assert df["Interval Start"].min() == date.tz_localize(
            self.iso.default_timezone,
        )
        assert df["Interval End"].max() == end.tz_localize(
            self.iso.default_timezone,
        )

    """get_as_deployment_factors_projected"""

    def _check_as_deployment_factors_projected(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "AS Type",
            "AS Deployment Factors",
        ]
        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"
        assert df.dtypes["AS Type"] == "object"
        assert df.dtypes["AS Deployment Factors"] == "float64"

    def test_get_as_deployment_factors_projected_latest(self):
        with api_vcr.use_cassette(
            "test_get_as_deployment_factors_projected_latest.yaml",
        ):
            df = self.iso.get_as_deployment_factors_projected("latest")

        self._check_as_deployment_factors_projected(df)

        # "latest" gets one day of data for tomorrow
        assert df["Interval Start"].min() >= self.local_now().normalize()
        assert df[
            "Interval Start"
        ].max() >= self.local_now().normalize() + pd.Timedelta(hours=23)
        assert df["Interval Start"].nunique() == 24

    def test_get_as_deployment_factors_projected_date_range(self):
        date = self.local_start_of_today() - pd.Timedelta(days=2)
        end = date + pd.Timedelta(days=1)

        with api_vcr.use_cassette(
            f"test_get_as_deployment_factors_projected_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_as_deployment_factors_projected(date, end)

        self._check_as_deployment_factors_projected(df)

        # Data is projected for the next day
        assert df["Interval Start"].min() == date + pd.DateOffset(days=1)
        assert df["Interval Start"].max() == end + pd.DateOffset(days=1) - pd.Timedelta(
            hours=1,
        )

    """get_as_deployment_factors_weekly_ruc"""

    # Check for weekly, daily, and hourly RUC
    def _check_as_deployment_factors_ruc(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "RUC Timestamp",
            "AS Type",
            "AS Deployment Factors",
        ]

        for col in ["Interval Start", "Interval End", "RUC Timestamp"]:
            assert df.dtypes[col] == "datetime64[ns, US/Central]"

        assert df.dtypes["AS Type"] == "object"
        assert df.dtypes["AS Deployment Factors"] == "float64"

        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()

    def test_get_as_deployment_factors_weekly_ruc_latest(self):
        with api_vcr.use_cassette(
            "test_get_as_deployment_factors_weekly_ruc_latest.yaml",
        ):
            df = self.iso.get_as_deployment_factors_weekly_ruc("latest")

        self._check_as_deployment_factors_ruc(df)

        assert df["RUC Timestamp"].nunique() == 1
        assert df["Interval Start"].nunique() == 120

    def test_get_as_deployment_factors_weekly_ruc_date_range(self):
        date = self.local_start_of_day(self.local_today() - pd.Timedelta(days=2))
        end = date + pd.DateOffset(days=2)

        with api_vcr.use_cassette(
            "test_get_as_deployment_factors_weekly_ruc_date_range.yaml",
        ):
            df = self.iso.get_as_deployment_factors_weekly_ruc(date, end)

        self._check_as_deployment_factors_ruc(df)

        assert df["RUC Timestamp"].nunique() == 2
        assert df["Interval Start"].min() == date + pd.DateOffset(days=1)
        assert df["Interval Start"].max() == end + pd.DateOffset(days=5) - pd.Timedelta(
            hours=1,
        )

        # Total of 6 days
        assert df["Interval Start"].nunique() == 144

    """get_as_deployment_factors_daily_ruc"""

    def test_get_as_deployment_factors_daily_ruc_latest(self):
        with api_vcr.use_cassette(
            "test_get_as_deployment_factors_daily_ruc_latest.yaml",
        ):
            df = self.iso.get_as_deployment_factors_daily_ruc("latest")

        self._check_as_deployment_factors_ruc(df)

        assert df["RUC Timestamp"].nunique() == 1
        assert df["Interval Start"].nunique() == 24

    def test_get_as_deployment_factors_daily_ruc_date_range(self):
        # Data is published per DRUC run (once per day) for the next day
        date = self.local_start_of_day(self.local_today() - pd.Timedelta(days=2))
        end = date + pd.DateOffset(days=2)

        with api_vcr.use_cassette(
            "test_get_as_deployment_factors_daily_ruc_date_range.yaml",
        ):
            df = self.iso.get_as_deployment_factors_daily_ruc(date, end)

        self._check_as_deployment_factors_ruc(df)

        assert df["RUC Timestamp"].nunique() == 2
        assert df["Interval Start"].min() == date + pd.DateOffset(days=1)
        assert df["Interval Start"].max() == end + pd.DateOffset(days=1) - pd.Timedelta(
            hours=1,
        )

        # Total of 2 days
        assert df["Interval Start"].nunique() == 48

    """get_as_deployment_factors_hourly_ruc"""

    def test_get_as_deployment_factors_hourly_ruc_latest(self):
        with api_vcr.use_cassette(
            "test_get_as_deployment_factors_hourly_ruc_latest.yaml",
        ):
            df = self.iso.get_as_deployment_factors_hourly_ruc("latest")

        self._check_as_deployment_factors_ruc(df)

        assert df["RUC Timestamp"].nunique() == 1
        # The number of intervals in the latest file differs depending on time of day
        assert df["Interval Start"].nunique() > 1

    def test_get_as_deployment_factors_hourly_ruc_date_range(self):
        # Data is published per HRUC run (once per hour) for the rest of the current day
        date = self.local_start_of_today() - pd.Timedelta(hours=2)
        end = date + pd.Timedelta(hours=3)

        with api_vcr.use_cassette(
            "test_get_as_deployment_factors_hourly_ruc_date_range.yaml",
        ):
            df = self.iso.get_as_deployment_factors_hourly_ruc(date, end)

        self._check_as_deployment_factors_ruc(df)

        assert df["RUC Timestamp"].nunique() == 3
        assert df["Interval Start"].min() == date + pd.Timedelta(hours=1)
        assert df["Interval Start"].max() == self.local_start_of_today() + pd.Timedelta(
            hours=23,
        )

    """get_dam_total_as_sold"""

    def _check_get_dam_total_as_sold(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "AS Type",
            "Quantity",
        ]
        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"
        assert df.dtypes["AS Type"] == "object"
        assert df.dtypes["Quantity"] == "float64"

    def test_get_dam_total_as_sold_latest(self):
        with api_vcr.use_cassette(
            "test_get_dam_total_as_sold_latest.yaml",
        ):
            df = self.iso.get_dam_total_as_sold("latest")

        self._check_get_dam_total_as_sold(df)

        assert df["Interval Start"].nunique() == 25

    def test_get_dam_total_as_sold_date_range(self):
        # Data is only available per DAM run so we use a set time we know it exists
        date = pd.Timestamp("2025-11-02", tz=self.iso.default_timezone)
        end = date + pd.DateOffset(days=1)

        with api_vcr.use_cassette(
            f"test_get_dam_total_as_sold_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_dam_total_as_sold(date, end)

        self._check_get_dam_total_as_sold(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == (end - pd.Timedelta(hours=1))

    """get_as_demand_curves_hourly_ruc"""

    def _check_hourly_ruc_as_demand_curves(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "RUC Timestamp",
            "AS Type",
            "Demand Curve Point",
            "Quantity",
            "Price",
        ]

        for col in ["Interval Start", "Interval End", "RUC Timestamp"]:
            assert df.dtypes[col] == "datetime64[ns, US/Central]"

        assert df.dtypes["AS Type"] == "object"
        assert df.dtypes["Demand Curve Point"] == "int64"
        assert df.dtypes["Quantity"] == "int64"
        assert df.dtypes["Price"] == "float64"

    def test_get_as_demand_curves_hourly_ruc_latest(self):
        with api_vcr.use_cassette(
            "test_get_as_demand_curves_hourly_ruc_latest.yaml",
        ):
            df = self.iso.get_as_demand_curves_hourly_ruc("latest")

        self._check_hourly_ruc_as_demand_curves(df)

        assert df["RUC Timestamp"].nunique() == 1

    def test_get_as_demand_curves_hourly_ruc_date_range(self):
        date = self.local_start_of_today() - pd.Timedelta(hours=1)
        end = date + pd.Timedelta(hours=3)

        with api_vcr.use_cassette(
            f"test_get_as_demand_curves_hourly_ruc_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_as_demand_curves_hourly_ruc(date, end)

        self._check_hourly_ruc_as_demand_curves(df)

        assert df["RUC Timestamp"].nunique() == 3
        assert df["Interval Start"].min() == date + pd.Timedelta(hours=1)
        assert df["Interval Start"].max() == self.local_start_of_today() + pd.Timedelta(
            hours=23,
        )

    """get_as_demand_curves_daily_ruc"""

    def _check_daily_ruc_as_demand_curves(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "RUC Timestamp",
            "AS Type",
            "Demand Curve Point",
            "Quantity",
            "Price",
        ]

        for col in ["Interval Start", "Interval End", "RUC Timestamp"]:
            assert df.dtypes[col] == "datetime64[ns, US/Central]"

        assert df.dtypes["AS Type"] == "object"
        assert df.dtypes["Demand Curve Point"] == "int64"
        assert df.dtypes["Quantity"] == "int64"
        assert df.dtypes["Price"] == "float64"

    def test_get_as_demand_curves_daily_ruc_latest(self):
        with api_vcr.use_cassette(
            "test_get_as_demand_curves_daily_ruc_latest.yaml",
        ):
            df = self.iso.get_as_demand_curves_daily_ruc("latest")

        self._check_daily_ruc_as_demand_curves(df)

        # One day of data is published at once
        assert df["Interval Start"].nunique() == 24
        assert df["RUC Timestamp"].nunique() == 1

    def test_get_as_demand_curves_daily_ruc_date_range(self):
        date = self.local_start_of_today() - pd.Timedelta(days=2)
        end = date + pd.Timedelta(days=2)

        with api_vcr.use_cassette(
            f"test_get_as_demand_curves_daily_ruc_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_as_demand_curves_daily_ruc(date, end)

        self._check_daily_ruc_as_demand_curves(df)

        assert df["RUC Timestamp"].nunique() == 2
        assert df["Interval Start"].min() == date + pd.Timedelta(days=1)
        assert df["Interval Start"].max() == end + pd.Timedelta(hours=23)

    """get_as_demand_curves_weekly_ruc"""

    def _check_weekly_ruc_as_demand_curves(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "RUC Timestamp",
            "AS Type",
            "Demand Curve Point",
            "Quantity",
            "Price",
        ]

        for col in ["Interval Start", "Interval End", "RUC Timestamp"]:
            assert df.dtypes[col] == "datetime64[ns, US/Central]"

        assert df.dtypes["AS Type"] == "object"
        assert df.dtypes["Demand Curve Point"] == "int64"
        assert df.dtypes["Quantity"] == "int64"
        assert df.dtypes["Price"] == "float64"

    def test_get_as_demand_curves_weekly_ruc_latest(self):
        with api_vcr.use_cassette(
            "test_get_as_demand_curves_weekly_ruc_latest.yaml",
        ):
            df = self.iso.get_as_demand_curves_weekly_ruc("latest")

        self._check_weekly_ruc_as_demand_curves(df)

        # Five days worth of data is published at once
        assert df["Interval Start"].nunique() == 120
        assert df["RUC Timestamp"].nunique() == 1

    def test_get_as_demand_curves_weekly_ruc_date_range(self):
        date = self.local_start_of_today() - pd.DateOffset(days=2)
        end = date + pd.DateOffset(days=2)

        with api_vcr.use_cassette(
            f"test_get_as_demand_curves_weekly_ruc_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_as_demand_curves_weekly_ruc(date, end)

        self._check_weekly_ruc_as_demand_curves(df)

        # 6 total days
        assert df["Interval Start"].nunique() == 144
        assert df["RUC Timestamp"].nunique() == 2

        assert df["Interval Start"].min() == date + pd.DateOffset(days=1)
        assert df["Interval Start"].max() == end + pd.DateOffset(days=4) + pd.Timedelta(
            hours=23,
        )

    """get_indicative_mcpc_rtd"""

    def _check_get_indicative_mcpc_rtd(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "RTD Timestamp",
            "REGUP",
            "REGDN",
            "RRS",
            "ECRS",
            "NSPIN",
        ]
        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"
        assert df.dtypes["RTD Timestamp"] == "datetime64[ns, US/Central]"

        for col in ["REGUP", "REGDN", "RRS", "ECRS", "NSPIN"]:
            assert df.dtypes[col] == "float64"

    def test_get_indicative_mcpc_rtd_latest(self):
        with api_vcr.use_cassette(
            "test_get_indicative_mcpc_rtd_latest.yaml",
        ):
            df = self.iso.get_indicative_mcpc_rtd("latest")

        self._check_get_indicative_mcpc_rtd(df)

        assert df["RTD Timestamp"].nunique() == 1

    def test_get_indicative_mcpc_rtd_date_range(self):
        # Use a date range that spans two days to test we handle day transitions
        date = self.local_start_of_today() - pd.Timedelta(hours=1)
        end = date + pd.Timedelta(hours=2)

        assert date.date() != end.date()

        with api_vcr.use_cassette(
            f"test_get_indicative_mcpc_rtd_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_indicative_mcpc_rtd(date, end)

        self._check_get_indicative_mcpc_rtd(df)

        assert df["Interval Start"].min().date() == date.date()
        assert df["Interval Start"].max().date() == end.date()

        # 2 hours / 5 minutes/interval = 24 RTD Timestamps
        assert df["RTD Timestamp"].nunique() == 24

    """get_as_total_capability"""

    def _check_get_as_total_capability(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "SCED Timestamp",
            "Publish Time",
            "Cap RegUp Total",
            "Cap RegDn Total",
            "Cap RRS Total",
            "Cap ECRS Total",
            "Cap NonSpin Total",
            "Cap RegUp RRS Total",
            "Cap RegUp RRS ECRS Total",
            "Cap RegUp RRS ECRS NonSpin Total",
        ]

        assert df.dtypes["SCED Timestamp"] == "datetime64[ns, US/Central]"

        for col in [
            "Cap RegUp Total",
            "Cap RegDn Total",
            "Cap RRS Total",
            "Cap ECRS Total",
            "Cap NonSpin Total",
            "Cap RegUp RRS Total",
            "Cap RegUp RRS ECRS Total",
            "Cap RegUp RRS ECRS NonSpin Total",
        ]:
            assert df.dtypes[col] == "float64"

    def test_get_as_total_capability_latest(self):
        with api_vcr.use_cassette(
            "test_get_as_total_capability_latest.yaml",
        ):
            df = self.iso.get_as_total_capability("latest")

        self._check_get_as_total_capability(df)

        # Each file has 5 SCED intervals
        assert df["SCED Timestamp"].nunique() == 5
        assert df["Publish Time"].nunique() == 1

    def test_get_as_total_capability_date_range(self):
        # Choose a date range that spans two days to test we handle day transitions
        date = self.local_start_of_today() - pd.Timedelta(hours=1)
        end = date + pd.Timedelta(hours=2)

        assert date.date() != end.date()

        with api_vcr.use_cassette(
            f"test_get_as_total_capability_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_as_total_capability(date, end)

        self._check_get_as_total_capability(df)

        assert df["SCED Timestamp"].min().date() == date.date()
        assert df["SCED Timestamp"].max().date() == end.date()

        # This dataset is odd in that each file has 5 SCED intervals (1 current
        # and 4 previous). This means the number of unique SCED intervals in 2 hours is
        # (2 hours / 5 minutes/interval) + 4 extra intervals = 28 intervals
        assert df["SCED Timestamp"].nunique() == 28
        assert df["Publish Time"].nunique() == 24

    """get_real_time_adders"""

    def _check_real_time_adders(self, df: pd.DataFrame):
        assert df.columns.tolist() == [
            "SCED Timestamp",
            "Interval Start",
            "Interval End",
            "System Lambda",
            "RTRDPA",
            "RTRDPARUS",
            "RTRDPARDS",
            "RTRDPARRS",
            "RTRDPAECRS",
            "RTRDPANSS",
            "RTRRUC",
            "RTRRMR",
            "RTDNCLR",
            "RTDERS",
            "RTDCTIEIMPORT",
            "RTDCTIEEXPORT",
            "RTBLTIMPORT",
            "RTBLTEXPORT",
            "RTOLLSL",
            "RTOLHSL",
        ]

        assert df.dtypes["SCED Timestamp"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()

        for col in [
            "System Lambda",
            "RTRDPA",
            "RTRDPARUS",
            "RTRDPARDS",
            "RTRDPARRS",
            "RTRDPAECRS",
            "RTRDPANSS",
            "RTRRUC",
            "RTRRMR",
            "RTDNCLR",
            "RTDERS",
            "RTDCTIEIMPORT",
            "RTDCTIEEXPORT",
            "RTBLTIMPORT",
            "RTBLTEXPORT",
            "RTOLLSL",
            "RTOLHSL",
        ]:
            assert df.dtypes[col] == "float64"

    def test_get_real_time_adders_latest(self):
        with api_vcr.use_cassette(
            "test_get_real_time_adders_latest.yaml",
        ):
            df = self.iso.get_real_time_adders("latest")

        self._check_real_time_adders(df)

        assert len(df) == 1

    def test_get_real_time_adders_date_range(self):
        # Choose a date range that spans two days to test we handle day transitions
        date = self.local_start_of_today() - pd.Timedelta(hours=1)
        end = date + pd.Timedelta(hours=2)

        assert date.date() != end.date()

        with api_vcr.use_cassette(
            f"test_get_real_time_adders_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_real_time_adders(date, end)

        self._check_real_time_adders(df)

        assert df["Interval Start"].min().date() == date.date()
        assert df["Interval Start"].max().date() == end.date()

        # 2 hours / 5 minutes/interval = 24 Timestamps
        assert df["Interval Start"].nunique() == 24

    """system_as_capacity_monitor"""

    def _check_system_as_capacity_monitor(self, df: pd.DataFrame) -> None:
        assert df.shape[0] == 1
        assert df.columns.tolist() == SYSTEM_AS_CAPACITY_MONITOR_COLUMNS

        assert df.dtypes["Time"] == "datetime64[ns, US/Central]"

        for col in SYSTEM_AS_CAPACITY_MONITOR_COLUMNS[1:]:
            assert df.dtypes[col] in ["float64", "int64"], (
                f"{col} has dtype {df.dtypes[col]}"
            )

    def test_get_system_as_capacity_monitor_latest(self):
        with api_vcr.use_cassette(
            "test_get_system_as_capacity_monitor_latest.yaml",
        ):
            df = self.iso.get_system_as_capacity_monitor("latest")

        self._check_system_as_capacity_monitor(df)

    def test_parse_system_as_capacity_monitor(self):
        fixture_json = {
            "lastUpdated": "2025-12-05T12:30:00Z",
            "data": {
                "rrsCapacity": [
                    ["header", "header"],
                    ["rrcCapPfrGenEsr", 1000.5],
                    ["rrcCapLrWoClr", 500.25],
                    ["rrcCapLr", 200.0],
                    ["rrcCapFfr", 300.0],
                    ["rrcCapFfrEsr", 150.0],
                ],
                "regCapability": [
                    ["header", "header"],
                    ["regUpCap", 800.0],
                    ["regDownCap", 750.0],
                    ["regUpUndeployed", 100.0],
                    ["regDownUndeployed", 90.0],
                    ["regUpDeployed", 50.0],
                    ["regDownDeployed", 45.0],
                ],
                "rrsAwards": [
                    ["header", "header"],
                    ["rrAwdGen", 400.0],
                    ["rrAwdNonClr", 200.0],
                    ["rrAwdClr", 100.0],
                    ["rrAwdFfr", 150.0],
                ],
                "regAwards": [
                    ["header", "header"],
                    ["regUpAwd", 350.0],
                    ["regDownAwd", 340.0],
                ],
                "ecrsCapability": [
                    ["header", "header"],
                    ["ecrsCapGen", 600.0],
                    ["ecrsCapNclr", 200.0],
                    ["ecrsCapClr", 150.0],
                    ["ecrsCapQs", 100.0],
                    ["ecrsCapEsr", 80.0],
                    ["ecrsCapDeployedGenLr", 50.0],
                ],
                "clrCapacity": [
                    ["header", "header"],
                    ["capClrDecreaseBp", 400.0],
                    ["capClrIncreaseBp", 450.0],
                ],
                "genCapacity": [
                    ["header", "header"],
                    ["capWEoIncreaseBp", 1200.0],
                    ["capWEoDecreaseBp", 1100.0],
                    ["capWoEoIncreaseBp", 300.0],
                    ["capWoEoDecreaseBp", 280.0],
                ],
                "esrCapacity": [
                    ["header", "header"],
                    ["esrCapWEoIncreaseBp", 200.0],
                    ["esrCapWEoDecreaseBp", 180.0],
                    ["esrCapWoEoIncreaseBp", 50.0],
                    ["esrCapWoEoDecreaseBp", 45.0],
                ],
                "genBpCapacity": [
                    ["header", "header"],
                    ["capIncreaseGenBp", 2000.0],
                    ["capDecreaseGenBp", 1800.0],
                ],
                "summaryCapacity": [
                    ["header", "header"],
                    ["sumCapResRegUpRrs", 1500.0],
                    ["sumCapResRegUpRrsEcrs", 1800.0],
                    ["sumCapResRegUpRrsEcrsNsr", 2200.0],
                ],
                "ecrsAwards": [
                    ["header", "header"],
                    ["ecrsAwdGen", 250.0],
                    ["ecrsAwdNonClr", 100.0],
                    ["ecrsAwdClr", 80.0],
                    ["ecrsAwdQs", 50.0],
                    ["ecrsAwdEsr", 40.0],
                ],
                "prcData": [
                    ["header", "header"],
                    ["prc", 5000.0],
                ],
                "nspinCapability": [
                    ["header", "header"],
                    ["nsrCapOnGenWoEo", 400.0],
                    ["nsrCapOffResWOs", 300.0],
                    ["nsrCapUndeployedLr", 200.0],
                    ["nsrCapOffGen", 350.0],
                    ["nsrCapEsr", 100.0],
                ],
                "ordcData": [
                    ["header", "header"],
                    ["rtReserveOnline", 3500.0],
                    ["rtReserveOnOffline", 4500.0],
                ],
                "nspinAwards": [
                    ["header", "header"],
                    ["nsrAwdGenWEo", 150.0],
                    ["nsrAwdGenWOs", 100.0],
                    ["nsrAwdLr", 80.0],
                    ["nsrAwdOffGen", 120.0],
                    ["nsrAwdQs", 60.0],
                    ["nsrAwdAs", 40.0],
                ],
                "telemeteredData": [
                    ["header", "header"],
                    ["telemHslEmr", 500.0],
                    ["telemHslOut", 200.0],
                    ["telemHslOutl", 150.0],
                ],
            },
        }

        df = self.iso._parse_system_as_capacity_monitor(fixture_json)

        assert df.shape[0] == 1
        assert "Time" in df.columns
        assert df.dtypes["Time"] == "datetime64[ns, US/Central]"
        assert df["Time"].iloc[0] == pd.Timestamp(
            "2025-12-05 06:30:00",
            tz="US/Central",
        )

        assert df["RRS Capability PFR Gen and ESR"].iloc[0] == 1000.5
        assert df["Reg Capability Reg Up"].iloc[0] == 800.0
        assert df["ECRS Capability Gen"].iloc[0] == 600.0
        assert df["PRC"].iloc[0] == 5000.0
        assert df["ORDC Online"].iloc[0] == 3500.0

    """get_settlement_points_electrical_bus_mapping"""

    settlement_points_electrical_bus_mapping_cols = [
        "Publish Date",
        "Electrical Bus",
        "Node Name",
        "PSSE Bus Name",
        "Voltage Level",
        "Substation",
        "Settlement Load Zone",
        "Resource Node",
        "Hub Bus Name",
        "Hub",
        "PSSE Bus Number",
    ]

    def test_get_settlement_points_electrical_bus_mapping(self):
        with api_vcr.use_cassette(
            "test_get_settlement_points_electrical_bus_mapping.yaml",
        ):
            df = self.iso.get_settlement_points_electrical_bus_mapping(date="latest")
        assert df.shape[0] > 0
        assert df.columns.tolist() == self.settlement_points_electrical_bus_mapping_cols
        assert df["Publish Date"].notna().all()
        assert isinstance(df["Publish Date"].iloc[0], datetime.date)
        assert not isinstance(df["Publish Date"].iloc[0], pd.Timestamp)

    """get_ccp_resource_names"""

    ccp_resource_names_cols = [
        "Publish Date",
        "CCP Name",
        "Logical Resource Node Name",
    ]

    def test_get_ccp_resource_names(self):
        with api_vcr.use_cassette("test_get_ccp_resource_names.yaml"):
            df = self.iso.get_ccp_resource_names(date="latest")
        assert df.shape[0] > 0
        assert df.columns.tolist() == self.ccp_resource_names_cols
        assert df["Publish Date"].notna().all()
        assert isinstance(df["Publish Date"].iloc[0], datetime.date)
        assert not isinstance(df["Publish Date"].iloc[0], pd.Timestamp)

    """get_noie_mapping"""

    noie_mapping_cols = [
        "Publish Date",
        "Physical Load",
        "NOIE",
        "Voltage",
        "Substation",
        "Electrical Bus",
    ]

    def test_get_noie_mapping(self):
        with api_vcr.use_cassette("test_get_noie_mapping.yaml"):
            df = self.iso.get_noie_mapping(date="latest")
        assert df.shape[0] > 0
        assert df.columns.tolist() == self.noie_mapping_cols
        assert df["Publish Date"].notna().all()
        assert isinstance(df["Publish Date"].iloc[0], datetime.date)
        assert not isinstance(df["Publish Date"].iloc[0], pd.Timestamp)

    """get_resource_node_to_unit"""

    resource_node_to_unit_cols = [
        "Publish Date",
        "Resource Node",
        "Unit Substation",
        "Unit Name",
    ]

    def test_get_resource_node_to_unit(self):
        with api_vcr.use_cassette("test_get_resource_node_to_unit.yaml"):
            df = self.iso.get_resource_node_to_unit(date="latest")
        assert df.shape[0] > 0
        assert df.columns.tolist() == self.resource_node_to_unit_cols
        assert df["Publish Date"].notna().all()
        assert isinstance(df["Publish Date"].iloc[0], datetime.date)
        assert not isinstance(df["Publish Date"].iloc[0], pd.Timestamp)

    """get_hub_name_dc_ties"""

    hub_name_dc_ties_cols = [
        "Publish Date",
        "Name",
    ]

    def test_get_hub_name_dc_ties(self):
        with api_vcr.use_cassette("test_get_hub_name_dc_ties.yaml"):
            df = self.iso.get_hub_name_dc_ties(date="latest")
        assert df.shape[0] > 0
        assert df.columns.tolist() == self.hub_name_dc_ties_cols
        assert df["Publish Date"].notna().all()
        assert isinstance(df["Publish Date"].iloc[0], datetime.date)
        assert not isinstance(df["Publish Date"].iloc[0], pd.Timestamp)


def check_load_forecast_by_model(df: pd.DataFrame) -> None:
    """Check load forecast by model DataFrame structure and types."""
    assert df.columns.tolist() == LOAD_FORECAST_BY_MODEL_COLUMNS
    assert ((df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)).all()
    assert df["Model"].notna().all()
    # Model column should have multiple unique values
    assert df["Model"].nunique() > 1

    # Verify exact dtypes for all columns
    assert pd.api.types.is_datetime64_any_dtype(df["Interval Start"])
    assert pd.api.types.is_datetime64_any_dtype(df["Interval End"])
    assert pd.api.types.is_datetime64_any_dtype(df["Publish Time"])
    assert df["Model"].dtype == object
    assert df["Coast"].dtype == float
    assert df["East"].dtype == float
    assert df["Far West"].dtype == float
    assert df["North"].dtype == float
    assert df["North Central"].dtype == float
    assert df["South Central"].dtype == float
    assert df["Southern"].dtype == float
    assert df["West"].dtype == float
    assert df["System Total"].dtype == float
    assert df["In Use Flag"].dtype == bool


def check_60_day_sced_disclosure(df_dict: Dict[str, pd.DataFrame]) -> None:
    load_resource = df_dict[SCED_LOAD_RESOURCE_KEY]
    gen_resource = df_dict[SCED_GEN_RESOURCE_KEY]
    smne = df_dict[SCED_SMNE_KEY]

    assert load_resource.columns.tolist() == SCED_LOAD_RESOURCE_COLUMNS
    assert gen_resource.columns.tolist() == SCED_GEN_RESOURCE_COLUMNS
    assert smne.columns.tolist() == SCED_SMNE_COLUMNS

    if SCED_ESR_KEY in df_dict:
        esr = df_dict[SCED_ESR_KEY]
        assert esr.columns.tolist() == SCED_ESR_COLUMNS
        assert len(esr) > 0
        assert esr["Resource Type"].unique().tolist() == ["ESR"]

    # AS Offer Updates and Resource AS Offers available starting 2025-12-05
    if SCED_AS_OFFER_UPDATES_IN_OP_HOUR_KEY in df_dict:
        as_offer_updates = df_dict[SCED_AS_OFFER_UPDATES_IN_OP_HOUR_KEY]
        assert (
            as_offer_updates.columns.tolist()
            == SCED_AS_OFFER_UPDATES_IN_OP_HOUR_COLUMNS
        )
        # Data may be empty for some dates
        if len(as_offer_updates) > 0:
            assert pd.api.types.is_datetime64_any_dtype(
                as_offer_updates["Interval Start"],
            )
            assert pd.api.types.is_datetime64_any_dtype(
                as_offer_updates["Interval End"],
            )

    if SCED_RESOURCE_AS_OFFERS_KEY in df_dict:
        resource_as_offers = df_dict[SCED_RESOURCE_AS_OFFERS_KEY]
        assert resource_as_offers.columns.tolist() == SCED_RESOURCE_AS_OFFERS_COLUMNS


def _make_sced_resource_as_offers_df(rows):
    """Build a DataFrame matching the raw SCED Resource AS Offers schema.

    Accepts dicts with explicit values. Missing PRICE/QUANTITY columns
    default to 0 (affected-period format) or can be set to np.nan by the
    caller (corrected-period format).
    """
    as_suffixes = ["URS", "DRS", "RRSPF", "RRSUF", "RRSFF", "NS", "ECRS"]
    n_blocks = 6
    all_cols = ["SCED Timestamp", "Resource Name"]
    for i in range(1, n_blocks + 1):
        for suffix in as_suffixes:
            all_cols.append(f"PRICE{i}_{suffix}")
        all_cols.append(f"QUANTITY_MW{i}")

    data = []
    for row in rows:
        record = {col: 0 for col in all_cols}
        record.update(row)
        data.append(record)
    return pd.DataFrame(data, columns=all_cols)


# fmt: off
# Actual rows from ERCOT 60-Day SCED Resource AS Offers, OD 2026-01-15
# (affected period: nulls converted to zeros)
_AEEC_ANTLP_3_ONRES_AFFECTED = {
    "SCED Timestamp": "2026-01-15 00:00:18",
    "Resource Name": "AEEC_ANTLP_3",
    "QUANTITY_MW1": 24.0, "QUANTITY_MW2": 9.8, "QUANTITY_MW3": 24.0,
    "QUANTITY_MW4": 0.0, "QUANTITY_MW5": 0.0, "QUANTITY_MW6": 54.0,
    "PRICE1_URS": 7.50, "PRICE2_URS": 0.0, "PRICE3_URS": 0.0,
    "PRICE4_URS": 0.0, "PRICE5_URS": 0.0, "PRICE6_URS": 1423.91,
    "PRICE1_DRS": 0.0, "PRICE2_DRS": 0.0, "PRICE3_DRS": 0.0,
    "PRICE4_DRS": 0.0, "PRICE5_DRS": 0.0, "PRICE6_DRS": 0.0,
    "PRICE1_RRSPF": 0.0, "PRICE2_RRSPF": 8.0, "PRICE3_RRSPF": 0.0,
    "PRICE4_RRSPF": 0.0, "PRICE5_RRSPF": 0.0, "PRICE6_RRSPF": 974.41,
    "PRICE1_RRSUF": 0.0, "PRICE2_RRSUF": 0.0, "PRICE3_RRSUF": 0.0,
    "PRICE4_RRSUF": 0.0, "PRICE5_RRSUF": 0.0, "PRICE6_RRSUF": 974.41,
    "PRICE1_RRSFF": 0.0, "PRICE2_RRSFF": 0.0, "PRICE3_RRSFF": 0.0,
    "PRICE4_RRSFF": 0.0, "PRICE5_RRSFF": 0.0, "PRICE6_RRSFF": 974.41,
    "PRICE1_NS": 0.0, "PRICE2_NS": 0.0, "PRICE3_NS": 8.0,
    "PRICE4_NS": 0.0, "PRICE5_NS": 0.0, "PRICE6_NS": 172.32,
    "PRICE1_ECRS": 0.0, "PRICE2_ECRS": 0.0, "PRICE3_ECRS": 0.0,
    "PRICE4_ECRS": 0.0, "PRICE5_ECRS": 0.0, "PRICE6_ECRS": 974.41,
}
_AEEC_ANTLP_3_REGDN_AFFECTED = {
    "SCED Timestamp": "2026-01-15 00:00:18",
    "Resource Name": "AEEC_ANTLP_3",
    "QUANTITY_MW1": 24.0, "QUANTITY_MW2": 0.0, "QUANTITY_MW3": 0.0,
    "QUANTITY_MW4": 0.0, "QUANTITY_MW5": 0.0, "QUANTITY_MW6": 54.0,
    "PRICE1_URS": 0.0, "PRICE2_URS": 0.0, "PRICE3_URS": 0.0,
    "PRICE4_URS": 0.0, "PRICE5_URS": 0.0, "PRICE6_URS": 0.0,
    "PRICE1_DRS": 10.0, "PRICE2_DRS": 0.0, "PRICE3_DRS": 0.0,
    "PRICE4_DRS": 0.0, "PRICE5_DRS": 0.0, "PRICE6_DRS": 1999.99,
    "PRICE1_RRSPF": 0.0, "PRICE2_RRSPF": 0.0, "PRICE3_RRSPF": 0.0,
    "PRICE4_RRSPF": 0.0, "PRICE5_RRSPF": 0.0, "PRICE6_RRSPF": 0.0,
    "PRICE1_RRSUF": 0.0, "PRICE2_RRSUF": 0.0, "PRICE3_RRSUF": 0.0,
    "PRICE4_RRSUF": 0.0, "PRICE5_RRSUF": 0.0, "PRICE6_RRSUF": 0.0,
    "PRICE1_RRSFF": 0.0, "PRICE2_RRSFF": 0.0, "PRICE3_RRSFF": 0.0,
    "PRICE4_RRSFF": 0.0, "PRICE5_RRSFF": 0.0, "PRICE6_RRSFF": 0.0,
    "PRICE1_NS": 0.0, "PRICE2_NS": 0.0, "PRICE3_NS": 0.0,
    "PRICE4_NS": 0.0, "PRICE5_NS": 0.0, "PRICE6_NS": 0.0,
    "PRICE1_ECRS": 0.0, "PRICE2_ECRS": 0.0, "PRICE3_ECRS": 0.0,
    "PRICE4_ECRS": 0.0, "PRICE5_ECRS": 0.0, "PRICE6_ECRS": 0.0,
}
_AEEC_ANTLP_3_OFFNS_AFFECTED = {
    "SCED Timestamp": "2026-01-15 00:00:18",
    "Resource Name": "AEEC_ANTLP_3",
    "QUANTITY_MW1": 54.0, "QUANTITY_MW2": 0.0, "QUANTITY_MW3": 0.0,
    "QUANTITY_MW4": 0.0, "QUANTITY_MW5": 0.0, "QUANTITY_MW6": 54.0,
    "PRICE1_URS": 0.0, "PRICE2_URS": 0.0, "PRICE3_URS": 0.0,
    "PRICE4_URS": 0.0, "PRICE5_URS": 0.0, "PRICE6_URS": 0.0,
    "PRICE1_DRS": 0.0, "PRICE2_DRS": 0.0, "PRICE3_DRS": 0.0,
    "PRICE4_DRS": 0.0, "PRICE5_DRS": 0.0, "PRICE6_DRS": 0.0,
    "PRICE1_RRSPF": 0.0, "PRICE2_RRSPF": 0.0, "PRICE3_RRSPF": 0.0,
    "PRICE4_RRSPF": 0.0, "PRICE5_RRSPF": 0.0, "PRICE6_RRSPF": 0.0,
    "PRICE1_RRSUF": 0.0, "PRICE2_RRSUF": 0.0, "PRICE3_RRSUF": 0.0,
    "PRICE4_RRSUF": 0.0, "PRICE5_RRSUF": 0.0, "PRICE6_RRSUF": 0.0,
    "PRICE1_RRSFF": 0.0, "PRICE2_RRSFF": 0.0, "PRICE3_RRSFF": 0.0,
    "PRICE4_RRSFF": 0.0, "PRICE5_RRSFF": 0.0, "PRICE6_RRSFF": 0.0,
    "PRICE1_NS": 8.0, "PRICE2_NS": 0.0, "PRICE3_NS": 0.0,
    "PRICE4_NS": 0.0, "PRICE5_NS": 0.0, "PRICE6_NS": 172.32,
    "PRICE1_ECRS": 0.0, "PRICE2_ECRS": 0.0, "PRICE3_ECRS": 0.0,
    "PRICE4_ECRS": 0.0, "PRICE5_ECRS": 0.0, "PRICE6_ECRS": 0.0,
}

# Same resource from OD 2026-02-03 (corrected period: proper nulls)
_N = np.nan
_AEEC_ANTLP_3_ONRES_CORRECTED = {
    "SCED Timestamp": "2026-02-03 00:00:23",
    "Resource Name": "AEEC_ANTLP_3",
    "QUANTITY_MW1": 16.0, "QUANTITY_MW2": 6.2, "QUANTITY_MW3": 16.0,
    "QUANTITY_MW4": 0.0, "QUANTITY_MW5": 0.0, "QUANTITY_MW6": 36.0,
    "PRICE1_URS": 7.50, "PRICE2_URS": _N, "PRICE3_URS": _N,
    "PRICE4_URS": _N, "PRICE5_URS": _N, "PRICE6_URS": 1420.8,
    "PRICE1_DRS": _N, "PRICE2_DRS": _N, "PRICE3_DRS": _N,
    "PRICE4_DRS": _N, "PRICE5_DRS": _N, "PRICE6_DRS": _N,
    "PRICE1_RRSPF": _N, "PRICE2_RRSPF": 8.0, "PRICE3_RRSPF": _N,
    "PRICE4_RRSPF": _N, "PRICE5_RRSPF": _N, "PRICE6_RRSPF": 742.99,
    "PRICE1_RRSUF": _N, "PRICE2_RRSUF": _N, "PRICE3_RRSUF": _N,
    "PRICE4_RRSUF": _N, "PRICE5_RRSUF": _N, "PRICE6_RRSUF": 742.99,
    "PRICE1_RRSFF": _N, "PRICE2_RRSFF": _N, "PRICE3_RRSFF": _N,
    "PRICE4_RRSFF": _N, "PRICE5_RRSFF": _N, "PRICE6_RRSFF": 742.99,
    "PRICE1_NS": _N, "PRICE2_NS": _N, "PRICE3_NS": 8.0,
    "PRICE4_NS": _N, "PRICE5_NS": _N, "PRICE6_NS": 183.16,
    "PRICE1_ECRS": _N, "PRICE2_ECRS": _N, "PRICE3_ECRS": _N,
    "PRICE4_ECRS": _N, "PRICE5_ECRS": _N, "PRICE6_ECRS": 742.99,
}
_AEEC_ANTLP_3_REGDN_CORRECTED = {
    "SCED Timestamp": "2026-02-03 00:00:23",
    "Resource Name": "AEEC_ANTLP_3",
    "QUANTITY_MW1": 16.0, "QUANTITY_MW2": 0.0, "QUANTITY_MW3": 0.0,
    "QUANTITY_MW4": 0.0, "QUANTITY_MW5": 0.0, "QUANTITY_MW6": 36.0,
    "PRICE1_URS": _N, "PRICE2_URS": _N, "PRICE3_URS": _N,
    "PRICE4_URS": _N, "PRICE5_URS": _N, "PRICE6_URS": _N,
    "PRICE1_DRS": 10.0, "PRICE2_DRS": _N, "PRICE3_DRS": _N,
    "PRICE4_DRS": _N, "PRICE5_DRS": _N, "PRICE6_DRS": 1999.99,
    "PRICE1_RRSPF": _N, "PRICE2_RRSPF": _N, "PRICE3_RRSPF": _N,
    "PRICE4_RRSPF": _N, "PRICE5_RRSPF": _N, "PRICE6_RRSPF": _N,
    "PRICE1_RRSUF": _N, "PRICE2_RRSUF": _N, "PRICE3_RRSUF": _N,
    "PRICE4_RRSUF": _N, "PRICE5_RRSUF": _N, "PRICE6_RRSUF": _N,
    "PRICE1_RRSFF": _N, "PRICE2_RRSFF": _N, "PRICE3_RRSFF": _N,
    "PRICE4_RRSFF": _N, "PRICE5_RRSFF": _N, "PRICE6_RRSFF": _N,
    "PRICE1_NS": _N, "PRICE2_NS": _N, "PRICE3_NS": _N,
    "PRICE4_NS": _N, "PRICE5_NS": _N, "PRICE6_NS": _N,
    "PRICE1_ECRS": _N, "PRICE2_ECRS": _N, "PRICE3_ECRS": _N,
    "PRICE4_ECRS": _N, "PRICE5_ECRS": _N, "PRICE6_ECRS": _N,
}
_AEEC_ANTLP_3_OFFNS_CORRECTED = {
    "SCED Timestamp": "2026-02-03 00:00:23",
    "Resource Name": "AEEC_ANTLP_3",
    "QUANTITY_MW1": 36.0, "QUANTITY_MW2": 0.0, "QUANTITY_MW3": 0.0,
    "QUANTITY_MW4": 0.0, "QUANTITY_MW5": 0.0, "QUANTITY_MW6": 36.0,
    "PRICE1_URS": _N, "PRICE2_URS": _N, "PRICE3_URS": _N,
    "PRICE4_URS": _N, "PRICE5_URS": _N, "PRICE6_URS": _N,
    "PRICE1_DRS": _N, "PRICE2_DRS": _N, "PRICE3_DRS": _N,
    "PRICE4_DRS": _N, "PRICE5_DRS": _N, "PRICE6_DRS": _N,
    "PRICE1_RRSPF": _N, "PRICE2_RRSPF": _N, "PRICE3_RRSPF": _N,
    "PRICE4_RRSPF": _N, "PRICE5_RRSPF": _N, "PRICE6_RRSPF": _N,
    "PRICE1_RRSUF": _N, "PRICE2_RRSUF": _N, "PRICE3_RRSUF": _N,
    "PRICE4_RRSUF": _N, "PRICE5_RRSUF": _N, "PRICE6_RRSUF": _N,
    "PRICE1_RRSFF": _N, "PRICE2_RRSFF": _N, "PRICE3_RRSFF": _N,
    "PRICE4_RRSFF": _N, "PRICE5_RRSFF": _N, "PRICE6_RRSFF": _N,
    "PRICE1_NS": 8.0, "PRICE2_NS": _N, "PRICE3_NS": _N,
    "PRICE4_NS": _N, "PRICE5_NS": _N, "PRICE6_NS": 183.16,
    "PRICE1_ECRS": _N, "PRICE2_ECRS": _N, "PRICE3_ECRS": _N,
    "PRICE4_ECRS": _N, "PRICE5_ECRS": _N, "PRICE6_ECRS": _N,
}
# fmt: on


class TestProcessScedResourceAsOffers:
    """Tests for process_sced_resource_as_offers curve type detection.

    ERCOT notice M-B040326-01: corrected files use NaN instead of zero
    for empty AS Sub-Type Offer Prices. These tests use actual rows from
    AEEC_ANTLP_3 on OD 2026-01-15 (affected) and OD 2026-02-03 (corrected)
    to verify curve type classification works with both formats.
    """

    def test_online_with_zeros(self):
        """Affected AEEC_ANTLP_3 ONRES row: zeros in inactive AS types."""
        df = _make_sced_resource_as_offers_df([_AEEC_ANTLP_3_ONRES_AFFECTED])
        result = process_sced_resource_as_offers(df)
        assert result["Curve Type"].iloc[0] == "Online"

    def test_online_with_nans(self):
        """Corrected AEEC_ANTLP_3 ONRES row: NaN in inactive AS types."""
        df = _make_sced_resource_as_offers_df([_AEEC_ANTLP_3_ONRES_CORRECTED])
        result = process_sced_resource_as_offers(df)
        assert result["Curve Type"].iloc[0] == "Online"

    def test_regulation_down_with_zeros(self):
        """Affected AEEC_ANTLP_3 REGDN row: zeros in all non-DRS columns."""
        df = _make_sced_resource_as_offers_df([_AEEC_ANTLP_3_REGDN_AFFECTED])
        result = process_sced_resource_as_offers(df)
        assert result["Curve Type"].iloc[0] == "Regulation Down"

    def test_regulation_down_with_nans(self):
        """Corrected AEEC_ANTLP_3 REGDN row: NaN in all non-DRS columns."""
        df = _make_sced_resource_as_offers_df([_AEEC_ANTLP_3_REGDN_CORRECTED])
        result = process_sced_resource_as_offers(df)
        assert result["Curve Type"].iloc[0] == "Regulation Down"

    def test_offline_with_zeros(self):
        """Affected AEEC_ANTLP_3 OFFNS row: zeros in all non-NS columns."""
        df = _make_sced_resource_as_offers_df([_AEEC_ANTLP_3_OFFNS_AFFECTED])
        result = process_sced_resource_as_offers(df)
        assert result["Curve Type"].iloc[0] == "Offline"

    def test_offline_with_nans(self):
        """Corrected AEEC_ANTLP_3 OFFNS row: NaN in all non-NS columns."""
        df = _make_sced_resource_as_offers_df([_AEEC_ANTLP_3_OFFNS_CORRECTED])
        result = process_sced_resource_as_offers(df)
        assert result["Curve Type"].iloc[0] == "Offline"

    def test_three_rows_per_resource_with_nans(self):
        """Corrected AEEC_ANTLP_3: all 3 rows classified correctly."""
        df = _make_sced_resource_as_offers_df(
            [
                _AEEC_ANTLP_3_ONRES_CORRECTED,
                _AEEC_ANTLP_3_REGDN_CORRECTED,
                _AEEC_ANTLP_3_OFFNS_CORRECTED,
            ],
        )
        result = process_sced_resource_as_offers(df)
        assert list(result["Curve Type"]) == [
            "Online",
            "Regulation Down",
            "Offline",
        ]

    def test_corrected_curves_exclude_nan_blocks(self):
        """Corrected ONRES row: only blocks with real prices appear in curves."""
        df = _make_sced_resource_as_offers_df([_AEEC_ANTLP_3_ONRES_CORRECTED])
        result = process_sced_resource_as_offers(df)

        # URS has prices in blocks 1 (7.50) and 6 (1420.8)
        urs_curve = result["URS Offer Curve"].iloc[0]
        assert urs_curve == [[16.0, 7.5], [36.0, 1420.8]]

        # RRSPF has prices in blocks 2 (8.0) and 6 (742.99)
        rrspf_curve = result["RRSPFR Offer Curve"].iloc[0]
        assert rrspf_curve == [[6.2, 8.0], [36.0, 742.99]]

        # DRS is entirely NaN — should be None
        assert result["DRS Offer Curve"].iloc[0] is None

    def test_output_columns(self):
        """Verify processed output has the standard column set."""
        df = _make_sced_resource_as_offers_df([_AEEC_ANTLP_3_ONRES_CORRECTED])
        result = process_sced_resource_as_offers(df)
        assert result.columns.tolist() == SCED_RESOURCE_AS_OFFERS_COLUMNS


def check_60_day_dam_disclosure(df_dict):
    assert df_dict is not None

    dam_gen_resource = df_dict[DAM_GEN_RESOURCE_KEY]
    dam_gen_resource_as_offers = df_dict[DAM_GEN_RESOURCE_AS_OFFERS_KEY]
    dam_load_resource = df_dict[DAM_LOAD_RESOURCE_KEY]
    dam_load_resource_as_offers = df_dict[DAM_LOAD_RESOURCE_AS_OFFERS_KEY]
    dam_energy_only_offer_awards = df_dict[DAM_ENERGY_ONLY_OFFER_AWARDS_KEY]
    dam_energy_only_offers = df_dict[DAM_ENERGY_ONLY_OFFERS_KEY]
    dam_ptp_obligation_bid_awards = df_dict[DAM_PTP_OBLIGATION_BID_AWARDS_KEY]
    dam_ptp_obligation_bids = df_dict[DAM_PTP_OBLIGATION_BIDS_KEY]
    dam_energy_bid_awards = df_dict[DAM_ENERGY_BID_AWARDS_KEY]
    dam_energy_bids = df_dict[DAM_ENERGY_BIDS_KEY]
    dam_ptp_obligation_option = df_dict[DAM_PTP_OBLIGATION_OPTION_KEY]
    dam_ptp_obligation_option_awards = df_dict[DAM_PTP_OBLIGATION_OPTION_AWARDS_KEY]

    assert dam_gen_resource.columns.tolist() == DAM_GEN_RESOURCE_COLUMNS
    assert dam_gen_resource_as_offers.columns.tolist() == DAM_RESOURCE_AS_OFFERS_COLUMNS
    assert dam_load_resource.columns.tolist() == DAM_LOAD_RESOURCE_COLUMNS

    assert (
        dam_load_resource_as_offers.columns.tolist() == DAM_RESOURCE_AS_OFFERS_COLUMNS
    )

    assert (
        dam_energy_only_offer_awards.columns.tolist()
        == DAM_ENERGY_ONLY_OFFER_AWARDS_COLUMNS
    )

    assert dam_energy_only_offers.columns.tolist() == DAM_ENERGY_ONLY_OFFERS_COLUMNS

    assert (
        dam_ptp_obligation_bid_awards.columns.tolist()
        == DAM_PTP_OBLIGATION_BID_AWARDS_COLUMNS
    )

    assert dam_ptp_obligation_bids.columns.tolist() == DAM_PTP_OBLIGATION_BIDS_COLUMNS

    assert dam_energy_bid_awards.columns.tolist() == DAM_ENERGY_BID_AWARDS_COLUMNS
    assert dam_energy_bids.columns.tolist() == DAM_ENERGY_BIDS_COLUMNS

    assert (
        dam_ptp_obligation_option.columns.tolist() == DAM_PTP_OBLIGATION_OPTION_COLUMNS
    )

    assert (
        dam_ptp_obligation_option_awards.columns.tolist()
        == DAM_PTP_OBLIGATION_OPTION_AWARDS_COLUMNS
    )

    assert not dam_gen_resource_as_offers.duplicated(
        subset=["Interval Start", "Interval End", "QSE", "DME", "Resource Name"],
    ).any()

    assert not dam_load_resource_as_offers.duplicated(
        subset=["Interval Start", "Interval End", "QSE", "DME", "Resource Name"],
    ).any()


_AS_ONLY_PK = ["Interval Start", "QSE", "AS Type", "Offer ID"]


def _check_dam_as_only_awards(df):
    assert df.columns.tolist() == DAM_AS_ONLY_AWARDS_COLUMNS
    assert len(df) > 0

    # Hour Ending - 1 hour => exactly 1-hour intervals
    assert ((df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)).all()

    # Primary-key components non-null and unique
    for pk_col in _AS_ONLY_PK:
        assert df[pk_col].notna().all(), f"{pk_col} has nulls"
    assert not df.duplicated(subset=_AS_ONLY_PK).any()

    # All quantity/price columns parse as numeric
    for col in [
        "Quantity1 Award",
        "Quantity2 Award",
        "Quantity3 Award",
        "Quantity4 Award",
        "Quantity5 Award",
        "Total Award",
        "MCPC",
    ]:
        assert pd.api.types.is_numeric_dtype(df[col]), f"{col} is not numeric"

    # Total Award should equal the sum of Quantity1..5 Award per row
    quantity_cols = [f"Quantity{i} Award" for i in range(1, 6)]
    assert np.allclose(
        df[quantity_cols].sum(axis=1).astype(float),
        df["Total Award"].astype(float),
    )


def _check_dam_as_only_offers(df):
    assert df.columns.tolist() == DAM_AS_ONLY_OFFERS_COLUMNS
    assert len(df) > 0

    assert ((df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)).all()

    for pk_col in _AS_ONLY_PK:
        assert df[pk_col].notna().all(), f"{pk_col} has nulls"
    assert not df.duplicated(subset=_AS_ONLY_PK).any()

    # Every non-null Offer Curve must be a non-empty list of [mw, price] pairs
    non_null = df["Offer Curve"].dropna()
    assert len(non_null) > 0
    for curve in non_null:
        assert isinstance(curve, list) and len(curve) > 0
        for point in curve:
            assert len(point) == 2
            mw, price = point
            assert isinstance(mw, (int, float))
            assert isinstance(price, (int, float))


def _list_to_pg_string(lst):
    """Convert a list-of-lists like [[1.0, 2.0], [3.0, 4.0]] to PG array string."""
    return str(lst).replace("[", "{").replace("]", "}").replace(" ", "")


class TestExtractCurveFormats:
    """Equivalence tests between list and pg_array output formats."""

    def _make_auto_detect_df(self, n_rows=100, n_blocks=5, curve_name="TestCurve"):
        """Create a synthetic DataFrame with auto-detect column naming."""

        rng = np.random.default_rng(42)
        data = {}
        for i in range(1, n_blocks + 1):
            data[f"{curve_name}-MW{i}"] = rng.uniform(10, 500, n_rows)
            data[f"{curve_name}-Price{i}"] = rng.uniform(5, 100, n_rows)
        return pd.DataFrame(data)

    def _make_explicit_cols_df(self, n_rows=100, n_blocks=6):
        """Create a synthetic DataFrame with explicit column naming (SCED-style)."""

        rng = np.random.default_rng(42)
        data = {}
        for i in range(1, n_blocks + 1):
            data[f"QUANTITY_MW{i}"] = rng.uniform(10, 500, n_rows)
            data[f"PRICE{i}_URS"] = rng.uniform(5, 100, n_rows)
        return pd.DataFrame(data)

    def test_extract_curve_list_vs_pg_array_auto_detect(self):
        """Test that pg_array output matches list output (auto-detect)."""
        df = self._make_auto_detect_df()
        list_result = extract_curve(
            df,
            curve_name="TestCurve",
            output_format=CurveOutputFormat.LIST,
        )
        pg_result = extract_curve(
            df,
            curve_name="TestCurve",
            output_format=CurveOutputFormat.PG_ARRAY_AS_STRING,
        )

        assert len(list_result) == len(pg_result)
        for i in range(len(list_result)):
            assert _list_to_pg_string(list_result.iloc[i]) == pg_result.iloc[i]

    def test_extract_curve_list_vs_pg_array_explicit_cols(self):
        """Test that pg_array output matches list output (explicit columns)."""
        df = self._make_explicit_cols_df()
        mw_cols = [f"QUANTITY_MW{i}" for i in range(1, 7)]
        price_cols = [f"PRICE{i}_URS" for i in range(1, 7)]

        list_result = extract_curve(
            df,
            mw_cols=mw_cols,
            price_cols=price_cols,
            output_format=CurveOutputFormat.LIST,
        )
        pg_result = extract_curve(
            df,
            mw_cols=mw_cols,
            price_cols=price_cols,
            output_format=CurveOutputFormat.PG_ARRAY_AS_STRING,
        )

        assert len(list_result) == len(pg_result)
        for i in range(len(list_result)):
            assert _list_to_pg_string(list_result.iloc[i]) == pg_result.iloc[i]

    def test_extract_curve_pg_array_edge_cases(self):
        """Test edge cases: all-NaN rows, partial NaN rows, single-block curves."""

        # All-NaN row
        df_nan = pd.DataFrame(
            {
                "C-MW1": [np.nan, 100.0],
                "C-MW2": [np.nan, 200.0],
                "C-Price1": [np.nan, 10.0],
                "C-Price2": [np.nan, 20.0],
            },
        )
        list_result = extract_curve(
            df_nan,
            curve_name="C",
            output_format=CurveOutputFormat.LIST,
        )
        pg_result = extract_curve(
            df_nan,
            curve_name="C",
            output_format=CurveOutputFormat.PG_ARRAY_AS_STRING,
        )

        # All-NaN row should produce None for both formats
        assert list_result.iloc[0] is None
        assert pg_result.iloc[0] is None

        # Valid row should match
        assert _list_to_pg_string(list_result.iloc[1]) == pg_result.iloc[1]

        # Partial NaN - only first block valid
        df_partial = pd.DataFrame(
            {
                "C-MW1": [100.0],
                "C-MW2": [np.nan],
                "C-Price1": [10.0],
                "C-Price2": [np.nan],
            },
        )
        list_result = extract_curve(
            df_partial,
            curve_name="C",
            output_format=CurveOutputFormat.LIST,
        )
        pg_result = extract_curve(
            df_partial,
            curve_name="C",
            output_format=CurveOutputFormat.PG_ARRAY_AS_STRING,
        )
        assert _list_to_pg_string(list_result.iloc[0]) == pg_result.iloc[0]

        # Single-block curve
        df_single = pd.DataFrame({"C-MW1": [50.0, 75.0], "C-Price1": [25.0, 30.0]})
        list_result = extract_curve(
            df_single,
            curve_name="C",
            output_format=CurveOutputFormat.LIST,
        )
        pg_result = extract_curve(
            df_single,
            curve_name="C",
            output_format=CurveOutputFormat.PG_ARRAY_AS_STRING,
        )
        for i in range(len(list_result)):
            assert _list_to_pg_string(list_result.iloc[i]) == pg_result.iloc[i]

    def _make_as_offer_curves_df(self):
        """Create synthetic input for process_as_offer_curves."""
        n_blocks = 3
        services = ["RRSPFR", "REGUP"]
        rows = []
        for resource in ["RES_A", "RES_B"]:
            row = {
                "Interval Start": pd.Timestamp("2025-01-01 00:00"),
                "Interval End": pd.Timestamp("2025-01-01 01:00"),
                "Resource Name": resource,
                "QSE": "QSE1",
                "DME": "DME1",
                "Multi-Hour Block Flag": "N",
            }
            for i in range(1, n_blocks + 1):
                row[f"BLOCK INDICATOR{i}"] = "ON"
                row[f"QUANTITY MW{i}"] = 100.0 * i
                for svc in services:
                    row[f"PRICE{i} {svc}"] = 10.0 * i + (0.5 if svc == "REGUP" else 0)
            rows.append(row)
        return pd.DataFrame(rows)

    def test_process_as_offer_curves_list_vs_pg_array(self):
        """Curves from list format match pg_array_as_string when converted."""
        df = self._make_as_offer_curves_df()
        list_result = process_as_offer_curves(
            df.copy(),
            output_format=CurveOutputFormat.LIST,
        )
        pg_result = process_as_offer_curves(
            df.copy(),
            output_format=CurveOutputFormat.PG_ARRAY_AS_STRING,
        )

        assert list(list_result.columns) == list(pg_result.columns)
        assert len(list_result) == len(pg_result)

        # Non-curve columns should match
        non_curve_cols = [
            c for c in list_result.columns if not c.endswith("Offer Curve")
        ]
        for col in non_curve_cols:
            assert list(list_result[col]) == list(pg_result[col])

        # Curve columns: convert list to pg string and compare
        curve_cols = [c for c in list_result.columns if c.endswith("Offer Curve")]
        for col in curve_cols:
            for i in range(len(list_result)):
                list_val = list_result[col].iloc[i]
                pg_val = pg_result[col].iloc[i]
                if list_val is pd.NA:
                    assert pg_val is pd.NA
                else:
                    assert _list_to_pg_string(list_val) == pg_val

    def test_curve_output_format_string_compat(self):
        """Test that raw string args still work with CurveOutputFormat comparisons."""
        assert CurveOutputFormat.LIST == "list"
        assert CurveOutputFormat.PG_ARRAY_AS_STRING == "pg_array_as_string"
        assert "list" == CurveOutputFormat.LIST
        assert "pg_array_as_string" == CurveOutputFormat.PG_ARRAY_AS_STRING


class TestCategorizeStrings:
    """Tests for _categorize_strings() helper."""

    def test_categorize_strings_converts_object_columns(self):
        """Non-curve object columns are converted to category dtype."""
        df = pd.DataFrame(
            {
                "Resource Name": ["RES_A", "RES_B", "RES_A"],
                "QSE": ["QSE1", "QSE2", "QSE1"],
                "HSL": [100.0, 200.0, 150.0],
                "SCED1 Offer Curve": [[[1, 2]], [[3, 4]], [[5, 6]]],
                "URS Offer Curve": ["{{1,2}}", "{{3,4}}", "{{5,6}}"],
                "Block Indicators": [["A"], ["B"], ["C"]],
            },
        )
        result = _categorize_strings(df)

        # String columns should be category
        assert result["Resource Name"].dtype.name == "category"
        assert result["QSE"].dtype.name == "category"

        # Numeric column unchanged
        assert result["HSL"].dtype == "float64"

        # Curve columns should remain object
        assert result["SCED1 Offer Curve"].dtype == "object"
        assert result["URS Offer Curve"].dtype == "object"

        # Block Indicators should remain object
        assert result["Block Indicators"].dtype == "object"

    def test_categorize_strings_no_object_columns(self):
        """DataFrame with no object columns is returned unchanged."""
        df = pd.DataFrame({"A": [1, 2, 3], "B": [4.0, 5.0, 6.0]})
        result = _categorize_strings(df)
        assert result["A"].dtype == "int64"
        assert result["B"].dtype == "float64"

    def test_categorize_strings_preserves_values(self):
        """Category conversion preserves actual string values."""
        df = pd.DataFrame({"Resource Name": ["RES_A", "RES_B", "RES_A"]})
        result = _categorize_strings(df)
        assert list(result["Resource Name"]) == ["RES_A", "RES_B", "RES_A"]
