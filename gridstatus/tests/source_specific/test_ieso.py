import datetime
from xml.etree import ElementTree

import numpy as np
import pandas as pd
import pytest
from pandas.core.dtypes.common import is_numeric_dtype

from gridstatus import IESO, utils
from gridstatus.base import NotSupported
from gridstatus.ieso import _safe_find_float, _safe_find_int, _safe_find_text
from gridstatus.ieso_constants import (
    INTERTIE_ACTUAL_SCHEDULE_FLOW_HOURLY_COLUMNS,
    INTERTIE_FLOW_5_MIN_COLUMNS,
    MAXIMUM_DAYS_IN_PAST_FOR_COMPLETE_GENERATOR_REPORT,
    ONTARIO_LOCATION,
    RESOURCE_ADEQUACY_REPORT_DATA_STRUCTURE_MAP,
    ZONAL_LOAD_COLUMNS,
)
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

TIME_COLUMN = "Interval Start"

file_vcr = setup_vcr(
    source="ieso",
    record_mode=RECORD_MODE,
)


class TestIESO(BaseTestISO):
    iso = IESO()
    default_timezone = iso.default_timezone

    def test_init(self):
        assert self.iso is not None

    """get_fuel_mix"""

    # start is not a valid keyword argument for get_fuel_mix for IESO
    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_date_or_start(self):
        pass

    @pytest.mark.integration
    def test_get_fuel_mix_historical(self):
        super().test_get_fuel_mix_historical(time_column=TIME_COLUMN)

    @pytest.mark.integration
    def test_get_fuel_mix_historical_with_date_range(self):
        super().test_get_fuel_mix_historical_with_date_range(
            time_column=TIME_COLUMN,
        )

    @pytest.mark.integration
    def test_get_fuel_mix_range_two_days_with_day_start_endpoint(self):
        yesterday = utils._handle_date(
            "today",
            self.iso.default_timezone,
        ) - pd.Timedelta(days=1)
        yesterday = yesterday.replace(hour=1, minute=0, second=0, microsecond=0)
        start = yesterday - pd.Timedelta(hours=3)

        df = self.iso.get_fuel_mix(date=start, end=yesterday + pd.Timedelta(minutes=1))

        assert df[TIME_COLUMN].max() >= yesterday.replace(
            hour=0,
            minute=0,
            second=0,
        )
        assert df[TIME_COLUMN].min() <= start

    @pytest.mark.integration
    def test_get_fuel_mix_start_end_same_day(self):
        yesterday = utils._handle_date(
            "today",
            self.iso.default_timezone,
        ) - pd.Timedelta(days=1)
        start = yesterday.replace(hour=0, minute=5, second=0, microsecond=0)
        end = yesterday.replace(hour=6, minute=5, second=0, microsecond=0)
        df = self.iso.get_fuel_mix(date=start, end=end)
        # ignore last row, since it is sometime midnight of next day
        assert df[TIME_COLUMN].iloc[:-1].dt.date.unique().tolist() == [
            yesterday.date(),
        ]
        self._check_fuel_mix(df)

    @pytest.mark.integration
    def test_get_fuel_mix_latest(self):
        super().test_get_fuel_mix_latest(time_column=TIME_COLUMN)

    @pytest.mark.integration
    def test_get_fuel_mix_in_future_raises_error(self):
        with pytest.raises(NotSupported):
            self.iso.get_fuel_mix(
                pd.Timestamp.now(tz=self.default_timezone).date()
                + pd.Timedelta(days=1),
            )

    """get_generator_report_hourly"""

    def test_get_generator_report_hourly_historical(self):
        date = pd.Timestamp("2025-04-19", tz=self.default_timezone)
        date_str = date.strftime("%m/%d/%Y")

        with file_vcr.use_cassette(
            f"test_get_generator_report_hourly_historical_{date.date()}.yaml",
        ):
            df = self.iso.get_generator_report_hourly(date_str)
            assert isinstance(df, pd.DataFrame)
            assert df.loc[0][TIME_COLUMN].strftime("%m/%d/%Y") == date_str
            assert df.loc[0][TIME_COLUMN].tz is not None
            self._check_get_generator_report_hourly(df)

            timestamp_obj = date.date()
            df = self.iso.get_generator_report_hourly(timestamp_obj)
            assert isinstance(df, pd.DataFrame)
            assert df.loc[0][TIME_COLUMN].strftime(
                "%Y%m%d",
            ) == timestamp_obj.strftime("%Y%m%d")
            assert df.loc[0][TIME_COLUMN].tz is not None
            self._check_get_generator_report_hourly(df)

            date_obj = date.date()
            df = self.iso.get_generator_report_hourly(date_obj)
            assert isinstance(df, pd.DataFrame)
            assert df.loc[0][TIME_COLUMN].strftime(
                "%Y%m%d",
            ) == date_obj.strftime("%Y%m%d")
            assert df.loc[0][TIME_COLUMN].tz is not None
            self._check_get_generator_report_hourly(df)

    def test_get_generator_report_hourly_historical_with_date_range(self):
        start = pd.Timestamp("2025-04-19", tz=self.default_timezone)
        end = start + pd.Timedelta(days=7)

        with file_vcr.use_cassette(
            f"test_get_generator_report_hourly_historical_with_date_range_{start.date()}_{end.date()}.yaml",
        ):
            df = self.iso.get_generator_report_hourly(
                date=start.date(),
                end=end.date(),
            )
            self._check_get_generator_report_hourly(df)
            assert df[TIME_COLUMN].dt.day.nunique() == 7

    def test_get_generator_report_hourly_range_two_days_with_end(self):
        start = pd.Timestamp("2025-04-19", tz=self.default_timezone)
        end = start + pd.Timedelta(hours=3)

        with file_vcr.use_cassette(
            f"test_get_generator_report_hourly_range_two_days_with_end_{start.date()}_{end.date()}.yaml",
        ):
            df = self.iso.get_generator_report_hourly(
                date=start,
                end=end,
            )
            assert df[TIME_COLUMN].max() >= end.replace(
                hour=0,
                minute=0,
                second=0,
            )
            assert df[TIME_COLUMN].min() <= start
            self._check_get_generator_report_hourly(df)

    def test_get_generator_report_hourly_start_end_same_day(self):
        start = pd.Timestamp("2025-04-19", tz=self.default_timezone)
        end = start + pd.Timedelta(hours=6)

        with file_vcr.use_cassette(
            f"test_get_generator_report_hourly_start_end_same_day_{start.date()}.yaml",
        ):
            df = self.iso.get_generator_report_hourly(date=start, end=end)
            assert df[TIME_COLUMN].iloc[:-1].dt.date.unique().tolist() == [start.date()]
            self._check_get_generator_report_hourly(df)

    def test_get_generator_report_hourly_latest(self):
        with file_vcr.use_cassette("test_get_generator_report_hourly_latest.yaml"):
            df = self.iso.get_generator_report_hourly("latest")
            self._check_get_generator_report_hourly(df)
            assert df[TIME_COLUMN].min() == pd.Timestamp.now(
                tz=self.default_timezone,
            ).floor("D")
            assert df[TIME_COLUMN].max() >= pd.Timestamp.now(
                tz=self.default_timezone,
            ).floor("h") - pd.Timedelta(hours=2)

    def test_get_generator_report_hourly_today(self):
        with file_vcr.use_cassette("test_get_generator_report_hourly_today.yaml"):
            df = self.iso.get_generator_report_hourly("today")
            assert df.equals(self.iso.get_generator_report_hourly("latest"))

    def test_get_generator_report_hourly_too_far_in_past_raises_error(self):
        with file_vcr.use_cassette(
            "test_get_generator_report_hourly_too_far_in_past.yaml",
        ):
            with pytest.raises(NotSupported):
                self.iso.get_generator_report_hourly(
                    pd.Timestamp.now(tz=self.default_timezone).date()
                    - pd.Timedelta(
                        days=MAXIMUM_DAYS_IN_PAST_FOR_COMPLETE_GENERATOR_REPORT + 1,
                    ),
                )

    def test_get_generator_report_hourly_in_future_raises_error(self):
        with file_vcr.use_cassette("test_get_generator_report_hourly_in_future.yaml"):
            with pytest.raises(NotSupported):
                self.iso.get_generator_report_hourly(
                    pd.Timestamp.now(tz=self.default_timezone).date()
                    + pd.Timedelta(days=1),
                )

    """get_interconnection_queue"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_interconnection_queue(self):
        pass

    """get_lmp"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_lmp_date_range(self, market=None):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_lmp_historical(self, market=None):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_lmp_latest(self, market=None):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_lmp_today(self, market=None):
        pass

    @pytest.mark.skip(reason="Method no longer supported after IESO Market Renewal")
    def test_get_load_today(self):
        with pytest.raises(NotSupported):
            self.iso.get_load("today")

    @pytest.mark.skip(reason="Method no longer supported after IESO Market Renewal")
    def test_get_load_latest(self):
        with pytest.raises(NotSupported):
            self.iso.get_load("latest")

    @pytest.mark.skip(reason="Method no longer supported after IESO Market Renewal")
    def test_get_load_historical(self):
        with pytest.raises(NotSupported):
            self.iso.get_load("today")

    @pytest.mark.skip(reason="Method no longer supported after IESO Market Renewal")
    def test_get_load_historical_with_date_range(self):
        with pytest.raises(NotSupported):
            self.iso.get_load("today")

    def test_get_load_not_supported(self):
        with pytest.raises(NotSupported):
            self.iso.get_load("today")

    """get_load_forecast"""

    @pytest.mark.skip(reason="Method no longer supported after IESO Market Renewal")
    def test_get_load_forecast_today(self):
        with pytest.raises(NotSupported):
            self.iso.get_load_forecast("today")

    @pytest.mark.skip(reason="Method no longer supported after IESO Market Renewal")
    def test_get_load_forecast_historical(self):
        with pytest.raises(NotSupported):
            self.iso.get_load_forecast("today")

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical_with_date_range(self):
        pass

    """get_status"""

    @pytest.mark.integration
    def test_get_status_latest(self):
        # ensure there is a homepage if gridstatus can retrieve a status
        assert isinstance(self.iso.status_homepage, str)

    """get_storage"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_storage_historical(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_storage_today(self):
        pass

    # TODO: this is overridden in the base class
    def _check_time_columns(self, df, instant_or_interval="interval"):
        assert isinstance(df, pd.DataFrame)

        time_cols = [TIME_COLUMN, "Interval End"]
        ordered_by_col = TIME_COLUMN

        assert time_cols == df.columns[: len(time_cols)].tolist()
        # check all time cols are localized timestamps
        for col in time_cols:
            assert isinstance(df.loc[0][col], pd.Timestamp)
            assert df.loc[0][col].tz is not None

        self._check_ordered_by_time(df, ordered_by_col)

    def _check_fuel_mix(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.name is None

        time_type = "interval"
        self._check_time_columns(df, instant_or_interval=time_type)

        assert list(df.columns) == [
            "Interval Start",
            "Interval End",
            "Biofuel",
            "Gas",
            "Hydro",
            "Nuclear",
            "Solar",
            "Wind",
            "Other",
        ]

    def _check_get_generator_report_hourly(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.shape[0] >= 0

        time_type = "interval"
        self._check_time_columns(df, instant_or_interval=time_type)

        for col in [
            "Output MW",
            "Capability MW",
            "Available Capacity MW",
            "Forecast MW",
        ]:
            assert col in df.columns
            assert is_numeric_dtype(df[col])

        for col in ["Generator Name", "Fuel Type"]:
            assert col in df.columns
            assert df[col].dtype == "object"

        assert list(df["Fuel Type"].unique()) == [
            "BIOFUEL",
            "GAS",
            "HYDRO",
            "NUCLEAR",
            "OTHER",
            "SOLAR",
            "WIND",
        ]

    """get_mcp_real_time_5_min"""

    def _check_mcp(self, df: pd.DataFrame) -> None:
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Location",
            "Non-sync 10 Min",
            "Sync 10 Min",
            "Reserves 30 Min",
            "Energy",
        ]

        assert sorted(df["Location"].unique()) == [
            "Manitoba",
            "Manitoba SK",
            "Michigan",
            "Minnesota",
            "New-York",
            "Ontario",
            "Quebec AT",
            "Quebec B5D.B31L",
            "Quebec D4Z",
            "Quebec D5A",
            "Quebec H4Z",
            "Quebec H9A",
            "Quebec P33C",
            "Quebec Q4C",
            "Quebec X2Y",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()

    def test_get_mcp_real_time_5_min_date_range(self):
        with pytest.raises(NotSupported):
            self.iso.get_mcp_real_time_5_min()

    """get_mcp_historical_5_min"""

    def test_get_mcp_historical_5_min_date_range(self):
        start = pd.Timestamp("2025-02-01")

        with file_vcr.use_cassette(
            f"test_get_mcp_historical_5_min_date_range_{start.date()}.yaml",
        ):
            df = self.iso.get_mcp_historical_5_min(start)

        self._check_mcp(df)

        # Historical data starts at the beginning of the year and runs through
        # the end of the previous day
        assert df["Interval Start"].min() == self.local_start_of_day("2025-01-01")
        assert df["Interval End"].max() == pd.Timestamp(
            "2025-04-30 23:10:00",
            tz=self.default_timezone,
        )

    """get_hoep_real_time_hourly"""

    @pytest.mark.parametrize(
        "start, end",
        [
            (
                pd.Timestamp("2025-04-01 00:00:00"),
                pd.Timestamp("2025-04-01 04:00:00"),
            ),
        ],
    )
    def test_get_hoep_real_time_hourly_date_range(self, start, end):
        start = start.tz_localize(self.default_timezone)
        end = end.tz_localize(self.default_timezone)
        with file_vcr.use_cassette(
            f"test_get_hoep_real_time_hourly_date_range_{start.date()}_{end.date()}.yaml",
        ):
            df = self.iso.get_hoep_real_time_hourly(start, end)

        assert df.columns.tolist() == ["Interval Start", "Interval End", "HOEP"]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()
        assert df["Interval Start"].min() == start
        assert df["Interval End"].max() == self.local_start_of_day(
            end.tz_localize(None) + pd.DateOffset(days=1),
        )

    """get_hoep_historical_hourly"""

    def test_get_hoep_historical_hourly_date_range(self):
        start = pd.Timestamp("2024-02-01")

        with file_vcr.use_cassette(
            f"test_get_hoep_historical_hourly_date_range_{start.date()}.yaml",
        ):
            df = self.iso.get_hoep_historical_hourly(start)

        # NOTE: different columns from real-time
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "HOEP",
            "Hour 1 Predispatch",
            "Hour 2 Predispatch",
            "Hour 3 Predispatch",
            "OR 10 Min Sync",
            "OR 10 Min non-sync",
            "OR 30 Min",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()
        assert df["Interval Start"].min() == self.local_start_of_day("2024-01-01")
        assert df["Interval End"].max() == self.local_start_of_day("2025-01-01")

    """get_resource_adequacy_report"""

    # NOTE(kladar): we will see how future data rolls in and historical rolls off
    # NOTE(kladar, 2024-12-11): Tests rolled off, earliest is currently 2024-09-10, 92 days ago
    RESOURCE_ADEQUACY_TEST_DATES = [
        (
            (pd.Timestamp.now(tz=default_timezone) - pd.Timedelta(days=3)).strftime(
                "%Y-%m-%d",
            ),
            (pd.Timestamp.now(tz=default_timezone) - pd.Timedelta(days=1)).strftime(
                "%Y-%m-%d",
            ),
        ),
        (
            (pd.Timestamp.now(tz=default_timezone)).strftime("%Y-%m-%d"),
            (pd.Timestamp.now(tz=default_timezone) + pd.Timedelta(days=2)).strftime(
                "%Y-%m-%d",
            ),
        ),
        (
            (pd.Timestamp.now(tz=default_timezone) + pd.Timedelta(days=1)).strftime(
                "%Y-%m-%d",
            ),
            (pd.Timestamp.now(tz=default_timezone) + pd.Timedelta(days=3)).strftime(
                "%Y-%m-%d",
            ),
        ),
    ]

    REQUIRED_RESOURCE_ADEQUACY_COLUMNS = [
        "Interval Start",
        "Interval End",
        "Publish Time",
        "Forecast Supply Capacity",
        "Forecast Supply Energy MWh",
        "Forecast Supply Bottled Capacity",
        "Forecast Supply Regulation",
        "Total Forecast Supply",
        "Total Requirement",
        "Capacity Excess Shortfall",
        "Energy Excess Shortfall MWh",
        "Offered Capacity Excess Shortfall",
        "Resources Not Scheduled",
        "Imports Not Scheduled",
        "Nuclear Capacity",
        "Nuclear Outages",
        "Nuclear Offered",
        "Nuclear Scheduled",
        "Gas Capacity",
        "Gas Outages",
        "Gas Offered",
        "Gas Scheduled",
        "Hydro Capacity",
        "Hydro Outages",
        "Hydro Forecasted MWh",
        "Hydro Offered",
        "Hydro Scheduled",
        "Wind Capacity",
        "Wind Outages",
        "Wind Forecasted",
        "Wind Scheduled",
        "Solar Capacity",
        "Solar Outages",
        "Solar Forecasted",
        "Solar Scheduled",
        "Biofuel Capacity",
        "Biofuel Outages",
        "Biofuel Offered",
        "Biofuel Scheduled",
        "Other Capacity",
        "Other Outages",
        "Other Offered Forecasted",
        "Other Scheduled",
        "Manitoba Imports Offered",
        "Manitoba Imports Scheduled",
        "Minnesota Imports Offered",
        "Minnesota Imports Scheduled",
        "Michigan Imports Offered",
        "Michigan Imports Scheduled",
        "New York Imports Offered",
        "New York Imports Scheduled",
        "Quebec Imports Offered",
        "Quebec Imports Scheduled",
        "Total Internal Resources Outages",
        "Total Internal Resources Offered Forecasted",
        "Total Internal Resources Scheduled",
        "Total Imports Offers",
        "Total Imports Scheduled",
        "Total Imports Estimated",
        "Total Imports Capacity",
        "Manitoba Exports Offered",
        "Manitoba Exports Scheduled",
        "Minnesota Exports Offered",
        "Minnesota Exports Scheduled",
        "Michigan Exports Offered",
        "Michigan Exports Scheduled",
        "New York Exports Offered",
        "New York Exports Scheduled",
        "Quebec Exports Offered",
        "Quebec Exports Scheduled",
        "Total Exports Bids",
        "Total Exports Scheduled",
        "Total Exports Capacity",
        "Total Operating Reserve",
        "Minimum 10 Minute Operating Reserve",
        "Minimum 10 Minute Spin OR",
        "Load Forecast Uncertainties",
        "Additional Contingency Allowances",
        "Ontario Demand Forecast",
        "Ontario Peak Demand",
        "Ontario Northeast Peak Demand",
        "Ontario Southwest Peak Demand",
        "Ontario Northwest Peak Demand",
        "Ontario Southeast Peak Demand",
        "Ontario Average Demand",
        "Ontario Northeast Average Demand",
        "Ontario Southwest Average Demand",
        "Ontario Northwest Average Demand",
        "Ontario Southeast Average Demand",
        "Ontario Wind Embedded Forecast",
        "Ontario Solar Embedded Forecast",
        "Ontario Dispatchable Load Capacity",
        "Ontario Dispatchable Load Bid Forecasted",
        "Ontario Dispatchable Load Scheduled ON",
        "Ontario Dispatchable Load Scheduled OFF",
        "Ontario Hourly Demand Response Bid Forecasted",
        "Ontario Hourly Demand Response Scheduled",
        "Ontario Hourly Demand Response Curtailed",
        "Last Modified",
    ]

    @pytest.mark.parametrize(
        "date",
        [date[0] for date in RESOURCE_ADEQUACY_TEST_DATES],
    )
    def test_get_resource_adequacy_report_single_date_latest_report(
        self,
        date: str | datetime.date,
    ):
        with file_vcr.use_cassette(f"test_get_resource_adequacy_report_{date}.yaml"):
            df = self.iso.get_resource_adequacy_report(date, vintage="latest")

        assert isinstance(df, pd.DataFrame)
        assert df.shape == (24, 99)  # 24 rows and 99 columns for each file
        for col in self.REQUIRED_RESOURCE_ADEQUACY_COLUMNS:
            assert col in df.columns

        assert self._check_is_datetime_type(df["Interval Start"])
        assert self._check_is_datetime_type(df["Interval End"])
        assert self._check_is_datetime_type(df["Publish Time"])
        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()

    @pytest.mark.parametrize(
        "date, end",
        RESOURCE_ADEQUACY_TEST_DATES,
    )
    def test_get_latest_resource_adequacy_report_date_range(self, date: str, end: str):
        with file_vcr.use_cassette(
            f"test_get_latest_resource_adequacy_report_{date}_to_{end}.yaml",
        ):
            df = self.iso.get_resource_adequacy_report(date, end=end, vintage="latest")

        assert isinstance(df, pd.DataFrame)
        assert df.shape[1] == 99
        for col in self.REQUIRED_RESOURCE_ADEQUACY_COLUMNS:
            assert col in df.columns
        expected_rows = ((pd.Timestamp(end) - pd.Timestamp(date)).days) * 24
        assert df.shape[0] == expected_rows

    @pytest.mark.parametrize(
        "date, end",
        RESOURCE_ADEQUACY_TEST_DATES,
    )
    def test_get_all_resource_adequacy_report_date_range(self, date: str, end: str):
        with file_vcr.use_cassette(
            f"test_get_all_resource_adequacy_report_{date}_to_{end}.yaml",
        ):
            df = self.iso.get_resource_adequacy_report(date, end=end, vintage="all")

        assert isinstance(df, pd.DataFrame)
        assert df.shape[1] == 99
        for col in self.REQUIRED_RESOURCE_ADEQUACY_COLUMNS:
            assert col in df.columns

    # TODO(kladar): eventually don't record this each time
    @file_vcr.use_cassette(
        "test_get_latest_resource_adequacy_json.yaml",
        record_mode="ALL",
    )
    def test_get_latest_resource_adequacy_json(self):
        date = pd.Timestamp.now(tz=self.default_timezone)
        json_data, last_modified = self.iso._get_latest_resource_adequacy_json(date)

        assert isinstance(json_data, dict)
        assert isinstance(last_modified, pd.Timestamp)
        assert "Document" in json_data
        assert "DocHeader" in json_data["Document"]
        assert "DocBody" in json_data["Document"]

        doc_body = json_data["Document"]["DocBody"]
        assert "ForecastSupply" in doc_body
        assert "ForecastDemand" in doc_body
        assert "DeliveryDate" in doc_body

    @file_vcr.use_cassette("test_get_all_resource_adequacy_json.yaml")
    def test_get_all_resource_adequacy_json(self):
        date = pd.Timestamp.now(tz=self.default_timezone)
        json_data_with_times = self.iso._get_all_resource_adequacy_jsons(date)

        assert isinstance(json_data_with_times, list)
        for json_data, last_modified in json_data_with_times:
            assert isinstance(json_data, dict)
            assert isinstance(last_modified, pd.Timestamp)
            assert "Document" in json_data
            assert "DocHeader" in json_data["Document"]
            assert "DocBody" in json_data["Document"]

            doc_body = json_data["Document"]["DocBody"]
            assert "ForecastSupply" in doc_body
            assert "ForecastDemand" in doc_body
            assert "DeliveryDate" in doc_body

    def test_get_resource_adequacy_data_structure_map(self):
        data_map = RESOURCE_ADEQUACY_REPORT_DATA_STRUCTURE_MAP

        assert isinstance(data_map, dict)
        assert "supply" in data_map
        assert "demand" in data_map

        supply = data_map["supply"]
        assert "hourly" in supply
        assert "fuel_type_hourly" in supply
        assert "total_internal_resources" in supply
        assert "zonal_import_hourly" in supply

        demand = data_map["demand"]
        assert "ontario_demand" in demand
        assert "zonal_export_hourly" in demand
        assert "total_exports" in demand
        assert "reserves" in demand

    def test_extract_hourly_values(self):
        test_data = {
            "Capacities": {
                "Capacity": [
                    {"DeliveryHour": "1", "EnergyMW": "100"},
                    {"DeliveryHour": "2", "EnergyMW": "200"},
                    {"DeliveryHour": "3", "EnergyMW": "300"},
                    {"DeliveryHour": "4", "EnergyMW": "400"},
                    {"DeliveryHour": "5", "EnergyMW": "500"},
                    {"DeliveryHour": "6", "EnergyMW": "600"},
                    {"DeliveryHour": "7", "EnergyMW": "700"},
                    {"DeliveryHour": "8", "EnergyMW": "800"},
                    {"DeliveryHour": "9", "EnergyMW": "900"},
                    {"DeliveryHour": "10", "EnergyMW": "1000"},
                    {"DeliveryHour": "11", "EnergyMW": "1100"},
                    {"DeliveryHour": "12", "EnergyMW": "1200"},
                    {"DeliveryHour": "13", "EnergyMW": "1300"},
                    {"DeliveryHour": "14", "EnergyMW": "1400"},
                    {"DeliveryHour": "15", "EnergyMW": "1500"},
                    {"DeliveryHour": "16", "EnergyMW": "1600"},
                    {"DeliveryHour": "17", "EnergyMW": "1700"},
                    {"DeliveryHour": "18", "EnergyMW": "1800"},
                    {"DeliveryHour": "19", "EnergyMW": "1900"},
                    {"DeliveryHour": "20", "EnergyMW": "2000"},
                    {"DeliveryHour": "21", "EnergyMW": "2100"},
                    {"DeliveryHour": "22", "EnergyMW": "2200"},
                    {"DeliveryHour": "23", "EnergyMW": "2300"},
                    {"DeliveryHour": "24", "EnergyMW": "2400"},
                ],
            },
        }

        report_data = []
        self.iso._extract_hourly_values(
            data=test_data,
            path=["Capacities", "Capacity"],
            column_name="Test Capacity",
            value_key="EnergyMW",
            report_data=report_data,
        )

        assert len(report_data) == 24
        assert report_data[0]["DeliveryHour"] == 1
        assert report_data[0]["Test Capacity"] == 100.0
        assert report_data[1]["DeliveryHour"] == 2
        assert report_data[1]["Test Capacity"] == 200.0

    """get_forecast_surplus_baseload"""

    def test_get_forecast_surplus_baseload_generation_single_date(self):
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        yesterday = today - pd.Timedelta(days=1)
        with file_vcr.use_cassette(
            f"test_get_forecast_surplus_baseload_generation_{yesterday.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_forecast_surplus_baseload_generation(yesterday)

        assert isinstance(df, pd.DataFrame)
        self._check_forecast_surplus_baseload(df)

        assert df["Interval Start"].min().date() == today.date()
        assert df["Interval End"].max().date() == today.date() + pd.Timedelta(days=10)

    def test_get_forecast_surplus_baseload_generation_date_range(self):
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        start = today - pd.Timedelta(days=3)
        end = today
        with file_vcr.use_cassette(
            f"test_get_forecast_surplus_baseload_generation_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_forecast_surplus_baseload_generation(start, end=end)

        assert isinstance(df, pd.DataFrame)
        self._check_forecast_surplus_baseload(df)

        assert df["Interval Start"].min().date() == start.date() + pd.Timedelta(days=1)
        assert df["Interval End"].max().date() == end.date() + pd.Timedelta(days=10)

    def test_get_forecast_surplus_baseload_generation_latest(self):
        with file_vcr.use_cassette(
            "test_get_forecast_surplus_baseload_generation_latest.yaml",
        ):
            df = self.iso.get_forecast_surplus_baseload_generation("latest")

        assert isinstance(df, pd.DataFrame)
        self._check_forecast_surplus_baseload(df)

    def _check_forecast_surplus_baseload(self, df: pd.DataFrame) -> None:
        required_columns = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Surplus Baseload MW",
            "Surplus State",
            "Action",
            "Export Forecast MW",
            "Minimum Generation Status",
        ]
        assert all(col in df.columns for col in required_columns)

        assert self._check_is_datetime_type(df["Interval Start"])
        assert self._check_is_datetime_type(df["Interval End"])
        assert self._check_is_datetime_type(df["Publish Time"])

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

        assert (
            df["Surplus State"]
            .isin(
                [
                    "No Surplus",
                    "Managed with Exports",
                    "Nuclear Dispatch",
                    "Nuclear Shutdown",
                ],
            )
            .all()
        )

        assert df["Action"].isin(["Other", "Manoeuvre", "Shutdown", None]).all()

        assert is_numeric_dtype(df["Surplus Baseload MW"])
        assert is_numeric_dtype(df["Export Forecast MW"])

        publish_days = df["Publish Time"].nunique()
        assert publish_days == len(
            pd.date_range(
                df["Publish Time"].min().date(),
                df["Publish Time"].max().date(),
                freq="D",
            ),
        )
        assert df["Publish Time"].iloc[0].date() == df[
            "Interval Start"
        ].min().date() - pd.Timedelta(days=1)
        assert len(df) == 24 * 10 * publish_days
        assert len(df.columns) == 8

    """get_yearly_intertie_actual_schedule_flow_hourly"""

    @pytest.mark.parametrize("date", ["2024-01-01"])
    def test_get_yearly_intertie_actual_schedule_flow_hourly_single_date(self, date):
        with file_vcr.use_cassette(
            f"test_get_yearly_intertie_actual_schedule_flow_hourly_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_yearly_intertie_actual_schedule_flow_hourly(date)

        self._check_intertie_schedule_flow(df)
        assert df["Interval Start"].min().date() == pd.Timestamp(date).date()
        assert df["Interval Start"].max().date() == pd.Timestamp(date).date()
        assert len(df) == 24

    @pytest.mark.parametrize("date, end", [("2023-01-01", "2023-01-03")])
    def test_get_yearly_intertie_actual_schedule_flow_hourly_date_range(
        self,
        date,
        end,
    ):
        with file_vcr.use_cassette(
            f"test_get_yearly_intertie_actual_schedule_flow_hourly_{pd.Timestamp(date).strftime('%Y-%m-%d')}_{pd.Timestamp(end).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_yearly_intertie_actual_schedule_flow_hourly(
                date,
                end=end,
                vintage="latest",
            )

        self._check_intertie_schedule_flow(df)
        assert df["Interval Start"].min().date() == pd.Timestamp(date).date()
        assert df["Interval Start"].max().date() == pd.Timestamp(end).date()
        assert (
            len(df)
            == 24 * (pd.Timestamp(end).date() - pd.Timestamp(date).date()).days + 1
        )

    def test_get_yearly_intertie_actual_schedule_flow_hourly_latest(self):
        with file_vcr.use_cassette(
            "test_get_yearly_intertie_actual_schedule_flow_hourly_latest.yaml",
        ):
            df = self.iso.get_yearly_intertie_actual_schedule_flow_hourly("latest")

        self._check_intertie_schedule_flow(df)
        current_year = pd.Timestamp.now(tz=self.default_timezone).year
        assert df["Interval Start"].min().year == current_year
        assert df["Interval Start"].max().year == current_year

    @pytest.mark.parametrize("date", ["2024-01-01"])
    def test_get_yearly_intertie_actual_schedule_flow_hourly_all_vintage(self, date):
        with file_vcr.use_cassette(
            f"test_get_yearly_intertie_actual_schedule_flow_hourly_all_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_yearly_intertie_actual_schedule_flow_hourly(
                date,
                vintage="all",
            )
        self._check_intertie_schedule_flow(df)

    @pytest.mark.parametrize(
        "date, end",
        [("2023-01-01", "2024-01-02"), ("2024-01-01", "2025-01-02")],
    )
    def test_get_yearly_intertie_actual_schedule_flow_hourly_cross_year(
        self,
        date,
        end,
    ):
        with file_vcr.use_cassette(
            f"test_get_yearly_intertie_actual_schedule_flow_hourly_cross_year_{pd.Timestamp(date).strftime('%Y-%m-%d')}_{pd.Timestamp(end).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_yearly_intertie_actual_schedule_flow_hourly(date, end=end)

        self._check_intertie_schedule_flow(df)
        assert df["Interval Start"].min().date() == pd.Timestamp(date).date()
        assert df["Interval Start"].max().date() == pd.Timestamp(end).date()
        assert (
            len(df)
            == 24 * (pd.Timestamp(end).date() - pd.Timestamp(date).date()).days + 1
        )

    def _check_intertie_schedule_flow(self, df):
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert self._check_is_datetime_type(df[TIME_COLUMN])
        assert self._check_is_datetime_type(df["Interval End"])
        assert self._check_is_datetime_type(df["Publish Time"])
        assert (df["Interval End"] - df[TIME_COLUMN] == pd.Timedelta(hours=1)).all()

        zone_prefixes = ["Manitoba", "Michigan", "Minnesota", "New York"]
        flow_types = ["Flow", "Import", "Export"]

        for zone in zone_prefixes:
            for flow_type in flow_types:
                col_name = f"{zone} {flow_type}"
                assert col_name in df.columns
                assert is_numeric_dtype(df[col_name])

        pq_columns = [col for col in df.columns if col.startswith("PQ")]
        assert len(pq_columns) > 0
        assert df[TIME_COLUMN].equals(df[TIME_COLUMN].sort_values())

    """get_intertie_actual_schedule_flow_hourly"""

    def _check_intertie_schedule_flow_hourly(self, df):
        assert list(df.columns) == INTERTIE_ACTUAL_SCHEDULE_FLOW_HOURLY_COLUMNS

        time_columns = [TIME_COLUMN, "Interval End", "Publish Time"]

        for col in time_columns:
            assert self._check_is_datetime_type(df[col])

        assert df[TIME_COLUMN].is_monotonic_increasing

        assert df[
            [col for col in df.columns if col not in time_columns]
        ].dtypes.unique() == np.dtype("float64")

        assert (df["Interval End"] - df[TIME_COLUMN] == pd.Timedelta(hours=1)).all()

    def test_get_intertie_actual_schedule_flow_hourly_latest(self):
        with file_vcr.use_cassette(
            "test_get_intertie_actual_schedule_flow_hourly_latest.yaml",
        ):
            df = self.iso.get_intertie_actual_schedule_flow_hourly("latest")

        self._check_intertie_schedule_flow_hourly(df)

        # Check that the data is for today
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        assert (df[TIME_COLUMN].dt.date == today.date()).all()

    def test_get_intertie_actual_schedule_flow_hourly_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.DateOffset(days=2)

        with file_vcr.use_cassette(
            f"test_get_intertie_actual_schedule_flow_hourly_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            df = self.iso.get_intertie_actual_schedule_flow_hourly(start, end=end)

        self._check_intertie_schedule_flow_hourly(df)

        # Check that the data is for the specified date range
        assert df[TIME_COLUMN].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(hours=1)

    """get_intertie_flow_5_min"""

    def _check_intertie_flow_5_min(self, df):
        assert list(df.columns) == INTERTIE_FLOW_5_MIN_COLUMNS

        time_columns = [TIME_COLUMN, "Interval End", "Publish Time"]

        for col in time_columns:
            assert self._check_is_datetime_type(df[col])

        assert df[TIME_COLUMN].is_monotonic_increasing

        assert (df["Interval End"] - df[TIME_COLUMN] == pd.Timedelta(minutes=5)).all()

        # Make sure all columns except for the time columns are numeric
        assert df[
            [col for col in df.columns if col not in time_columns]
        ].dtypes.unique() == np.dtype("float64")

    def test_get_intertie_flow_5_min_latest(self):
        with file_vcr.use_cassette(
            "test_get_intertie_flow_5_min_latest.yaml",
        ):
            df = self.iso.get_intertie_flow_5_min("latest")

        self._check_intertie_flow_5_min(df)

        # Check that the data is for today
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        assert (df[TIME_COLUMN].dt.date == today.date()).all()

    def test_get_intertie_flow_5_min_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.DateOffset(days=2)

        with file_vcr.use_cassette(
            f"test_get_intertie_flow_5_min_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            df = self.iso.get_intertie_flow_5_min(start, end=end)

        self._check_intertie_flow_5_min(df)

        # Check that the data is for the specified date range
        assert df[TIME_COLUMN].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(minutes=5)

    """get_lmp_real_time_5_min"""

    def _check_lmp_data(
        self,
        data: pd.DataFrame,
        interval_minutes: int,
        predispatch: bool = False,
    ) -> None:
        column_list = [
            "Interval Start",
            "Interval End",
            "Location",
            "LMP",
            "Energy",
            "Congestion",
            "Loss",
        ]

        if predispatch:
            column_list.insert(column_list.index("Interval End") + 1, "Publish Time")

        assert data.columns.tolist() == column_list

        time_type = "interval"
        self._check_time_columns(data, instant_or_interval=time_type)

        assert np.allclose(
            data["LMP"],
            data["Energy"] + data["Loss"] + data["Congestion"],
        )

        assert (
            data["Interval End"] - data["Interval Start"]
            == pd.Timedelta(minutes=interval_minutes)
        ).all()

        assert data[TIME_COLUMN].is_monotonic_increasing

        # Make sure none of the locations have :LMP or :HUB in them
        assert not data["Location"].str.contains(":LMP").any()
        assert not data["Location"].str.contains(":HUB").any()

    def test_get_lmp_real_time_5_min_latest(self):
        with file_vcr.use_cassette("test_get_lmp_real_time_5_min_latest.yaml"):
            data = self.iso.get_lmp_real_time_5_min("latest")

        self._check_lmp_data(data, interval_minutes=5)

        # Check that the data is for today
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        assert (data[TIME_COLUMN].dt.date == today.date()).all()

    def test_get_lmp_real_time_5_min_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.Timedelta(hours=2)

        with file_vcr.use_cassette(
            f"test_get_lmp_real_time_5_min_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_lmp_real_time_5_min(start, end=end)

        self._check_lmp_data(data, interval_minutes=5)

        # Check that the data is for the specified date range
        assert data[TIME_COLUMN].min() == start
        assert data[TIME_COLUMN].max() == end - pd.Timedelta(minutes=5)

    """get_lmp_day_ahead_hourly"""

    def test_get_lmp_day_ahead_hourly_latest(self):
        with file_vcr.use_cassette("test_get_lmp_day_ahead_hourly_latest.yaml"):
            data = self.iso.get_lmp_day_ahead_hourly("latest")

        self._check_lmp_data(data, interval_minutes=60)

        # Check that the data is all for one day. We can't check for a specific date
        # because, based on the time of day, the latest file will have data for
        # today or tomorrow.
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        tomorrow = today + pd.Timedelta(days=1)
        assert ((data[TIME_COLUMN].dt.date == today.date()).all()) or (
            (data[TIME_COLUMN].dt.date == tomorrow.date()).all()
        )

    def test_get_lmp_day_ahead_hourly_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.DateOffset(days=2)

        with file_vcr.use_cassette(
            f"test_get_lmp_day_ahead_hourly_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_lmp_day_ahead_hourly(start, end=end)

        self._check_lmp_data(data, interval_minutes=60)

        # Check that the data is for the specified date range
        assert data[TIME_COLUMN].min() == start
        assert data[TIME_COLUMN].max() == end - pd.Timedelta(minutes=60)

    """get_lmp_predispatch_hourly"""

    def test_get_lmp_predispatch_hourly_latest(self):
        with file_vcr.use_cassette("test_get_lmp_dispatch_hourly_latest.yaml"):
            data = self.iso.get_lmp_predispatch_hourly("latest")

        self._check_lmp_data(data, interval_minutes=60, predispatch=True)

        # Check that the data is for today
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        assert (data[TIME_COLUMN].dt.date == today.date()).all()

        assert not data.duplicated(
            subset=["Interval Start", "Publish Time", "Location"],
        ).any()

    def test_get_lmp_predispatch_hourly_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.DateOffset(days=1)

        with file_vcr.use_cassette(
            f"test_get_lmp_predispatch_hourly_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_lmp_predispatch_hourly(start, end=end)

        self._check_lmp_data(data, interval_minutes=60, predispatch=True)

        assert not data.duplicated(
            subset=["Interval Start", "Publish Time", "Location"],
        ).any()

        # Since we retrieve data by publish time, the data will go out 23 hours
        # after the end.
        assert data[TIME_COLUMN].min() == start
        assert data[TIME_COLUMN].max() == end + pd.Timedelta(hours=23)

        assert data["Publish Time"].min() > start
        assert data["Publish Time"].max() < end

    """get_lmp_real_time_5_min_virtual_zonal"""

    def _check_lmp_virtual_zonal_data(
        self,
        data: pd.DataFrame,
        interval_minutes: int,
        predispatch: bool = False,
    ) -> None:
        column_list = [
            "Interval Start",
            "Interval End",
            "Location",
            "LMP",
            "Energy",
            "Congestion",
            "Loss",
        ]

        if predispatch:
            column_list.insert(column_list.index("Interval End") + 1, "Publish Time")

        assert data.columns.tolist() == column_list

        time_type = "interval"
        self._check_time_columns(data, instant_or_interval=time_type)

        assert np.allclose(
            data["LMP"],
            data["Energy"] + data["Loss"] + data["Congestion"],
        )

        assert (
            data["Interval End"] - data["Interval Start"]
            == pd.Timedelta(minutes=interval_minutes)
        ).all()

        assert data[TIME_COLUMN].is_monotonic_increasing

        assert list(data["Location"].unique()) == [
            "EAST",
            "ESSA",
            "NIAGARA",
            "NORTHEAST",
            "NORTHWEST",
            "OTTAWA",
            "SOUTHWEST",
            "TORONTO",
            "WEST",
        ]

    def test_get_lmp_real_time_5_min_virtual_zonal_latest(self):
        with file_vcr.use_cassette(
            "test_get_lmp_real_time_5_min_virtual_zonal_latest.yaml",
        ):
            data = self.iso.get_lmp_real_time_5_min_virtual_zonal("latest")

        self._check_lmp_virtual_zonal_data(data, interval_minutes=5)

        # Check that the data is for tomorrow
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        assert (data[TIME_COLUMN].dt.date == today.date()).all()

    def test_get_lmp_real_time_5_min_virtual_zonal_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.Timedelta(hours=2)

        with file_vcr.use_cassette(
            f"test_get_lmp_real_time_5_min_virtual_zonal_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_lmp_real_time_5_min_virtual_zonal(start, end=end)

        self._check_lmp_virtual_zonal_data(data, interval_minutes=5)

        # Check that the data is for the specified date range
        assert data[TIME_COLUMN].min() == start
        assert data[TIME_COLUMN].max() == end - pd.Timedelta(minutes=5)

    """get_lmp_day_ahead_hourly_virtual_zonal"""

    def test_get_lmp_day_ahead_hourly_virtual_zonal_latest(self):
        with file_vcr.use_cassette(
            "test_get_lmp_day_ahead_hourly_virtual_zonal_latest.yaml",
        ):
            data = self.iso.get_lmp_day_ahead_hourly_virtual_zonal("latest")

        self._check_lmp_virtual_zonal_data(data, interval_minutes=60)

        # Check that the data is all for one day. We can't check for a specific date
        # because, based on the time of day, the latest file will have data for
        # today or tomorrow.
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        tomorrow = today + pd.Timedelta(days=1)
        assert ((data[TIME_COLUMN].dt.date == today.date()).all()) or (
            (data[TIME_COLUMN].dt.date == tomorrow.date()).all()
        )

    def test_get_lmp_day_ahead_hourly_virtual_zonal_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.DateOffset(days=1)

        with file_vcr.use_cassette(
            f"test_get_lmp_day_ahead_hourly_virtual_zonal_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_lmp_day_ahead_hourly_virtual_zonal(start, end=end)

        self._check_lmp_virtual_zonal_data(data, interval_minutes=60)

        # Check that the data is for the specified date range
        assert data[TIME_COLUMN].min() == start
        assert data[TIME_COLUMN].max() == end - pd.Timedelta(minutes=60)

    """get_lmp_predispatch_hourly_virtual_zonal"""

    def test_get_lmp_predispatch_hourly_virtual_zonal_latest(self):
        with file_vcr.use_cassette(
            "test_get_lmp_dispatch_hourly_virtual_zonal_latest.yaml",
        ):
            data = self.iso.get_lmp_predispatch_hourly_virtual_zonal("latest")

        self._check_lmp_virtual_zonal_data(data, interval_minutes=60, predispatch=True)

        assert not data.duplicated(
            subset=["Interval Start", "Publish Time", "Location"],
        ).any()

        # Check that the data is for today
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        assert (data[TIME_COLUMN].dt.date == today.date()).all()

    def test_get_lmp_predispatch_hourly_virtual_zonal_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.DateOffset(days=1)

        with file_vcr.use_cassette(
            f"test_get_lmp_predispatch_hourly_virtual_zonal_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_lmp_predispatch_hourly_virtual_zonal(start, end=end)

        self._check_lmp_virtual_zonal_data(data, interval_minutes=60, predispatch=True)

        assert not data.duplicated(
            subset=["Interval Start", "Publish Time", "Location"],
        ).any()

        assert data[TIME_COLUMN].min() == start
        # Since we retrieve data by publish time, the actual data will extend
        # beyond the end time by 23 hours
        assert data[TIME_COLUMN].max() == end + pd.Timedelta(hours=23)

        assert data["Publish Time"].min() > start
        assert data["Publish Time"].max() < end

    """get_lmp_real_time_5_min_intertie"""

    def _check_lmp_intertie(
        self,
        data: pd.DataFrame,
        interval_minutes: int,
        predispatch: bool = False,
    ) -> None:
        column_list = [
            "Interval Start",
            "Interval End",
            "Location",
            "LMP",
            "Energy",
            "Congestion",
            "Loss",
            "External Congestion",
            "Interchange Scheduling Limit Price",
        ]

        if predispatch:
            # Add Publish Time after the Interval End column
            column_list.insert(
                column_list.index("Interval End") + 1,
                "Publish Time",
            )

        assert data.columns.tolist() == column_list

        time_type = "interval"
        self._check_time_columns(data, instant_or_interval=time_type)

        assert np.allclose(
            data["LMP"],
            data["Energy"]
            + data["Loss"]
            + data["Congestion"]
            + data["External Congestion"]
            + data["Interchange Scheduling Limit Price"],
        )

        assert (
            data["Interval End"] - data["Interval Start"]
            == pd.Timedelta(minutes=interval_minutes)
        ).all()

        assert data[TIME_COLUMN].is_monotonic_increasing

        assert list(data["Location"].unique()) == [
            "EC.MARITIMES_NYSI",
            "MB.SEVENSISTERS_MBSK",
            "MB.WHITESHELL_MBSI",
            "MD.CALVERTCLIFF_MISI",
            "MD.CALVERTCLIFF_NYSI",
            "MI.LUDINGTON_MISI",
            "MN.INTFALLS_MNSI",
            "NY.ROSETON_NYSI",
            "PQ.BEAUHARNOIS_PQBE",
            "PQ.BRYSON_PQXY",
            "PQ.KIPAWA_PQHZ",
            "PQ.MACLAREN_PQDA",
            "PQ.MASSON_PQHA",
            "PQ.OUTAOUAIS_PQAT",
            "PQ.PAUGAN_PQPC",
            "PQ.QUYON_PQQC",
            "PQ.RAPIDDESISLE_PQDZ",
            "WC.PRAIRERANGES_MISI",
        ]

    def test_get_lmp_real_time_5_min_intertie_latest(self):
        with file_vcr.use_cassette(
            "test_get_lmp_real_time_5_min_intertie_latest.yaml",
        ):
            data = self.iso.get_lmp_real_time_5_min_intertie("latest")

        self._check_lmp_intertie(data, interval_minutes=5)

        # Check that the data is for today
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        assert (data[TIME_COLUMN].dt.date == today.date()).all()

    def test_get_lmp_real_time_5_min_intertie_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.Timedelta(hours=2)

        with file_vcr.use_cassette(
            f"test_get_lmp_real_time_5_min_intertie_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_lmp_real_time_5_min_intertie(start, end=end)

        self._check_lmp_intertie(data, interval_minutes=5)

        # Check that the data is for the specified date range
        assert data[TIME_COLUMN].min() == start
        assert data[TIME_COLUMN].max() == end - pd.Timedelta(minutes=5)

    """get_lmp_day_ahead_hourly_intertie"""

    def test_get_lmp_day_ahead_hourly_intertie_latest(self):
        with file_vcr.use_cassette(
            "test_get_lmp_day_ahead_hourly_intertie_latest.yaml",
        ):
            data = self.iso.get_lmp_day_ahead_hourly_intertie("latest")

        self._check_lmp_intertie(data, interval_minutes=60)

        # Check that the data is all for one day. We can't check for a specific date
        # because, based on the time of day, the latest file will have data for
        # today or tomorrow.
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        tomorrow = today + pd.Timedelta(days=1)
        print(data[TIME_COLUMN])
        assert ((data[TIME_COLUMN].dt.date == today.date()).all()) or (
            (data[TIME_COLUMN].dt.date == tomorrow.date()).all()
        )

    def test_get_lmp_day_ahead_hourly_intertie_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.DateOffset(days=1)

        with file_vcr.use_cassette(
            f"test_get_lmp_day_ahead_hourly_intertie_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_lmp_day_ahead_hourly_intertie(start, end=end)

        self._check_lmp_intertie(data, interval_minutes=60)

        # Check that the data is for the specified date range
        assert data[TIME_COLUMN].min() == start
        assert data[TIME_COLUMN].max() == end - pd.Timedelta(minutes=60)

    """get_lmp_predispatch_hourly_intertie"""

    def test_get_lmp_predispatch_hourly_intertie_latest(self):
        with file_vcr.use_cassette(
            "test_get_lmp_predispatch_hourly_intertie_latest.yaml",
        ):
            data = self.iso.get_lmp_predispatch_hourly_intertie("latest")

        self._check_lmp_intertie(data, interval_minutes=60, predispatch=True)

        assert not data.duplicated(
            subset=["Interval Start", "Publish Time", "Location"],
        ).any()

        # Check that the data is for today
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        assert (data[TIME_COLUMN].dt.date == today.date()).all()

    def test_get_lmp_predispatch_hourly_intertie_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.DateOffset(days=1)

        with file_vcr.use_cassette(
            f"test_get_lmp_predispatch_hourly_intertie_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_lmp_predispatch_hourly_intertie(start, end=end)

        self._check_lmp_intertie(data, interval_minutes=60, predispatch=True)

        assert not data.duplicated(
            subset=["Interval Start", "Publish Time", "Location"],
        ).any()

        assert data[TIME_COLUMN].min() == start
        # Since we retrieve data by publish time, the actual data will extend
        # beyond the end time by 23 hours
        assert data[TIME_COLUMN].max() == end + pd.Timedelta(hours=23)

        assert data["Publish Time"].min() > start
        assert data["Publish Time"].max() < end

    """get_lmp_real_time_5_min_ontario_zonal"""

    def _check_lmp_ontario_zonal_data(
        self,
        data: pd.DataFrame,
        interval_minutes: int,
        predispatch: bool = False,
    ) -> None:
        column_list = [
            "Interval Start",
            "Interval End",
            "Location",
            "LMP",
            "Energy",
            "Congestion",
            "Loss",
        ]
        if predispatch:
            column_list.insert(column_list.index("Interval End") + 1, "Publish Time")

        assert data.columns.tolist() == column_list

        time_type = "interval"
        self._check_time_columns(data, instant_or_interval=time_type)

        assert np.allclose(
            data["LMP"],
            data["Energy"] + data["Loss"] + data["Congestion"],
        )

        assert (
            data["Interval End"] - data["Interval Start"]
            == pd.Timedelta(minutes=interval_minutes)
        ).all()

        assert data[TIME_COLUMN].is_monotonic_increasing
        assert (data["Location"] == ONTARIO_LOCATION).all()

    def test_get_lmp_real_time_5_min_ontario_zonal_latest(self):
        with file_vcr.use_cassette(
            "test_get_lmp_real_time_5_min_ontario_zonal_latest.yaml",
        ):
            data = self.iso.get_lmp_real_time_5_min_ontario_zonal("latest")

        self._check_lmp_ontario_zonal_data(data, interval_minutes=5)

        # Check that the data is for today
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        assert (data[TIME_COLUMN].dt.date == today.date()).all()

    def test_get_lmp_real_time_5_min_ontario_zonal_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.Timedelta(hours=2)

        with file_vcr.use_cassette(
            f"test_get_lmp_real_time_5_min_ontario_zonal_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_lmp_real_time_5_min_ontario_zonal(start, end=end)

        self._check_lmp_ontario_zonal_data(data, interval_minutes=5)

        # Check that the data is for the specified date range
        assert data[TIME_COLUMN].min() == start
        assert data[TIME_COLUMN].max() == end - pd.Timedelta(minutes=5)

    """get_lmp_day_ahead_hourly_ontario_zonal"""

    def test_get_lmp_day_ahead_hourly_ontario_zonal_latest(self):
        with file_vcr.use_cassette(
            "test_get_lmp_day_ahead_hourly_ontario_zonal_latest.yaml",
        ):
            data = self.iso.get_lmp_day_ahead_hourly_ontario_zonal("latest")

        self._check_lmp_ontario_zonal_data(data, interval_minutes=60)

        # Check that the data is all for one day. We can't check for a specific date
        # because, based on the time of day, the latest file will have data for
        # today or tomorrow.
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        tomorrow = today + pd.Timedelta(days=1)
        assert ((data[TIME_COLUMN].dt.date == today.date()).all()) or (
            (data[TIME_COLUMN].dt.date == tomorrow.date()).all()
        )

    def test_get_lmp_day_ahead_hourly_ontario_zonal_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.DateOffset(days=1)

        with file_vcr.use_cassette(
            f"test_get_lmp_day_ahead_hourly_ontario_zonal_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_lmp_day_ahead_hourly_ontario_zonal(start, end=end)

        self._check_lmp_ontario_zonal_data(data, interval_minutes=60)

        # Check that the data is for the specified date range
        assert data[TIME_COLUMN].min() == start
        assert data[TIME_COLUMN].max() == end - pd.Timedelta(minutes=60)

    """get_lmp_predispatch_hourly_ontario_zonal"""

    def test_get_lmp_predispatch_hourly_ontario_zonal_latest(self):
        with file_vcr.use_cassette(
            "test_get_lmp_predispatch_hourly_ontario_zonal_latest.yaml",
        ):
            data = self.iso.get_lmp_predispatch_hourly_ontario_zonal("latest")

        self._check_lmp_ontario_zonal_data(data, interval_minutes=60, predispatch=True)

        assert not data.duplicated(
            subset=["Interval Start", "Publish Time", "Location"],
        ).any()

        # Check that the data is for today
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        assert (data[TIME_COLUMN].dt.date == today.date()).all()

    def test_get_lmp_predispatch_hourly_ontario_zonal_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.DateOffset(days=1)

        with file_vcr.use_cassette(
            f"test_get_lmp_predispatch_hourly_ontario_zonal_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_lmp_predispatch_hourly_ontario_zonal(start, end=end)

        self._check_lmp_ontario_zonal_data(data, interval_minutes=60, predispatch=True)

        assert not data.duplicated(
            subset=["Interval Start", "Publish Time", "Location"],
        ).any()

        assert data[TIME_COLUMN].min() == start
        # Since we retrieve data by publish time, the actual data will extend
        # beyond the end time by 23 hours
        assert data[TIME_COLUMN].max() == end + pd.Timedelta(hours=23)

        assert data["Publish Time"].min() > start
        assert data["Publish Time"].max() < end

    """get_transmission_outages_planned"""

    def _check_transmission_outages_planned(self, data: pd.DataFrame) -> None:
        assert isinstance(data, pd.DataFrame)
        assert data.shape[0] >= 0

        assert set(data.columns) == {
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Outage ID",
            "Name",
            "Priority",
            "Recurrence",
            "Type",
            "Voltage",
            "Constraint",
            "Recall Time",
            "Status",
        }

        assert self._check_is_datetime_type(data["Interval Start"])
        assert self._check_is_datetime_type(data["Interval End"])
        assert self._check_is_datetime_type(data["Publish Time"])

        assert data["Outage ID"].dtype == "object"
        assert data["Name"].dtype == "object"
        assert data["Priority"].dtype == "object"
        assert data["Recurrence"].dtype == "object"
        assert data["Type"].dtype == "object"
        assert data["Voltage"].dtype == "object"
        assert data["Constraint"].dtype == "object"
        assert data["Recall Time"].dtype == "object"
        assert data["Status"].dtype == "object"

    def test_get_transmission_outages_planned_latest(self):
        with file_vcr.use_cassette("transmission_outages_planned_latest.yaml"):
            data = self.iso.get_transmission_outages_planned("latest")

        self._check_transmission_outages_planned(data)

        assert not (
            data.duplicated(
                subset=[c for c in data.columns if c != "Publish Time"],
            ).any()
        )

    def test_get_transmission_outages_planned_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone) - pd.Timedelta(days=3)
        end = start + pd.DateOffset(days=1)
        with file_vcr.use_cassette(
            f"transmission_outages_planned_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_transmission_outages_planned("today")

        self._check_transmission_outages_planned(data)

        assert not (
            data.duplicated(
                subset=[c for c in data.columns if c != "Publish Time"],
            ).any()
        )

    """get_in_service_transmission_limits"""

    def _check_transmission_limits(self, data: pd.DataFrame) -> None:
        assert set(data.columns) == {
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Issue Time",
            "Type",
            "Facility",
            "Operating Limit",
            "Comments",
        }

        assert self._check_is_datetime_type(data["Interval Start"])
        assert self._check_is_datetime_type(data["Interval End"])
        assert self._check_is_datetime_type(data["Publish Time"])
        assert self._check_is_datetime_type(data["Issue Time"])

        assert data["Facility"].dtype == "object"
        assert data["Type"].dtype == "object"
        assert data["Operating Limit"].dtype == "int64"
        assert data["Comments"].dtype == "object"

        assert not (
            data.duplicated(
                subset=[c for c in data.columns if c != "Publish Time"],
            ).any()
        )

    def test_get_in_service_transmission_limits_latest(self):
        with file_vcr.use_cassette(
            "in_service_transmission_limits_latest.yaml",
        ):
            data = self.iso.get_in_service_transmission_limits("latest")

        self._check_transmission_limits(data)

    def test_get_in_service_transmission_limits_historical_date(self):
        # Only date for which data is available
        start = pd.Timestamp("2025-04-17")

        with file_vcr.use_cassette(
            f"in_service_transmission_limits_historical_date_range_{start.date()}.yaml",
        ):
            data = self.iso.get_in_service_transmission_limits(start)

        self._check_transmission_limits(data)

    """get_outage_transmission_limits"""

    def test_get_outage_transmission_limits_latest(self):
        with file_vcr.use_cassette(
            "outage_transmission_limits_latest.yaml",
        ):
            data = self.iso.get_outage_transmission_limits("latest")

        self._check_transmission_limits(data)

    def test_get_outage_transmission_limits_historical_date(self):
        # Only date for which data is available
        start = pd.Timestamp("2025-04-17")

        with file_vcr.use_cassette(
            f"outage_transmission_limits_historical_date_range_{start.date()}.yaml",
        ):
            data = self.iso.get_outage_transmission_limits(start)

        self._check_transmission_limits(data)

    """get_load_daily_zonal_5_min"""

    def _check_load_zonal(self, data: pd.DataFrame, frequency_minutes: int) -> None:
        assert isinstance(data, pd.DataFrame)
        assert data.shape[0] >= 0
        assert set(data.columns) == set(ZONAL_LOAD_COLUMNS)
        assert (
            data["Interval End"] - data["Interval Start"]
            == pd.Timedelta(minutes=frequency_minutes)
        ).all()

        numeric_cols = [
            col for col in data.columns if col not in ["Interval Start", "Interval End"]
        ]
        for col in numeric_cols:
            assert is_numeric_dtype(data[col])

    def test_get_load_daily_zonal_5_min_latest(self):
        with file_vcr.use_cassette("test_get_load_zonal_5_min_latest.yaml"):
            data = self.iso.get_load_zonal_5_min("latest")
        self._check_load_zonal(data, 5)

    def test_get_load_zonal_5_min_historical_date_range(self):
        # NB: Data stopped updating here
        start = pd.Timestamp("2025-04-20", tz=self.default_timezone)
        end = pd.Timestamp("2025-04-22", tz=self.default_timezone)

        with file_vcr.use_cassette(
            f"test_get_load_zonal_5_min_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_load_zonal_5_min(start, end=end)

        self._check_load_zonal(data, 5)

        assert data["Interval Start"].min() == start
        assert data["Interval End"].max() == end

    """get_load_daily_zonal_hourly"""

    def test_get_load_zonal_hourly_latest(self):
        with file_vcr.use_cassette("test_get_load_zonal_hourly_latest.yaml"):
            data = self.iso.get_load_zonal_hourly("latest")
        self._check_load_zonal(data, 60)

    def test_get_load_zonal_hourly_historical_date_range(self):
        # NB: Data stopped updating here
        start = pd.Timestamp("2025-04-20", tz=self.default_timezone)
        end = pd.Timestamp("2025-04-22", tz=self.default_timezone)

        with file_vcr.use_cassette(
            f"test_get_load_zonal_hourly_historical_date_range_{start.date()}_{end.date()}.yaml",
        ):
            data = self.iso.get_load_zonal_hourly(start, end=end)

        self._check_load_zonal(data, 60)

        assert data["Interval Start"].min() == start
        assert data["Interval End"].max() == end

    """get_real_time_totals"""

    def _check_real_time_totals(self, data: pd.DataFrame) -> None:
        assert list(data.columns) == [
            "Interval Start",
            "Interval End",
            "Total Energy",
            "Total Loss",
            "Market Total Load",
            "Total Dispatchable Load Scheduled Off",
            "Total 10S",
            "Total 10N",
            "Total 30R",
            "Ontario Load",
            "Flag",
        ]

        assert self._check_is_datetime_type(data["Interval Start"])
        assert self._check_is_datetime_type(data["Interval End"])

        assert data["Total Energy"].dtype == "float64"
        assert data["Total Loss"].dtype == "float64"
        assert data["Market Total Load"].dtype == "float64"
        assert data["Total Dispatchable Load Scheduled Off"].dtype == "float64"
        assert data["Total 10S"].dtype == "float64"
        assert data["Total 10N"].dtype == "float64"
        assert data["Total 30R"].dtype == "float64"
        assert data["Ontario Load"].dtype == "float64"

        assert data["Flag"].dtype == "object"

        assert (
            data["Interval End"] - data["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()

        assert data["Interval Start"].is_monotonic_increasing

    def test_get_real_time_totals_latest(self):
        with file_vcr.use_cassette("test_get_real_time_totals_latest.yaml"):
            data = self.iso.get_real_time_totals("latest")

        self._check_real_time_totals(data)

        # Check that the data is for today
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        assert (data["Interval Start"].dt.date == today.date()).all()

    @pytest.mark.parametrize(
        "start_date, end_date",
        [
            ("2025-06-01T09:00:00Z", "2025-06-01T12:00:00Z"),
        ],
    )
    def test_get_real_time_totals_historical_date_range(self, start_date, end_date):
        with file_vcr.use_cassette(
            f"test_get_real_time_totals_historical_date_range_{start_date}_{end_date}.yaml",
        ):
            data = self.iso.get_real_time_totals(start_date, end=end_date)

        self._check_real_time_totals(data)

        assert data["Interval Start"].min() == pd.Timestamp(start_date)
        assert data["Interval End"].max() == pd.Timestamp(end_date)

    """get_variable_generation_forecast"""

    def _check_variable_generation_forecast(self, df: pd.DataFrame) -> None:
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert self._check_is_datetime_type(df["Interval Start"])
        assert self._check_is_datetime_type(df["Interval End"])
        assert self._check_is_datetime_type(df["Publish Time"])
        assert self._check_is_datetime_type(df["Last Modified"])

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

        expected_cols = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Last Modified",
            "Zone",
            "Generation Forecast",
        ]
        assert all(col in df.columns for col in expected_cols)
        assert is_numeric_dtype(df["Generation Forecast"])

    def test_get_solar_embedded_forecast_latest(self):
        with file_vcr.use_cassette("test_get_solar_embedded_forecast_latest.yaml"):
            df = self.iso.get_solar_embedded_forecast("latest")

        self._check_variable_generation_forecast(df)

    def test_get_solar_embedded_forecast_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.DateOffset(days=2)

        with file_vcr.use_cassette(
            f"test_get_solar_embedded_forecast_historical_{start.date()}_{end.date()}.yaml",
        ):
            df = self.iso.get_solar_embedded_forecast(start, end=end)

        self._check_variable_generation_forecast(df)

        assert df["Interval Start"].min() == start + pd.Timedelta(days=1)
        assert df["Interval End"].max() == end + pd.Timedelta(days=2)

    def test_get_solar_embedded_forecast_all_versions(self):
        date = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=2,
        )

        with file_vcr.use_cassette(
            f"test_get_solar_embedded_forecast_all_{date.date()}.yaml",
        ):
            df = self.iso.get_solar_embedded_forecast(date, vintage="all")

        self._check_variable_generation_forecast(df)

        assert df["Publish Time"].nunique() > 1

    def test_get_wind_embedded_forecast_latest(self):
        with file_vcr.use_cassette("test_get_wind_embedded_forecast_latest.yaml"):
            df = self.iso.get_wind_embedded_forecast("latest")

        self._check_variable_generation_forecast(df)

    def test_get_wind_embedded_forecast_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.DateOffset(days=2)

        with file_vcr.use_cassette(
            f"test_get_wind_embedded_forecast_historical_{start.date()}_{end.date()}.yaml",
        ):
            df = self.iso.get_wind_embedded_forecast(start, end=end)

        self._check_variable_generation_forecast(df)
        assert df["Interval Start"].min() == start + pd.Timedelta(days=1)
        assert df["Interval End"].max() == end + pd.Timedelta(days=2)

    def test_get_wind_market_participant_forecast_latest(self):
        with file_vcr.use_cassette(
            "test_get_wind_market_participant_forecast_latest.yaml",
        ):
            df = self.iso.get_wind_market_participant_forecast("latest")

        self._check_variable_generation_forecast(df)

    def test_get_wind_market_participant_forecast_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.DateOffset(days=2)

        with file_vcr.use_cassette(
            f"test_get_wind_market_participant_forecast_historical_{start.date()}_{end.date()}.yaml",
        ):
            df = self.iso.get_wind_market_participant_forecast(start, end=end)

        self._check_variable_generation_forecast(df)
        assert df["Interval Start"].min() == start + pd.Timedelta(days=1)
        assert df["Interval End"].max() == end + pd.Timedelta(days=2)

    def test_get_solar_market_participant_forecast_latest(self):
        with file_vcr.use_cassette(
            "test_get_solar_market_participant_forecast_latest.yaml",
        ):
            df = self.iso.get_solar_market_participant_forecast("latest")

        self._check_variable_generation_forecast(df)

    def test_get_solar_market_participant_forecast_historical_date_range(self):
        start = pd.Timestamp.now(tz=self.default_timezone).normalize() - pd.DateOffset(
            days=3,
        )
        end = start + pd.DateOffset(days=2)

        with file_vcr.use_cassette(
            f"test_get_solar_market_participant_forecast_historical_{start.date()}_{end.date()}.yaml",
        ):
            df = self.iso.get_solar_market_participant_forecast(start, end=end)

        self._check_variable_generation_forecast(df)
        assert df["Interval Start"].min() == start + pd.Timedelta(days=1)
        assert df["Interval End"].max() == end + pd.Timedelta(days=2)

    def _check_shadow_prices(self, df: pd.DataFrame):
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert set(df.columns) == {
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Constraint",
            "Shadow Price",
        }
        assert self._check_is_datetime_type(df["Interval Start"])
        assert self._check_is_datetime_type(df["Interval End"])
        assert self._check_is_datetime_type(df["Publish Time"])
        assert df["Shadow Price"].dtype == "float64"

    def test_get_shadow_prices_real_time_5_min_latest(self):
        with file_vcr.use_cassette(
            "test_get_shadow_prices_real_time_5_min_latest.yaml",
        ):
            df = self.iso.get_shadow_prices_real_time_5_min("latest")
            self._check_shadow_prices(df)
            assert df["Interval Start"].is_monotonic_increasing

    @pytest.mark.parametrize(
        "date, end",
        [
            (
                pd.Timestamp.now().normalize() - pd.Timedelta(days=12),
                pd.Timestamp.now().normalize() - pd.Timedelta(days=10),
            ),
        ],
    )
    def test_get_shadow_prices_real_time_5_min_historical_range(self, date, end):
        cassette_name = f"test_get_shadow_prices_real_time_5_min_historical_range_{pd.Timestamp(date, tz=self.default_timezone).date()}_{pd.Timestamp(end, tz=self.default_timezone).date()}.yaml"
        with file_vcr.use_cassette(cassette_name):
            df = self.iso.get_shadow_prices_real_time_5_min(date, end=end)
            self._check_shadow_prices(df)

    def test_get_shadow_prices_day_ahead_hourly_latest(self):
        with file_vcr.use_cassette(
            "test_get_shadow_prices_day_ahead_hourly_latest.yaml",
        ):
            df = self.iso.get_shadow_prices_day_ahead_hourly("latest")
            self._check_shadow_prices(df)
            assert df["Interval Start"].is_monotonic_increasing

    @pytest.mark.parametrize(
        "date, end",
        [
            (
                pd.Timestamp.now().normalize() - pd.Timedelta(days=12),
                pd.Timestamp.now().normalize() - pd.Timedelta(days=10),
            ),
        ],
    )
    def test_get_shadow_prices_day_ahead_hourly_historical_range(self, date, end):
        cassette_name = f"test_get_shadow_prices_day_ahead_hourly_historical_range_{pd.Timestamp(date, tz=self.default_timezone).date()}_{pd.Timestamp(end, tz=self.default_timezone).date()}.yaml"
        with file_vcr.use_cassette(cassette_name):
            df = self.iso.get_shadow_prices_day_ahead_hourly(date, end=end)
            self._check_shadow_prices(df)

        """get_lmp_real_time_operating_reserves"""

    def _check_lmp_real_time_5_min_operating_reserves(self, data: pd.DataFrame) -> None:
        assert data.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Location",
            "LMP 10S",
            "Congestion 10S",
            "LMP 10N",
            "Congestion 10N",
            "LMP 30R",
            "Congestion 30R",
        ]

        assert self._check_is_datetime_type(data["Interval Start"])
        assert self._check_is_datetime_type(data["Interval End"])

        assert data["Location"].dtype == "object"
        for col in [
            "LMP 10S",
            "Congestion 10S",
            "LMP 10N",
            "Congestion 10N",
            "LMP 30R",
            "Congestion 30R",
        ]:
            assert data[col].dtype == "float64"

        assert (
            data["Interval End"] - data["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()

    def test_get_lmp_real_time_operating_reserves_latest(self):
        with file_vcr.use_cassette(
            "test_lmp_get_real_time_5_min_operating_reserves_latest.yaml",
        ):
            data = self.iso.get_lmp_real_time_operating_reserves("latest")

        self._check_lmp_real_time_5_min_operating_reserves(data)
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        assert (data["Interval Start"].dt.date == today.date()).all()

    def test_get_lmp_real_time_operating_reserves_historical_date_range(self):
        # Only date for which data is available
        start_date = (pd.Timestamp.utcnow() - pd.DateOffset(days=3)).normalize()
        end_date = start_date + pd.DateOffset(days=1)

        with file_vcr.use_cassette(
            f"test_lmp_get_real_time_5_min_operating_reserves_historical_date_range_{start_date.date()}_{end_date.date()}.yaml",
        ):
            data = self.iso.get_lmp_real_time_operating_reserves(
                start_date,
                end=end_date,
            )

        self._check_lmp_real_time_5_min_operating_reserves(data)

        assert data["Interval Start"].min() == start_date
        assert data["Interval Start"].max() == end_date - pd.Timedelta(minutes=5)


class TestIESOSafeXMLParsing:
    """Test the safe XML parsing helper functions."""

    def test_safe_find_text(self):
        """Test _safe_find_text with various scenarios."""
        # Valid element
        root = ElementTree.fromstring("<root><item>test_value</item></root>")
        assert _safe_find_text(root, "item") == "test_value"

        # Missing element
        assert _safe_find_text(root, "missing", default="default") == "default"

        # Empty element
        empty_root = ElementTree.fromstring("<root><item></item></root>")
        assert _safe_find_text(empty_root, "item", default="default") == "default"

        # Whitespace-only element
        ws_root = ElementTree.fromstring("<root><item>   </item></root>")
        assert _safe_find_text(ws_root, "item", default="default") == "default"

        # None element
        assert _safe_find_text(None, "item", default="default") == "default"

    def test_safe_find_int(self):
        """Test _safe_find_int with various scenarios."""
        # Valid number
        root = ElementTree.fromstring("<root><item>123</item></root>")
        assert _safe_find_int(root, "item") == 123

        # Missing element
        assert _safe_find_int(root, "missing", default=999) == 999

        # Invalid text
        invalid_root = ElementTree.fromstring("<root><item>not_a_number</item></root>")
        assert _safe_find_int(invalid_root, "item", default=999) == 999

    def test_safe_find_float(self):
        """Test _safe_find_float with various scenarios."""
        # Valid number
        root = ElementTree.fromstring("<root><item>123.45</item></root>")
        assert _safe_find_float(root, "item") == 123.45

        # Missing element
        assert _safe_find_float(root, "missing", default=999.99) == 999.99

        # Invalid text
        invalid_root = ElementTree.fromstring("<root><item>not_a_number</item></root>")
        assert _safe_find_float(invalid_root, "item", default=999.99) == 999.99

    def test_safe_find_with_namespaces(self):
        """Test safe find functions work with XML namespaces."""
        xml_with_ns = """<root xmlns:ns="http://example.com">
            <ns:item>test_value</ns:item>
            <ns:number>42</ns:number>
        </root>"""
        root = ElementTree.fromstring(xml_with_ns)
        ns = {"ns": "http://example.com"}

        assert _safe_find_text(root, "ns:item", namespaces=ns) == "test_value"
        assert _safe_find_int(root, "ns:number", namespaces=ns) == 42
