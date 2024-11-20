import datetime

import pandas as pd
import pytest
from pandas.core.dtypes.common import is_numeric_dtype

from gridstatus import IESO, utils
from gridstatus.base import NotSupported
from gridstatus.ieso import (
    MAXIMUM_DAYS_IN_FUTURE_FOR_ZONAL_LOAD_FORECAST,
    MAXIMUM_DAYS_IN_PAST_FOR_COMPLETE_GENERATOR_REPORT,
    MAXIMUM_DAYS_IN_PAST_FOR_LOAD,
)
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

TIME_COLUMN = "Interval Start"

api_vcr = setup_vcr(
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

    @pytest.mark.integration
    def test_get_generator_report_hourly_historical(self):
        # date string works
        date = pd.Timestamp.now(tz=self.default_timezone) - pd.Timedelta(days=10)
        date_str = date.strftime("%m/%d/%Y")
        df = self.iso.get_generator_report_hourly(date_str)

        assert isinstance(df, pd.DataFrame)
        assert df.loc[0][TIME_COLUMN].strftime("%m/%d/%Y") == date_str
        assert df.loc[0][TIME_COLUMN].tz is not None
        self._check_get_generator_report_hourly(df)

        # timestamp object works
        timestamp_obj = date.date()
        df = self.iso.get_generator_report_hourly(timestamp_obj)
        assert isinstance(df, pd.DataFrame)
        assert df.loc[0][TIME_COLUMN].strftime(
            "%Y%m%d",
        ) == timestamp_obj.strftime("%Y%m%d")
        assert df.loc[0][TIME_COLUMN].tz is not None
        self._check_get_generator_report_hourly(df)

        # datetime object works
        date_obj = date.date()
        df = self.iso.get_generator_report_hourly(date_obj)
        assert isinstance(df, pd.DataFrame)
        assert df.loc[0][TIME_COLUMN].strftime(
            "%Y%m%d",
        ) == date_obj.strftime("%Y%m%d")
        assert df.loc[0][TIME_COLUMN].tz is not None
        self._check_get_generator_report_hourly(df)

    @pytest.mark.integration
    def test_get_generator_report_hourly_historical_with_date_range(self):
        # range not inclusive, add one to include today
        num_days = 7
        end = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ) + pd.Timedelta(days=1)
        start = end - pd.Timedelta(days=num_days)

        df = self.iso.get_generator_report_hourly(
            date=start.date(),
            end=end.date(),
        )
        self._check_get_generator_report_hourly(df)

        # make sure right number of days are returned
        assert df[TIME_COLUMN].dt.day.nunique() == num_days

    @pytest.mark.integration
    def test_get_generator_report_hourly_range_two_days_with_end(self):
        yesterday = utils._handle_date(
            "today",
            self.iso.default_timezone,
        ) - pd.Timedelta(days=1)
        yesterday = yesterday.replace(hour=1, minute=0, second=0, microsecond=0)
        start = yesterday - pd.Timedelta(hours=3)

        df = self.iso.get_generator_report_hourly(
            date=start,
            end=yesterday + pd.Timedelta(minutes=1),
        )

        assert df[TIME_COLUMN].max() >= yesterday.replace(
            hour=0,
            minute=0,
            second=0,
        )
        assert df[TIME_COLUMN].min() <= start

        self._check_get_generator_report_hourly(df)

    @pytest.mark.integration
    def test_get_generator_report_hourly_start_end_same_day(self):
        yesterday = utils._handle_date(
            "today",
            self.iso.default_timezone,
        ) - pd.Timedelta(days=1)
        start = yesterday.replace(hour=0, minute=5, second=0, microsecond=0)
        end = yesterday.replace(hour=6, minute=5, second=0, microsecond=0)
        df = self.iso.get_generator_report_hourly(date=start, end=end)
        # ignore last row, since it is sometime midnight of next day
        assert df[TIME_COLUMN].iloc[:-1].dt.date.unique().tolist() == [
            yesterday.date(),
        ]
        self._check_get_generator_report_hourly(df)

    @pytest.mark.integration
    def test_get_generator_report_hourly_latest(self):
        df = self.iso.get_generator_report_hourly("latest")
        self._check_get_generator_report_hourly(df)

        assert df[TIME_COLUMN].min() == pd.Timestamp.now(
            tz=self.default_timezone,
        ).floor("D")

        assert df[TIME_COLUMN].max() >= pd.Timestamp.now(
            tz=self.default_timezone,
            # Account for data not immediately available
        ).floor("h") - pd.Timedelta(hours=2)

    @pytest.mark.integration
    def test_get_generator_report_hourly_today(self):
        df = self.iso.get_generator_report_hourly("today")
        assert df.equals(self.iso.get_generator_report_hourly("latest"))

    @pytest.mark.integration
    def test_get_generator_report_hourly_too_far_in_past_raises_error(self):
        with pytest.raises(NotSupported):
            self.iso.get_generator_report_hourly(
                pd.Timestamp.now(tz=self.default_timezone).date()
                - pd.Timedelta(
                    days=MAXIMUM_DAYS_IN_PAST_FOR_COMPLETE_GENERATOR_REPORT + 1,
                ),
            )

    @pytest.mark.integration
    def test_get_generator_report_hourly_in_future_raises_error(self):
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

    """get_load"""

    @pytest.mark.integration
    def test_get_load_today(self):
        df = self.iso.get_load("today")
        self._check_load(df)

        today = pd.Timestamp.now(tz=self.default_timezone)
        # First interval on the day
        assert df[TIME_COLUMN].min() == today.normalize()
        assert df["Interval End"].min() == today.normalize() + pd.Timedelta(minutes=5)
        assert df[TIME_COLUMN].max().date() == today.date()

        assert (df[TIME_COLUMN].dt.date == today.date()).all()

    @pytest.mark.integration
    def test_get_load_latest(self):
        df = self.iso.get_load("latest")

        self._check_load(df)
        now = pd.Timestamp.now(tz=self.default_timezone)
        # First interval should be the first interval of the hour
        assert df[TIME_COLUMN].min() == now.floor("h")

        assert df.shape[0] <= 12

    @pytest.mark.integration
    def test_get_load_yesterday_full_day(self):
        date = (
            pd.Timestamp.now(tz=self.default_timezone) - pd.Timedelta(days=1)
        ).date()
        end = date + pd.Timedelta(days=1)
        df = self.iso.get_load(date, end=end)
        assert df.shape[0] == 288

        beginning_of_date = pd.Timestamp(date, tz=self.default_timezone).replace(
            hour=0,
            minute=0,
            second=0,
        )
        assert df[TIME_COLUMN].min() == beginning_of_date

        end_of_date = beginning_of_date + pd.Timedelta(days=1)
        assert df["Interval End"].max() == end_of_date

    @pytest.mark.integration
    def test_get_load_historical_with_date_range(self):
        num_days = 2
        end = pd.Timestamp.now(
            tz=self.default_timezone,
        ) + pd.Timedelta(days=1)
        start = end - pd.Timedelta(days=num_days)

        data = self.iso.get_load(date=start.date(), end=end.date())
        self._check_load(data)
        # make sure right number of days are returned
        assert data[TIME_COLUMN].dt.day.nunique() == num_days

        data_tuple = self.iso.get_load(date=(start.date(), end.date()))

        assert data_tuple.equals(data)

    @pytest.mark.integration
    def test_get_load_historical(self):
        # pick a test date 2 weeks back
        test_date = (pd.Timestamp.now() - pd.Timedelta(days=14)).date()

        # date string works
        date_str = test_date.strftime("%Y%m%d")
        df = self.iso.get_load(date_str)
        self._check_load(df)
        assert df.loc[0][TIME_COLUMN].strftime("%Y%m%d") == date_str

        # timestamp object works
        df = self.iso.get_load(test_date)

        self._check_load(df)
        assert df.loc[0][TIME_COLUMN].strftime(
            "%Y%m%d",
        ) == test_date.strftime("%Y%m%d")

        # datetime object works
        df = self.iso.get_load(test_date)
        self._check_load(df)
        assert df.loc[0][TIME_COLUMN].strftime(
            "%Y%m%d",
        ) == test_date.strftime("%Y%m%d")

    @pytest.mark.integration
    def test_get_load_tomorrow_raises_error(self):
        with pytest.raises(NotSupported):
            self.iso.get_load(
                pd.Timestamp.now(tz=self.default_timezone).date()
                + pd.Timedelta(days=1),
            )

    @pytest.mark.integration
    def test_get_load_too_far_in_past_raises_error(self):
        with pytest.raises(NotSupported):
            self.iso.get_load(
                pd.Timestamp.now(tz=self.default_timezone).date()
                - pd.Timedelta(days=MAXIMUM_DAYS_IN_PAST_FOR_LOAD + 1),
            )

    """get_load_forecast"""

    @pytest.mark.integration
    def test_get_load_forecast_today(self):
        forecast = self.iso.get_load_forecast("today")
        self._check_load_forecast(forecast)

        assert forecast["Publish Time"].nunique() == 1
        assert forecast[TIME_COLUMN].min() == pd.Timestamp.now(
            tz=self.default_timezone,
        ).normalize() - pd.Timedelta(days=5)

        assert forecast[TIME_COLUMN].max() == pd.Timestamp.now(
            tz=self.default_timezone,
        ).normalize() + pd.Timedelta(days=2)

    @pytest.mark.integration
    def test_get_load_forecast_latest(self):
        assert self.iso.get_load_forecast("latest").equals(
            self.iso.get_load_forecast("today"),
        )

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical_with_date_range(self):
        pass

    """get_zonal_load_forecast"""

    @pytest.mark.integration
    def test_get_zonal_load_forecast_historical(self):
        test_date = (pd.Timestamp.now() - pd.Timedelta(days=3)).date()
        forecast = self.iso.get_zonal_load_forecast(date=test_date)
        self._check_zonal_load_forecast(forecast)

    @pytest.mark.integration
    def test_get_zonal_load_forecast_historical_with_date_range(self):
        end = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
        start = (end - pd.Timedelta(days=2)).date()
        forecast = self.iso.get_zonal_load_forecast(
            start,
            end=end,
        )
        self._check_zonal_load_forecast(forecast)

    @pytest.mark.integration
    def test_get_zonal_load_forecast_today(self):
        forecast = self.iso.get_zonal_load_forecast("today")

        assert (
            forecast[TIME_COLUMN].max().date()
            - pd.Timestamp.now(tz=self.default_timezone).date()
        ).days == MAXIMUM_DAYS_IN_FUTURE_FOR_ZONAL_LOAD_FORECAST

        assert (
            forecast[TIME_COLUMN].min()
            == pd.Timestamp.now(tz=self.default_timezone).normalize()
        )

        self._check_zonal_load_forecast(forecast)

        assert (
            forecast[TIME_COLUMN].min()
            == pd.Timestamp.now(tz=self.default_timezone).normalize()
        )

        self._check_zonal_load_forecast(forecast)

    @pytest.mark.integration
    def test_get_zonal_load_forecast_latest(self):
        assert self.iso.get_zonal_load_forecast("latest").equals(
            self.iso.get_zonal_load_forecast("today"),
        )

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

    def _check_load(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.shape[0] >= 0

        time_type = "interval"
        self._check_time_columns(df, instant_or_interval=time_type)

        for col in ["Market Total Load", "Ontario Load"]:
            assert col in df.columns
            assert is_numeric_dtype(df[col])

    def _check_time_columns(self, df, instant_or_interval="instant"):
        assert isinstance(df, pd.DataFrame)

        time_cols = [TIME_COLUMN, "Interval End"]
        ordered_by_col = TIME_COLUMN

        assert time_cols == df.columns[: len(time_cols)].tolist()
        # check all time cols are localized timestamps
        for col in time_cols:
            assert isinstance(df.loc[0][col], pd.Timestamp)
            assert df.loc[0][col].tz is not None

        self._check_ordered_by_time(df, ordered_by_col)

    def _check_load_forecast(self, df):
        assert set(df.columns) == set(
            [
                TIME_COLUMN,
                "Interval End",
                "Publish Time",
                "Ontario Load Forecast",
            ],
        )

        assert self._check_is_datetime_type(df["Publish Time"])
        assert self._check_is_datetime_type(df[TIME_COLUMN])
        assert self._check_is_datetime_type(df["Interval End"])
        assert df["Ontario Load Forecast"].dtype == "float64"

    def _check_zonal_load_forecast(self, df):
        assert set(df.columns) == set(
            [
                TIME_COLUMN,
                "Interval End",
                "Publish Time",
                "Ontario Load Forecast",
                "East Load Forecast",
                "West Load Forecast",
            ],
        )

        assert self._check_is_datetime_type(df["Publish Time"])
        assert self._check_is_datetime_type(df[TIME_COLUMN])
        assert self._check_is_datetime_type(df["Interval End"])
        assert df["Ontario Load Forecast"].dtype == "float64"
        assert df["East Load Forecast"].dtype == "float64"
        assert df["West Load Forecast"].dtype == "float64"

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
            "SOLAR",
            "WIND",
        ]

    """get_resource_adequacy_report"""

    @pytest.mark.parametrize(
        "date",
        [
            (pd.Timestamp.now(tz=default_timezone) - pd.Timedelta(days=119)).strftime(
                "%Y-%m-%d",
            ),  # NOTE(kladar): we will see how historical data rolls off
            (pd.Timestamp.now(tz=default_timezone) - pd.Timedelta(days=1)).strftime(
                "%Y-%m-%d",
            ),  # yesterday
            (pd.Timestamp.now(tz=default_timezone)).strftime("%Y-%m-%d"),
            (pd.Timestamp.now(tz=default_timezone) + pd.Timedelta(days=1)).strftime(
                "%Y-%m-%d",
            ),  # tomorrow
            (pd.Timestamp.now(tz=default_timezone) + pd.Timedelta(days=30)).strftime(
                "%Y-%m-%d",
            ),  # NOTE(kladar): we will see how future data rolls in
        ],
    )
    def test_get_resource_adequacy_report(self, date: str | datetime.date):
        with api_vcr.use_cassette(f"test_get_resource_adequacy_report_{date}.yaml"):
            df = self.iso.get_resource_adequacy_report(date)

        assert isinstance(df, pd.DataFrame)
        assert df.shape == (24, 90)  # 24 rows and 90 columns for each file

        required_cols = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Forecast Supply Capacity MW",
            "Forecast Supply Energy MWhr",
            "Forecast Supply Bottled Capacity MW",
            "Forecast Supply Regulation MW",
            "Total Forecast Supply MW",
            "Total Requirement MW",
            "Capacity Excess / Shortfall MW",
            "Energy Excess / Shortfall MWhr",
            "Offered Capacity Excess / Shortfall MW",
            "Resources Not Scheduled MW",
            "Imports Not Scheduled MW",
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
            "Hydro Forecasted (MWhr)",
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
            "Other Offered/Forecasted",
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
            "Total Internal Resources Offered/Forecasted",
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
            "Ontario Average Demand",
            "Ontario Wind Embedded Forecast",
            "Ontario Solar Embedded Forecast",
            "Ontario Dispatchable Load Capacity",
            "Ontario Dispatchable Load Bid / Forecasted",
            "Ontario Dispatchable Load Scheduled ON",
            "Ontario Dispatchable Load Scheduled OFF",
            "Ontario Hourly Demand Response Bid/Forecasted",
            "Ontario Hourly Demand Response Scheduled",
            "Ontario Hourly Demand Response Curtailed",
        ]
        for col in required_cols:
            assert col in df.columns

        assert self._check_is_datetime_type(df["Interval Start"])
        assert self._check_is_datetime_type(df["Interval End"])
        assert self._check_is_datetime_type(df["Publish Time"])
        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()

    # TODO(kladar): eventually don't record this each time
    @api_vcr.use_cassette("test_get_resource_adequacy_json.yaml")
    def test_get_resource_adequacy_json(self):
        date = pd.Timestamp.now(tz=self.default_timezone)
        json_data = self.iso._get_resource_adequacy_json(date)

        assert isinstance(json_data, dict)
        assert "Document" in json_data
        assert "DocHeader" in json_data["Document"]
        assert "DocBody" in json_data["Document"]

        doc_body = json_data["Document"]["DocBody"]
        assert "ForecastSupply" in doc_body
        assert "ForecastDemand" in doc_body
        assert "DeliveryDate" in doc_body

    def test_get_resource_adequacy_data_structure_map(self):
        data_map = self.iso._get_resource_adequacy_data_structure_map()

        assert isinstance(data_map, dict)
        assert "Supply" in data_map
        assert "Demand" in data_map

        supply = data_map["Supply"]
        assert "Hourly" in supply
        assert "Fuel_Type_Hourly" in supply
        assert "Total_Internal_Resources" in supply
        assert "Zonal_Import_Hourly" in supply

        demand = data_map["Demand"]
        assert "Ontario_Demand" in demand
        assert "Zonal_Export_Hourly" in demand
        assert "Total_Exports" in demand
        assert "Reserves" in demand

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
