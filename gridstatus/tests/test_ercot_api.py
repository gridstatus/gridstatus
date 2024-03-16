import datetime

import pandas as pd
import pytest
import pytz

from gridstatus.base import Markets
from gridstatus.ercot import ELECTRICAL_BUS_LOCATION_TYPE
from gridstatus.ercot_api.api_parser import VALID_VALUE_TYPES
from gridstatus.ercot_api.ercot_api import HISTORICAL_DAYS_THRESHOLD, ErcotAPI
from gridstatus.tests.base_test_iso import TestHelperMixin


@pytest.mark.requires_ercot_auth
class TestErcotAPI(TestHelperMixin):
    @classmethod
    def setup_class(cls):
        # https://docs.pytest.org/en/stable/how-to/xunit_setup.html
        # Runs before all tests in this class
        cls.iso = ErcotAPI()

    """lmp_by_bus_dam"""

    def _check_lmp_by_bus_dam(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Market",
            "Location",
            "Location Type",
            "LMP",
        ]

        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"

        assert (df["Market"] == Markets.DAY_AHEAD_HOURLY.name).all()

        assert df.dtypes["Location"] == "object"
        assert (df["Location Type"] == ELECTRICAL_BUS_LOCATION_TYPE).all()

        assert df.dtypes["LMP"] == "float64"

        assert ((df["Interval End"] - df["Interval Start"]) == pd.Timedelta("1h")).all()

    def test_get_lmp_by_bus_dam_today_and_latest(self):
        df = self.iso.get_lmp_by_bus_dam("today")

        self._check_lmp_by_bus_dam(df)

        assert df["Interval Start"].min() == self.local_start_of_today()

        # The end date will depend on when this runs, so check if it's today or tomorrow
        assert df["Interval End"].max() in [
            (self.local_start_of_today() + pd.Timedelta(days=d)) for d in [1, 2]
        ]

        assert self.iso.get_lmp_by_bus_dam("latest").equals(df)

    def test_get_lmp_by_bus_dam_historical(self):
        past_date = self.local_start_of_today() - pd.Timedelta(
            days=HISTORICAL_DAYS_THRESHOLD * 2,
        )

        df = self.iso.get_lmp_by_bus_dam(past_date, verbose=True)

        self._check_lmp_by_bus_dam(df)

        assert df["Interval Start"].min() == past_date.normalize()
        assert df["Interval End"].max() == past_date.normalize() + pd.Timedelta(
            days=1,
        )

    def test_get_lmp_by_bus_dam_historical_range(self):
        past_date = self.local_start_of_today() - pd.Timedelta(
            days=HISTORICAL_DAYS_THRESHOLD * 3,
        )
        past_end_date = past_date + pd.Timedelta(days=2)

        df = self.iso.get_lmp_by_bus_dam(past_date, past_end_date, verbose=True)

        self._check_lmp_by_bus_dam(df)

        assert df["Interval Start"].min() == past_date.normalize()
        assert df["Interval End"].max() == past_end_date.normalize()

    """shadow_prices_dam"""

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

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

        assert (
            df.loc[df["Contingency Name"] == "BASE CASE", "Limiting Facility"]
            .isna()
            .all()
        )

    def test_get_shadow_prices_dam_today_and_latest(self):
        df = self.iso.get_shadow_prices_dam("today", verbose=True)

        self._check_shadow_prices_dam(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        # The end date will depend on when this runs, so check if it's end of today
        # or end of tomorrow
        assert df["Interval End"].max() in [
            self.local_start_of_today()
            + pd.Timedelta(
                days=d,
            )
            for d in [1, 2]
        ]

        assert self.iso.get_shadow_prices_dam("latest").equals(df)

    def test_get_shadow_prices_dam_historical(self):
        past_date = self.local_start_of_today() - pd.Timedelta(
            days=HISTORICAL_DAYS_THRESHOLD * 3,
        )
        df = self.iso.get_shadow_prices_dam(past_date, verbose=True)

        self._check_shadow_prices_dam(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval Start"].max() == self.local_start_of_day(
            past_date,
        ) + pd.Timedelta(hours=23)

    def test_get_shadow_prices_dam_historical_range(self):
        past_date = self.local_start_of_today() - pd.Timedelta(
            days=HISTORICAL_DAYS_THRESHOLD * 4,
        )
        past_end_date = past_date + pd.Timedelta(days=2)

        df = self.iso.get_shadow_prices_dam(
            date=past_date,
            end=past_end_date,
            verbose=True,
        )

        self._check_shadow_prices_dam(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        # The data ends at the end of the day before the end date
        assert df["Interval Start"].max() == self.local_start_of_day(
            past_end_date,
        ) - pd.Timedelta(hours=1)

    """shadow_prices_sced"""

    expected_shadow_prices_sced_columns = [
        "SCED Timestamp",
        "Constraint ID",
        "Constraint Name",
        "Contingency Name",
        "Limiting Facility",
        "Shadow Price",
        "Max Shadow Price",
        "Limit",
        "Value",
        "Violated MW",
        "From Station",
        "To Station",
        "From Station kV",
        "To Station kV",
        "CCT Status",
    ]

    def _check_shadow_prices_sced(self, df):
        assert df.columns.tolist() == self.expected_shadow_prices_sced_columns

        self._check_time_columns(df, instant_or_interval="instant", sced=True)

    def test_get_shadow_prices_sced_today_and_latest(self):
        df = self.iso.get_shadow_prices_sced("today", verbose=True)
        self._check_shadow_prices_sced(df)

        # We don't know the exact SCED Timestamps
        assert df["SCED Timestamp"].min() < self.local_start_of_today() + pd.Timedelta(
            minutes=5,
        )
        assert df["SCED Timestamp"].max() < self.local_now()

        assert self.iso.get_shadow_prices_sced("latest").equals(df)

    def test_get_shadow_prices_sced_historical(self):
        past_date = self.local_start_of_today() - pd.Timedelta(
            days=HISTORICAL_DAYS_THRESHOLD * 3,
        )
        df = self.iso.get_shadow_prices_sced(past_date, verbose=True)

        self._check_shadow_prices_sced(df)

        start_of_past_date = self.local_start_of_day(past_date)

        assert df["SCED Timestamp"].min() < start_of_past_date

        max_timestamp = df["SCED Timestamp"].max()

        assert (
            start_of_past_date + pd.Timedelta(hours=22)
            < max_timestamp
            < start_of_past_date + pd.Timedelta(hours=24)
        )

    def test_get_shadow_prices_sced_historical_range(self):
        past_date = self.local_start_of_today() - pd.Timedelta(
            days=HISTORICAL_DAYS_THRESHOLD * 2,
        )
        past_end_date = past_date + pd.Timedelta(days=2)

        df = self.iso.get_shadow_prices_sced(
            date=past_date,
            end=past_end_date,
            verbose=True,
        )

        self._check_shadow_prices_sced(df)

        assert df["SCED Timestamp"].min() < self.local_start_of_day(past_date)

        max_timestamp = df["SCED Timestamp"].max()

        assert (
            self.local_start_of_day(past_end_date)
            - pd.Timedelta(days=1)
            + pd.Timedelta(hours=22)
            < max_timestamp
            < self.local_start_of_day(past_end_date)
        )

    """hit_ercot_api"""

    def test_hit_ercot_api(self):
        """
        First we test that entering a bad endpoint results in a keyerror
        """
        with pytest.raises(KeyError) as _:
            self.iso.hit_ercot_api("just a real bad endpoint right here")

        """
        Now a happy path test, using "actual system load by weather zone" endpoint.
        Starting from two days ago should result in 48 hourly values (or 24, depending
            on when the data is released and when the test is run), and there are
            12 columns in the resulting dataframe.
        We are also testing here that datetime objects are correctly parsed into
            the desired date string format that the operatingDayFrom parameter expects.
        """
        two_days_ago = datetime.datetime.now(tz=pytz.UTC) - datetime.timedelta(days=2)
        actual_by_wzn_endpoint = "/np6-345-cd/act_sys_load_by_wzn"
        two_days_actual_by_wzn = self.iso.hit_ercot_api(
            actual_by_wzn_endpoint,
            operatingDayFrom=two_days_ago,
        )
        result_rows, result_cols = two_days_actual_by_wzn.shape
        assert result_rows in {24, 48}
        assert result_cols == 12

        """
        Now let's apply a value filter and test it.
        We start by taking the midpoint value between min and max of total load over
            the last two days, then query with a filter of only values above that,
            using the totalFrom parameter. There should be fewer than 48 rows, and all
            values for total load should be greater than the threshold we put in.
        """
        min_load = two_days_actual_by_wzn["Total"].min()
        max_load = two_days_actual_by_wzn["Total"].max()
        in_between_load = (max_load + min_load) / 2
        higher_loads_result = self.iso.hit_ercot_api(
            actual_by_wzn_endpoint,
            operatingDayFrom=two_days_ago,
            totalFrom=in_between_load,
        )
        assert len(higher_loads_result["Total"]) < result_rows
        assert all(higher_loads_result["Total"] > in_between_load)

        """
        Now we test the page_size and max_pages arguments. We know that our two days
            query returns 24 or 48 results, so if we lower page_size to 10 and max_pages
            to 2, we should only get 20 rows total. We can also use this opportunity to
            test that invalid parameter names are silently ignored.
        """
        small_pages_result = self.iso.hit_ercot_api(
            actual_by_wzn_endpoint,
            page_size=10,
            max_pages=2,
            operatingDayFrom=two_days_ago,
            wowWhatAFakeParameter=True,
            thisOneIsAlsoFake=42,
        )
        assert small_pages_result.shape == (20, 12)

    """endpoints_map"""

    def test_get_endpoints_map(self):
        endpoints_map = self.iso._get_endpoints_map()

        # update this count as needed, if ercot api evolves to add/remove endpoints
        assert len(endpoints_map) == 102

        # detailed check of all endpoints, fields, and values
        issues = []
        for endpoint, endpoint_dict in endpoints_map.items():
            for issue in self._endpoints_map_check(endpoint_dict):
                issues.append([f"{endpoint} - {issue}"])
        assert len(issues) == 0

    def _endpoints_map_check(self, endpoint_dict: dict) -> list[str]:
        """Applies unit test checks to a single endpoint in the endpoints map.

        Ensures that top-level fields are present, and each parameter has a valid
        "payload" of value_type and parser_method

        Returns empty list if the given endpoint passes the check,
        otherwise returns a list of everything that's wrong, for ease of debugging
        """
        issues = []

        if "summary" not in endpoint_dict:
            issues.append("missing summary")

        parameters = endpoint_dict.get("parameters")
        if parameters is None:
            issues.append("missing parameters")
        else:
            for param, param_dict in parameters.items():
                value_type = param_dict.get("value_type")
                if value_type is None:
                    issues.append(f"{param} is missing value_type")
                elif value_type not in VALID_VALUE_TYPES:
                    issues.append(f"{param} has invalid value_type {value_type}")
                parser_method = param_dict.get("parser_method")
                if parser_method is None:
                    issues.append(f"{param} is missing parser_method")
                elif not callable(parser_method):
                    issues.append(f"{param} has an invalid parser_method")
        return issues
