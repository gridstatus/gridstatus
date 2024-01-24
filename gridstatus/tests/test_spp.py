import pandas as pd
import pytest

from gridstatus import SPP, Markets, NotSupported
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets


class TestSPP(BaseTestISO):
    iso = SPP()

    """get_fuel_mix"""

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

    def test_get_fuel_mix_central_time(self):
        fm = self.iso.get_fuel_mix(date="latest")
        assert fm.Time.iloc[0].tz.zone == self.iso.default_timezone

    def test_get_fuel_mix_self_market(self):
        fm = self.iso.get_fuel_mix(date="latest", detailed=True)

        cols = [
            "Time",
            "Interval Start",
            "Interval End",
            "Coal Market",
            "Coal Self",
            "Diesel Fuel Oil Market",
            "Diesel Fuel Oil Self",
            "Hydro Market",
            "Hydro Self",
            "Natural Gas Market",
            "Natural Gas Self",
            "Nuclear Market",
            "Nuclear Self",
            "Solar Market",
            "Solar Self",
            "Waste Disposal Services Market",
            "Waste Disposal Services Self",
            "Wind Market",
            "Wind Self",
            "Waste Heat Market",
            "Waste Heat Self",
            "Other Market",
            "Other Self",
        ]

        assert fm.columns.tolist() == cols

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
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_latest(self, market):
        super().test_get_lmp_latest(market=market)

    @pytest.mark.parametrize(
        "market,location_type",
        [
            (Markets.REAL_TIME_5_MIN, "Hub"),
            (Markets.REAL_TIME_5_MIN, "Interface"),
        ],
    )
    def test_get_lmp_latest_with_locations(self, market, location_type):
        df = self.iso.get_lmp(
            date="latest",
            market=market,
            location_type=location_type,
        )
        self._check_lmp_columns(df, market)

        location_types = df["Location Type"].unique()
        assert len(location_types) == 1
        assert location_types[0] == location_type

    def test_get_lmp_latest_settlement_type_returns_three_location_types(self):
        market = Markets.REAL_TIME_5_MIN
        df = self.iso.get_lmp(
            date="latest",
            market=market,
            verbose=True,
        )
        self._check_lmp_columns(df, market)

        assert set(df["Location Type"]) == {
            "Interface",
            "Hub",
            "Settlement Location",
        }

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "market,location_type",
        [
            (Markets.DAY_AHEAD_HOURLY, "Hub"),
            (Markets.REAL_TIME_5_MIN, "Hub"),
        ],
    )
    def test_get_lmp_today_with_location(self, market, location_type):
        df = self.iso.get_lmp(
            date="today",
            market=market,
            location_type=location_type,
        )
        self._check_lmp_columns(df, market=market)
        location_types = df["Location Type"].unique()
        assert len(location_types) == 1
        assert location_types[0] == location_type

    @pytest.mark.parametrize(
        "date,market,location_type",
        [
            ("latest", Markets.REAL_TIME_15_MIN, "Interface"),
            (
                pd.Timestamp.now().normalize() - pd.Timedelta(days=2),
                Markets.REAL_TIME_15_MIN,
                "Interface",
            ),
        ],
    )
    def test_get_lmp_unsupported_raises_not_supported(
        self,
        date,
        market,
        location_type,
    ):
        with pytest.raises(NotSupported):
            self.iso.get_lmp(
                date=date,
                market=market,
                location_type=location_type,
            )

    @pytest.mark.parametrize(
        "date,market,location_type",
        [
            ("latest", Markets.DAY_AHEAD_HOURLY, "Hub"),
            ("latest", Markets.DAY_AHEAD_HOURLY, "Interface"),
        ],
    )
    def test_get_lmp_day_ahead_cannot_have_latest(self, date, market, location_type):
        with pytest.raises(ValueError):
            self.iso.get_lmp(
                date=date,
                market=market,
                location_type=location_type,
            )

    OPERATING_RESERVES_COLUMNS = [
        "Time",
        "Interval Start",
        "Interval End",
        "Reserve Zone",
        "Reg_Up_Cleared",
        "Reg_Dn_Cleared",
        "Ramp_Up_Cleared",
        "Ramp_Dn_Cleared",
        "Unc_Up_Cleared",
        "STS_Unc_Up_Cleared",
        "Spin_Cleared",
        "Supp_Cleared",
    ]

    def test_get_operating_reserves(self):
        yesterday = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(
            days=1,
        )  # noqa
        yesterday_1230am = yesterday + pd.Timedelta(minutes=30)

        df = self.iso.get_operating_reserves(start=yesterday, end=yesterday_1230am)
        assert len(df) > 0
        assert df.columns.tolist() == self.OPERATING_RESERVES_COLUMNS

    def test_get_operating_reserves_latest(self):
        df = self.iso.get_operating_reserves(date="latest")
        assert len(df) > 0
        assert df.columns.tolist() == self.OPERATING_RESERVES_COLUMNS

    DAY_AHEAD_MARGINAL_CLEARING_PRICES_COLUMNS = [
        "Interval Start",
        "Interval End",
        "Market",
        "Reserve Zone",
        "Reg_Up",
        "Reg_Dn",
        "Ramp_Up",
        "Ramp_Dn",
        "Spin",
        "Supp",
        "Unc_Up",
    ]

    def test_get_day_ahead_operating_reserve_prices(self):
        tomorrow = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() + pd.Timedelta(
            days=1,
        )
        three_days_ago = tomorrow - pd.Timedelta(days=4)

        df = self.iso.get_day_ahead_operating_reserve_prices(
            date=three_days_ago,
            end=tomorrow,
        )

        assert df["Interval Start"].min() == three_days_ago
        assert df["Interval End"].max() == tomorrow
        assert df.columns.tolist() == self.DAY_AHEAD_MARGINAL_CLEARING_PRICES_COLUMNS

    def test_get_day_ahead_operating_reserve_prices_today(self):
        df = self.iso.get_day_ahead_operating_reserve_prices(date="today")

        assert (
            df["Interval Start"].min()
            == pd.Timestamp.now(tz=self.iso.default_timezone).normalize()
        )
        assert df["Interval End"].max() == pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() + pd.Timedelta(days=1)
        assert df.columns.tolist() == self.DAY_AHEAD_MARGINAL_CLEARING_PRICES_COLUMNS

    WEIS_LMP_COLUMNS = [
        "Interval Start",
        "Interval End",
        "Market",
        "Location",
        "Location Type",
        "PNode",
        "LMP",
        "Energy",
        "Congestion",
        "Loss",
    ]

    def test_get_lmp_real_time_weis_latest(self):
        df = self.iso.get_lmp_real_time_weis(date="latest")

        assert len(df) > 0
        assert df.columns.tolist() == self.WEIS_LMP_COLUMNS

    def test_get_lmp_real_time_weis_1_hour_range(self):
        three_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(
            days=3,
        )  # noqa
        three_days_ago_0015 = three_days_ago + pd.Timedelta(minutes=15)

        df = self.iso.get_lmp_real_time_weis(
            start=three_days_ago,
            end=three_days_ago_0015,
        )

        assert df["Interval Start"].min() == three_days_ago
        assert df["Interval End"].max() == three_days_ago_0015
        assert df.columns.tolist() == self.WEIS_LMP_COLUMNS

    def test_get_lmp_real_time_weis_cross_day(self):
        two_days_ago_2345 = (
            pd.Timestamp.now(tz=self.iso.default_timezone).normalize()
            - pd.Timedelta(days=2)
            + pd.Timedelta(hours=23, minutes=45)
        )  # noqa
        one_day_ago_0010 = two_days_ago_2345 + pd.Timedelta(minutes=25)

        df = self.iso.get_lmp_real_time_weis(
            start=two_days_ago_2345,
            end=one_day_ago_0010,
        )

        assert df["Interval Start"].min() == two_days_ago_2345
        assert df["Interval End"].max() == one_day_ago_0010
        assert df.columns.tolist() == self.WEIS_LMP_COLUMNS

    def test_get_lmp_real_time_weis_single_interval(self):
        three_weeks_ago = pd.Timestamp.now(tz=self.iso.default_timezone) - pd.Timedelta(
            days=21,
        )  # noqa
        df = self.iso.get_lmp_real_time_weis(date=three_weeks_ago)

        # assert one interval that straddles date input
        assert df["Interval Start"].min() < three_weeks_ago
        assert df["Interval End"].max() > three_weeks_ago
        assert df["Interval Start"].nunique() == 1
        assert df.columns.tolist() == self.WEIS_LMP_COLUMNS

    """get_load"""

    def test_get_load_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_load_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_historical_with_date_range(self):
        pass

    """get_load_forecast"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical_with_date_range(self):
        pass

    """get_solar_and_wind_actuals"""

    def test_get_solar_and_wind_load_today(self):
        df = self.iso.get_solar_and_wind_load(date="today")

        self._check_solar_and_wind_load(df)

        now = pd.Timestamp.now(tz=self.iso.default_timezone)
        assert df["Interval Start"].min() == now.normalize()
        assert df["Interval End"].max() <= now

    def test_get_solar_and_wind_load_latest(self):
        assert self.iso.get_solar_and_wind_load("latest").equals(
            self.iso.get_solar_and_wind_load(date="today"),
        )

    def test_get_solar_and_wind_load_historical(self):
        three_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(
            days=3,
        )

        df = self.iso.get_solar_and_wind_load(date=three_days_ago)

        assert df["Interval Start"].min() == three_days_ago
        assert df["Interval Start"].max() <= three_days_ago.replace(hour=23, minute=55)

        self._check_solar_and_wind_load(df)

    def test_get_solar_and_wind_load_historical_with_date_range(self):
        three_days_ago = (
            pd.Timestamp.now(tz=self.iso.default_timezone)
            - pd.Timedelta(
                days=3,
            )
        ).floor("5T")

        two_days_ago_end = three_days_ago + pd.Timedelta(hours=4)

        df = self.iso.get_solar_and_wind_load(date=three_days_ago, end=two_days_ago_end)

        assert df["Interval Start"].min() == three_days_ago
        assert df["Interval Start"].max() == two_days_ago_end

    """get_solar_and_wind_forecast"""

    def test_get_solar_and_wind_load_forecast_today(self):
        df = self.iso.get_solar_and_wind_load_forecast(date="today")

        assert df["Interval Start"].min() <= pd.Timestamp.now(
            tz=self.iso.default_timezone,
        )
        assert df["Interval Start"].max() >= pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ) + pd.Timedelta(days=5)

        assert df["Publish Time"].nunique() == 1

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

        self._check_solar_and_wind_forecast(df)

    def test_get_solar_and_wind_load_forecast_latest(self):
        df = self.iso.get_solar_and_wind_load_forecast("latest")

        assert df["Interval Start"].min() >= pd.Timestamp.now(
            tz=self.iso.default_timezone,
        )
        assert df["Interval Start"].max() >= pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ) + pd.Timedelta(days=5)

        assert df["Publish Time"].nunique() == 1

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=60)
        ).all()

        self._check_solar_and_wind_forecast(df)

    def test_get_solar_and_wind_load_forecast_historical(self):
        three_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(
            days=3,
        )

        df = self.iso.get_solar_and_wind_load_forecast(date=three_days_ago)

        assert df["Interval Start"].min() == three_days_ago

        # Should be 7 days of forecast
        assert df["Interval Start"].max() >= three_days_ago + pd.Timedelta(days=7)

        assert df["Publish Time"].nunique() == 1

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=60)
        ).all()

        self._check_solar_and_wind_forecast(df)

    def test_get_solar_and_wind_load_forecast_historical_with_date_range(self):
        three_days_ago = (
            pd.Timestamp.now(tz=self.iso.default_timezone)
            - pd.Timedelta(
                days=3,
            )
        ).floor("5T")

        two_days_ago_end = three_days_ago + pd.Timedelta(hours=4)

        df = self.iso.get_solar_and_wind_load_forecast(
            date=three_days_ago,
            end=two_days_ago_end,
        )

        assert df["Interval Start"].min() == three_days_ago
        assert df["Interval Start"].max() == two_days_ago_end

        assert df["Publish Time"].nunique() == (
            two_days_ago_end - three_days_ago
        ) / pd.Timedelta(minutes=5)

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()

        self._check_solar_and_wind_forecast(df)

    """get_status"""

    def test_get_status_latest(self):
        with pytest.raises(NotImplementedError):
            super().test_get_status_latest()

    """get_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

    """ get_ver_curtailment """

    def _check_ver_curtailments(self, df):
        assert isinstance(df, pd.DataFrame)

        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "Wind Redispatch Curtailments",
            "Wind Manual Curtailments",
            "Wind Curtailed For Energy",
            "Solar Redispatch Curtailments",
            "Solar Manual Curtailments",
            "Solar Curtailed For Energy",
        ]

    def test_get_ver_curtailments_historical(self):
        two_days_ago = pd.Timestamp.now() - pd.Timedelta(days=2)
        start = two_days_ago - pd.Timedelta(days=2)
        df = self.iso.get_ver_curtailments(start=start, end=two_days_ago)

        assert df["Interval Start"].min().date() == start.date()
        assert df["Interval Start"].max().date() == two_days_ago.date()
        self._check_ver_curtailments(df)

    def test_get_ver_curtailments_annual(self):
        year = 2020
        df = self.iso.get_ver_curtailments_annual(year=year)

        assert df["Interval Start"].min().date() == pd.Timestamp(f"{year}-01-01").date()
        assert df["Interval Start"].max().date() == pd.Timestamp(f"{year}-12-31").date()

        self._check_ver_curtailments(df)

    # get_capacity_of_generation_on_outage

    def _check_capacity_of_generation_on_outage(self, df):
        columns = [
            "Publish Time",
            "Interval Start",
            "Interval End",
            "Total Outaged MW",
            "Coal MW",
            "Diesel Fuel Oil MW",
            "Hydro MW",
            "Natural Gas MW",
            "Nuclear MW",
            "Solar MW",
            "Waste Disposal MW",
            "Wind MW",
            "Waste Heat MW",
            "Other MW",
        ]

        assert df.columns.tolist() == columns

    def test_get_capacity_of_generation_on_outage(self):
        two_days_ago = pd.Timestamp.now() - pd.Timedelta(days=2)
        start = two_days_ago - pd.Timedelta(days=2)
        df = self.iso.get_capacity_of_generation_on_outage(
            start=start,
            end=two_days_ago,
        )

        self._check_capacity_of_generation_on_outage(df)

        # confirm three weeks of data
        assert df.shape[0] / 168 == 3
        assert df["Publish Time"].dt.date.nunique() == 3

    def test_get_capacity_of_generation_on_outage_annual(self):
        year = 2020
        df = self.iso.get_capacity_of_generation_on_outage_annual(year=year)

        assert df["Interval Start"].min().date() == pd.Timestamp(f"{year}-01-01").date()

        # 2020 was a leap year
        assert df["Publish Time"].nunique() == 366

        self._check_capacity_of_generation_on_outage(df)

    def _check_solar_and_wind_load(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Actual Wind MW",
            "Actual Solar MW",
        ]

        assert (df["Actual Wind MW"] >= 0).all()
        assert (df["Actual Solar MW"] >= 0).all()

    def _check_solar_and_wind_forecast(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Wind Forecast MW",
            "Solar Forecast MW",
        ]

        assert (df["Wind Forecast MW"] >= 0).all()
        assert (df["Solar Forecast MW"] >= 0).all()
