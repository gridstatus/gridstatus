import pandas as pd
import pytest

import gridstatus
from gridstatus import Ercot, Markets, NotSupported
from gridstatus.ercot import parse_timestamp_from_friendly_name
from gridstatus.tests.base_test_iso import BaseTestISO


class TestErcot(BaseTestISO):
    iso = Ercot()

    def test_get_sced_system_lambda(self):
        for i in ["latest", "today"]:
            df = self.iso.get_sced_system_lambda(i, verbose=True)
            assert df.shape[0] >= 0
            assert df.columns.tolist() == [
                "SCED Timestamp",
                "System Lambda",
            ]
            today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
            assert df["SCED Timestamp"].unique()[0].date() == today
            assert isinstance(df["System Lambda"].unique()[0], float)

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

    def test_get_as_monitor(self):
        df = self.iso.get_as_monitor()

        # asset length is 1, 49 columns
        assert df.shape == (1, 49)
        # assert every colunn but the first is int dtype
        assert df.iloc[:, 1:].dtypes.unique() == "int64"
        assert df.columns[0] == "Time"

    def test_get_real_time_system_conditions(self):
        df = self.iso.get_real_time_system_conditions()
        assert df.shape == (1, 15)
        assert df.columns[0] == "Time"

    def test_get_energy_storage_resources(self):
        df = self.iso.get_energy_storage_resources()

        assert df.columns.tolist() == [
            "Time",
            "Total Charging",
            "Total Discharging",
            "Net Output",
        ]

    """get_fuel_mix"""

    def test_get_fuel_mix(self):
        # today
        cols = [
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
        df = self.iso.get_fuel_mix("today")
        self._check_fuel_mix(df)
        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        # latest
        df = self.iso.get_fuel_mix("latest")
        self._check_fuel_mix(df)
        # returns two days of data
        assert df["Time"].dt.date.nunique() == 2
        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

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
    def test_range_two_days_with_day_start_endpoint(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_start_end_same_day(self):
        pass

    """get_lmp"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_lmp_date_range(self, markets=None):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_lmp_historical(self, markets=None):
        pass

    def test_get_load_3_days_ago(self):
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        three_days_ago = today - pd.Timedelta(days=3)
        df = self.iso.get_load(three_days_ago)
        self._check_load(df)
        assert df["Time"].unique()[0].date() == three_days_ago

    def test_get_load_by_weather_zone(self):
        df = self.iso.get_load_by_weather_zone("today")
        self._check_time_columns(df, instant_or_interval="interval")
        cols = [
            "Time",
            "Interval Start",
            "Interval End",
            "COAST",
            "EAST",
            "FAR_WEST",
            "NORTH",
            "NORTH_C",
            "SOUTHERN",
            "SOUTH_C",
            "WEST",
            "TOTAL",
        ]
        assert df.columns.tolist() == cols

        # test 5 days ago
        five_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(days=5)
        df = self.iso.get_load_by_weather_zone(five_days_ago)
        self._check_time_columns(df, instant_or_interval="interval")
        assert df["Time"].unique()[0].date() == five_days_ago

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

    def test_get_load_forecast_range(self):
        end = pd.Timestamp.now(tz=self.iso.default_timezone)
        start = end - pd.Timedelta(hours=3)
        df = self.iso.get_load_forecast(start=start, end=end)

        unique_load_forecast_time = df["Publish Time"].unique()
        # make sure each is between start and end
        assert (unique_load_forecast_time >= start).all()
        assert (unique_load_forecast_time <= end).all()

    def test_get_load_forecast_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_load_forecast_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical_with_date_range(self):
        pass

    """get_spp"""

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

    @pytest.mark.skip(reason="takes too long to run")
    def test_get_spp_rtm_historical(self):
        rtm = gridstatus.Ercot().get_rtm_spp(2020)
        assert isinstance(rtm, pd.DataFrame)
        assert len(rtm) > 0

    @pytest.mark.slow
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

        df_dict = self.iso.get_60_day_sced_disclosure(date=days_ago_65, process=True)

        load_resource = df_dict["sced_load_resource"]
        gen_resource = df_dict["sced_gen_resource"]
        smne = df_dict["sced_smne"]

        assert load_resource["SCED Time Stamp"].dt.date.unique()[0] == days_ago_65
        assert gen_resource["SCED Time Stamp"].dt.date.unique()[0] == days_ago_65
        assert smne["Interval Time"].dt.date.unique()[0] == days_ago_65

        assert load_resource.shape[1] == 22
        assert gen_resource.shape[1] == 29
        assert smne.shape[1] == 6

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

        df_dict = self.iso.get_60_day_sced_disclosure(
            start=days_ago_66,
            end=days_ago_65
            + pd.Timedelta(days=1),  # add one day to end date since exclusive
            verbose=True,
        )

        load_resource = df_dict["sced_load_resource"]
        gen_resource = df_dict["sced_gen_resource"]
        smne = df_dict["sced_smne"]

        assert load_resource["SCED Time Stamp"].dt.date.unique().tolist() == [
            days_ago_66,
            days_ago_65,
        ]

        assert gen_resource["SCED Time Stamp"].dt.date.unique().tolist() == [
            days_ago_66,
            days_ago_65,
        ]

        assert smne["Interval Time"].dt.date.unique().tolist() == [
            days_ago_66,
            days_ago_65,
        ]

    """get_60_day_dam_disclosure"""

    def test_get_60_day_dam_disclosure_historical(self):
        days_ago_65 = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=65,
        )

        df_dict = self.iso.get_60_day_dam_disclosure(date=days_ago_65, process=True)
        assert df_dict is not None

        dam_gen_resource = df_dict["dam_gen_resource"]
        dam_gen_resource_as_offers = df_dict["dam_gen_resource_as_offers"]
        dam_load_resource = df_dict["dam_load_resource"]
        dam_load_resource_as_offers = df_dict["dam_load_resource_as_offers"]
        dam_energy_bids = df_dict["dam_energy_bids"]
        dam_energy_bid_awards = df_dict["dam_energy_bid_awards"]

        assert dam_gen_resource.shape[1] == 29
        assert dam_gen_resource_as_offers.shape[1] == 62
        assert dam_load_resource.shape[1] == 19
        assert dam_load_resource_as_offers.shape[1] == 63
        assert dam_energy_bids.shape[1] == 28
        assert dam_energy_bid_awards.shape[1] == 8

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

    def test_spp_real_time_parse_retry_file_name(self):
        assert parse_timestamp_from_friendly_name(
            "SPPHLZNP6905_retry_20230608_1545_csv"
        ) == pd.Timestamp("2023-06-08 15:45:00-0500", tz="US/Central")

        assert parse_timestamp_from_friendly_name(
            "SPPHLZNP6905_20230608_1545_csv"
        ) == pd.Timestamp("2023-06-08 15:45:00-0500", tz="US/Central")

    """get_unplanned_resource_outages"""

    def test_get_unplanned_resource_outages(self):
        five_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(
            days=5,
        )
        df = self.iso.get_unplanned_resource_outages(date=five_days_ago)

        cols = [
            "Report Time",
            "Resource Name",
            "Resource Unit Code",
            "Fuel Type",
            "Outage Type",
            "Available MW Maximum",
            "Available MW During Outage",
            "Effective MW Reduction Due to Outage",
            "Actual Outage Start",
            "Planned End Date",
            "Actual End Date",
            "Nature Of Work",
        ]

        time_cols = [
            "Report Time",
            "Actual Outage Start",
            "Planned End Date",
            "Actual End Date",
        ]

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols
        assert df["Report Time"].dt.date.unique() == [five_days_ago.date()]
        for col in time_cols:
            assert df[col].dt.tz is not None

        start = five_days_ago - pd.DateOffset(1)
        df_2_days = self.iso.get_unplanned_resource_outages(
            start=start,
            end=five_days_ago + pd.DateOffset(1),
        )

        assert df_2_days.shape[0] >= 0
        assert df_2_days.columns.tolist() == cols
        assert df_2_days["Report Time"].dt.date.nunique() == 2
        assert df_2_days["Report Time"].min().date() == start.date()
        assert df_2_days["Report Time"].max().date() == five_days_ago.date()

    """test get_highest_price_as_offer_selected"""

    def test_get_highest_price_as_offer_selected(self):
        four_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(
            days=4,
        )

        five_days_ago = four_days_ago - pd.Timedelta(
            days=1,
        )

        df = self.iso.get_highest_price_as_offer_selected(
            start=five_days_ago,
            end=four_days_ago
            + pd.Timedelta(
                days=1,
            ),
        )

        assert (
            df["Interval Start"].dt.date.unique()
            == [five_days_ago.date(), four_days_ago.date()]
        ).all()

        cols = [
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

        assert df.columns.tolist() == cols

    """test get_as_reports"""

    def test_get_as_reports(self):
        four_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(
            days=4,
        )

        five_days_ago = four_days_ago - pd.Timedelta(
            days=1,
        )

        df = self.iso.get_as_reports(
            start=five_days_ago,
            end=four_days_ago
            + pd.Timedelta(
                days=1,
            ),
        )

        assert (
            df["Interval Start"].dt.date.unique()
            == [five_days_ago.date(), four_days_ago.date()]
        ).all()

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

        assert df.columns.tolist() == cols

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
        end = date.floor("H")
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

        return df

    def test_get_hourly_wind_report(self):
        # test specific hour
        cols = [
            "Publish Time",
            "Time",
            "Interval Start",
            "Interval End",
            "ACTUAL SYSTEM WIDE",
            "COP HSL SYSTEM WIDE",
            "STWPF SYSTEM WIDE",
            "WGRPP SYSTEM WIDE",
            "ACTUAL LZ SOUTH HOUSTON",
            "COP HSL LZ SOUTH HOUSTON",
            "STWPF LZ SOUTH HOUSTON",
            "WGRPP LZ SOUTH HOUSTON",
            "ACTUAL LZ WEST",
            "COP HSL LZ WEST",
            "STWPF LZ WEST",
            "WGRPP LZ WEST",
            "ACTUAL LZ NORTH",
            "COP HSL LZ NORTH",
            "STWPF LZ NORTH",
            "WGRPP LZ NORTH",
        ]
        date = pd.Timestamp.now(tz=self.iso.default_timezone) - pd.Timedelta(
            days=1,
        )
        df = self.iso.get_hourly_wind_report(date)
        assert df["Publish Time"].nunique() == 1
        assert df["Publish Time"].min() < date
        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

    def test_get_hourly_solar_report(self):
        # test specific hour
        cols = [
            "Publish Time",
            "Time",
            "Interval Start",
            "Interval End",
            "GEN SYSTEM WIDE",
            "COP HSL SYSTEM WIDE",
            "STPPF SYSTEM WIDE",
            "PVGRPP SYSTEM WIDE",
            "GEN CenterWest",
            "COP HSL CenterWest",
            "STPPF CenterWest",
            "PVGRPP CenterWest",
            "GEN NorthWest",
            "COP HSL NorthWest",
            "STPPF NorthWest",
            "PVGRPP NorthWest",
            "GEN FarWest",
            "COP HSL FarWest",
            "STPPF FarWest",
            "PVGRPP FarWest",
            "GEN FarEast",
            "COP HSL FarEast",
            "STPPF FarEast",
            "PVGRPP FarEast",
            "GEN SouthEast",
            "COP HSL SouthEast",
            "STPPF SouthEast",
            "PVGRPP SouthEast",
            "GEN CenterEast",
            "COP HSL CenterEast",
            "STPPF CenterEast",
            "PVGRPP CenterEast",
        ]
        date = pd.Timestamp.now(tz=self.iso.default_timezone) - pd.Timedelta(
            days=1,
        )
        df = self.iso.get_hourly_solar_report(date, verbose=True)

        assert df["Publish Time"].nunique() == 1
        assert df["Publish Time"].min() < date
        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

    """get_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

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

    def test_get_lmp_electrical_bus(self):
        cols = [
            "SCED Timestamp",
            "Market",
            "Location",
            "Location Type",
            "LMP",
        ]

        df = self.iso.get_lmp(
            date="latest",
            location_type="Electrical Bus",
        )

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        now = pd.Timestamp.now(tz=self.iso.default_timezone)
        start = now - pd.Timedelta(hours=1)
        df = self.iso.get_lmp(
            location_type="Electrical Bus",
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

    def test_get_lmp_settlement_point(self):
        df = self.iso.get_lmp(
            date="latest",
            location_type="Settlement Point",
        )

        cols = [
            "SCED Timestamp",
            "Market",
            "Location",
            "Location Type",
            "LMP",
        ]

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

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
