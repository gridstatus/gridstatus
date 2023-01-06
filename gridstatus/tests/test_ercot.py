import pandas as pd
import pytest

import gridstatus
from gridstatus import Ercot, Markets, NotSupported
from gridstatus.tests.base_test_iso import BaseTestISO


class TestErcot(BaseTestISO):
    iso = Ercot()

    def test_get_as_prices(self):
        as_cols = [
            "Time",
            "Market",
            "Non-Spinning Reserves",
            "Regulation Down",
            "Regulation Up",
            "Responsive Reserves",
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
        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        # latest
        df = self.iso.get_fuel_mix("latest").mix
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

    """get_lmp"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_lmp_historical(self, markets=None):
        pass

    def test_get_load_3_days_ago(self):
        cols = [
            "Time",
            "Load",
        ]
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        three_days_ago = today - pd.Timedelta(days=3)
        df = self.iso.get_load(three_days_ago)
        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols
        assert df["Time"].unique()[0].date() == three_days_ago

    """get_load_forecast"""

    def test_get_load_forecast_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_load_forecast_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical_with_date_range(self):
        pass

    """get_spp"""

    def test_get_spp_dam_latest_day_ahead_hourly_zone_should_raise_exception(self):
        with pytest.raises(ValueError):
            self.iso.get_spp(
                date="latest",
                market=Markets.DAY_AHEAD_HOURLY,
                location_type="zone",
            )

    def test_get_spp_dam_today_day_ahead_hourly_hub(self):
        df = self.iso.get_spp(
            date="today",
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="hub",
        )
        self._check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Hub")

    def test_get_spp_dam_today_day_ahead_hourly_node(self):
        df = self.iso.get_spp(
            date="today",
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="node",
        )
        self._check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Node")

    def test_get_spp_dam_today_day_ahead_hourly_zone(self):
        df = self.iso.get_spp(
            date="today",
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="zone",
        )
        self._check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Zone")

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
            location_type="zone",
        )
        self._check_ercot_spp(df, Markets.REAL_TIME_15_MIN, "Zone")

    def test_get_spp_two_days_ago_day_ahead_hourly_zone(self):
        two_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=2,
        )
        df = self.iso.get_spp(
            date=two_days_ago,
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="zone",
        )
        self._check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Zone")

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
            location_type="zone",
        )
        self._check_ercot_spp(df, Markets.REAL_TIME_15_MIN, "Zone")

    """get_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

    """other"""

    def test__parse_delivery_date_hour_ending(self):
        df = pd.DataFrame(
            [
                {
                    "ExpectedTime": pd.Timestamp(
                        "2022-01-01 00:00:00-06:00",
                        tz="US/Central",
                    ),
                    "DeliveryDate": "01/01/2022",
                    "HourEnding": "01:00",
                },
                {
                    "ExpectedTime": pd.Timestamp(
                        "2022-01-01 23:00:00-06:00",
                        tz="US/Central",
                    ),
                    "DeliveryDate": "01/01/2022",
                    "HourEnding": "24:00",
                },
            ],
        )
        df["ActualTime"] = gridstatus.Ercot._parse_delivery_date_hour_ending(
            df,
            "US/Central",
        )
        assert df["ActualTime"].tolist() == df["ExpectedTime"].tolist()

    def test__parse_delivery_date_hour_interval(self):
        df = pd.DataFrame(
            [
                {
                    "ExpectedTime": pd.Timestamp(
                        "2022-01-01 00:00:00-06:00",
                        tz="US/Central",
                    ),
                    "DeliveryDate": "01/01/2022",
                    "DeliveryHour": "1",
                    "DeliveryInterval": "1",
                },
                {
                    "ExpectedTime": pd.Timestamp(
                        "2022-01-02 23:45:00-06:00",
                        tz="US/Central",
                    ),
                    "DeliveryDate": "01/02/2022",
                    "DeliveryHour": "24",
                    "DeliveryInterval": "4",
                },
            ],
        )
        df["ActualTime"] = gridstatus.Ercot._parse_delivery_date_hour_interval(
            df,
            "US/Central",
        )
        assert df["ActualTime"].tolist() == df["ExpectedTime"].tolist()

    def test__parse_oper_day_hour_ending(self):
        df = pd.DataFrame(
            [
                {
                    "ExpectedTime": pd.Timestamp(
                        "2022-01-01 00:00:00-06:00",
                        tz="US/Central",
                    ),
                    "Oper Day": "01/01/2022",
                    "Hour Ending": "100",
                },
                {
                    "ExpectedTime": pd.Timestamp(
                        "2022-01-01 23:00:00-06:00",
                        tz="US/Central",
                    ),
                    "Oper Day": "01/01/2022",
                    "Hour Ending": "2400",
                },
            ],
        )
        df["ActualTime"] = gridstatus.Ercot._parse_oper_day_hour_ending(
            df,
            "US/Central",
        )
        assert df["ActualTime"].tolist() == df["ExpectedTime"].tolist()

    @staticmethod
    def _check_ercot_spp(df, market, location_type):
        """Common checks for SPP data:
        - Columns
        - One Market
        - One Location Type
        """
        cols = [
            "Location",
            "Time",
            "Market",
            "Location Type",
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
