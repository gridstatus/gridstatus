import sys

import pandas as pd
import requests
from bs4 import BeautifulSoup

from gridstatus import utils
from gridstatus.base import (
    FuelMix,
    GridStatus,
    InterconnectionQueueStatus,
    ISOBase,
    Markets,
    NotSupported,
)
from gridstatus.decorators import support_date_range

LOCATION_TYPE_HUB = "HUB"
LOCATION_TYPE_INTERFACE = "INTERFACE"

QUERY_RTM5_HUBS_URL = "https://pricecontourmap.spp.org/arcgis/rest/services/MarketMaps/RTBM_FeatureData/MapServer/1/query"
QUERY_RTM5_INTERFACES_URL = "https://pricecontourmap.spp.org/arcgis/rest/services/MarketMaps/RTBM_FeatureData/MapServer/2/query"
QUERY_DAM_DELTA_HUBS_URL = "https://pricecontourmap.spp.org/arcgis/rest/services/MarketMaps/DELTA_FeatureData/MapServer/1/query"
QUERY_DAM_DELTA_INTERFACES_URL = "https://pricecontourmap.spp.org/arcgis/rest/services/MarketMaps/DELTA_FeatureData/MapServer/2/query"


class SPP(ISOBase):
    """Southwest Power Pool (SPP)"""

    name = "Southwest Power Pool"
    iso_id = "spp"

    default_timezone = "US/Central"

    status_homepage = "https://www.spp.org/markets-operations/current-grid-conditions/"
    interconnection_homepage = (
        "https://www.spp.org/engineering/generator-interconnection/"
    )

    markets = [
        Markets.REAL_TIME_5_MIN,
        Markets.DAY_AHEAD_HOURLY,
    ]

    location_types = [
        LOCATION_TYPE_HUB,
        LOCATION_TYPE_INTERFACE,
    ]

    def get_status(self, date=None, verbose=False):
        if date != "latest":
            raise NotSupported()

        url = "https://www.spp.org/markets-operations/current-grid-conditions/"
        html_text = requests.get(url).text
        soup = BeautifulSoup(html_text, "html.parser")
        conditions_element = soup.find("h1")
        last_update_time = conditions_element.findNextSibling("p").text[14:-1]
        status_text = (
            conditions_element.findNextSibling(
                "p",
            )
            .findNextSibling("p")
            .text
        )

        date_str = last_update_time[: last_update_time.index(", at ")]
        if "a.m." in last_update_time:
            time_str = last_update_time[
                last_update_time.index(", at ") + 5 : last_update_time.index(" a.m.")
            ]
            hour, minute = map(int, time_str.split(":"))
        elif "p.m." in last_update_time:
            time_str = last_update_time[
                last_update_time.index(", at ") + 5 : last_update_time.index(" p.m.")
            ]
            hour, minute = map(int, time_str.split(":"))
            if hour < 12:
                hour += 12
        else:
            raise "Cannot parse time of status"

        date_obj = (
            pd.to_datetime(date_str)
            .replace(
                hour=hour,
                minute=minute,
            )
            .tz_localize(self.default_timezone)
        )

        if (
            status_text
            == "SPP is currently in Normal Operations with no effective advisories or alerts."
        ):
            status = "Normal"
            notes = [status_text]
        else:
            status = status_text
            notes = None

        return GridStatus(
            time=date_obj,
            status=status,
            notes=notes,
            reserves=None,
            iso=self,
        )

    def get_fuel_mix(self, date, verbose=False):

        if date != "latest":
            raise NotSupported

        url = "https://marketplace.spp.org/chart-api/gen-mix/asChart"
        r = self._get_json(url)["response"]

        data = {"Timestamp": r["labels"]}
        data.update((d["label"], d["data"]) for d in r["datasets"])

        historical_mix = pd.DataFrame(data)

        current_mix = historical_mix.iloc[0].to_dict()

        time = pd.Timestamp(current_mix.pop("Timestamp"))

        return FuelMix(time=time, mix=current_mix, iso=self.name)

    def get_load(self, date, verbose=False):
        """Returns load for last 24hrs in 5 minute intervals"""

        if date == "latest":
            return self._latest_from_today(self.get_load)

        elif utils.is_today(date):
            df = self._get_load_and_forecast(verbose=verbose)

            df = df.dropna(subset=["Actual Load"])

            df = df.rename(columns={"Actual Load": "Load"})

            df = df[["Time", "Load"]]

            return df

        else:
            raise NotSupported()

    def get_load_forecast(self, date, forecast_type="MID_TERM", verbose=False):
        """

        type (str): MID_TERM is hourly for next 7 days or SHORT_TERM is every five minutes for a few hours
        """
        df = self._get_load_and_forecast(verbose=verbose)

        # gives forecast from before current day
        # only include forecasts start at current day
        last_actual = df.dropna(subset=["Actual Load"])["Time"].max()
        current_day = last_actual.replace(hour=0, minute=0)

        current_day_forecast = df[df["Time"] > current_day].copy()

        # assume forecast is made at last actual
        current_day_forecast["Forecast Time"] = last_actual

        if forecast_type == "MID_TERM":
            forecast_col = "Mid-Term Forecast"
        elif forecast_type == "SHORT_TERM":
            forecast_col = "Short-Term Forecast"
        else:
            raise RuntimeError("Invalid forecast type")

        # there will be empty rows regardless of forecast type since they dont align
        current_day_forecast = current_day_forecast.dropna(
            subset=[forecast_col],
        )

        current_day_forecast = current_day_forecast[
            ["Forecast Time", "Time", forecast_col]
        ].rename({forecast_col: "Load Forecast"}, axis=1)

        return current_day_forecast

    def _get_load_and_forecast(self, verbose=False):
        url = "https://marketplace.spp.org/chart-api/load-forecast/asChart"

        if verbose:
            print("Getting load and forecast from {}".format(url))

        r = self._get_json(url)["response"]

        data = {"Time": r["labels"]}
        for d in r["datasets"][:3]:
            if d["label"] == "Actual Load":
                data["Actual Load"] = d["data"]
            elif d["label"] == "Mid-Term Load Forecast":
                data["Mid-Term Forecast"] = d["data"]
            elif d["label"] == "Short-Term Load Forecast":
                data["Short-Term Forecast"] = d["data"]

        df = pd.DataFrame(data)

        df["Time"] = pd.to_datetime(
            df["Time"],
        ).dt.tz_convert(self.default_timezone)

        return df

        # todo where does date got in argument order
        # def get_historical_lmp(self, date, market: str, nodes: list):
        # 5 minute interal data
        # https://marketplace.spp.org/file-browser-api/download/rtbm-lmp-by-location?path=/2022/08/By_Interval/08/RTBM-LMP-SL-202208082125.csv

        # hub and interface prices
        # https://marketplace.spp.org/pages/hub-and-interface-prices

        # historical generation mix
        # https://marketplace.spp.org/pages/generation-mix-rolling-365
        # https://marketplace.spp.org/chart-api/gen-mix-365/asFile
        # 15mb file with five minute resolution

    def get_interconnection_queue(self, verbose=False):
        """Get interconnection queue

        Returns:
            pd.DataFrame: Interconnection queue


        """
        url = "https://opsportal.spp.org/Studies/GenerateActiveCSV"
        if verbose:
            print("Getting interconnection queue from {}".format(url))

        queue = pd.read_csv(url, skiprows=1)

        queue["Status (Original)"] = queue["Status"]

        queue["Status"] = queue["Status"].map(
            {
                "IA FULLY EXECUTED/COMMERCIAL OPERATION": InterconnectionQueueStatus.COMPLETED.value,
                "IA FULLY EXECUTED/ON SCHEDULE": InterconnectionQueueStatus.COMPLETED.value,
                "IA FULLY EXECUTED/ON SUSPENSION": InterconnectionQueueStatus.COMPLETED.value,
                "IA PENDING": InterconnectionQueueStatus.ACTIVE.value,
                "DISIS STAGE": InterconnectionQueueStatus.ACTIVE.value,
                "None": InterconnectionQueueStatus.ACTIVE.value,
            },
        )

        queue["Generation Type"] = queue[["Generation Type", "Fuel Type"]].apply(
            lambda x: " - ".join(x.dropna()),
            axis=1,
        )

        queue["Proposed Completion Date"] = queue["Commercial Operation Date"]

        rename = {
            "Generation Interconnection Number": "Queue ID",
            " Nearest Town or County": "County",
            "State": "State",
            "TO at POI": "Transmission Owner",
            "Capacity": "Capacity (MW)",
            "MAX Summer MW": "Summer Capacity (MW)",
            "MAX Winter MW": "Winter Capacity (MW)",
            "Generation Type": "Generation Type",
            "Request Received": "Queue Date",
            "Substation or Line": "Interconnection Location",
        }

        # todo: there are a few columns being parsed as "unamed" that aren't being included but should
        extra_columns = [
            "In-Service Date",
            "Commercial Operation Date",
            "Cessation Date",
            "Current Cluster",
            "Cluster Group",
            "Replacement Generator Commercial Op Date",
            "Service Type",
        ]

        missing = [
            "Project Name",
            "Interconnecting Entity",
            "Withdrawn Date",
            "Withdrawal Comment",
            "Actual Completion Date",
        ]

        queue = utils.format_interconnection_df(
            queue=queue,
            rename=rename,
            extra=extra_columns,
            missing=missing,
        )

        return queue

    @support_date_range(frequency="1D")
    def get_lmp(
        self,
        date,
        end=None,
        market: str = None,
        locations: list = "ALL",
        location_type: str = LOCATION_TYPE_HUB,
        verbose=False,
    ):
        """Get LMP data

        Supported Markets: REAL_TIME_5_MIN, DAY_AHEAD_HOURLY

        Supported Location Types: "hub", "interface"
        """
        market = Markets(market)
        if market not in self.markets:
            raise NotSupported(f"Market {market} not supported")
        if date != "latest":
            raise NotSupported(f"Date {date} is not supported for SPP")
        location_type = SPP._normalize_location_type(location_type)
        if market == Markets.REAL_TIME_5_MIN:
            df = self._get_latest_rtm5_lmp(
                location_type,
                verbose,
            )
        elif market == Markets.DAY_AHEAD_HOURLY:
            df = self._get_latest_dam_lmp(
                location_type,
                verbose,
            )
        else:
            raise NotSupported(
                f"Location type {location_type} is not supported for SPP",
            )

        df["Market"] = market.value
        df["Location Type"] = SPP._get_location_type_name(location_type)

        return SPP._finalize_spp_df(df, locations)

    def _get_feature_data(self, base_url, verbose=False):
        """Fetches data from ArcGIS Map Service with Feature Data

        Returns:
            pd.DataFrame of features
        """
        args = (
            ("f", "json"),
            ("where", "OBJECTID IS NOT NULL"),
            ("returnGeometry", "false"),
            (
                "outFields",
                "*",
            ),
        )
        url = utils.url_with_query_args(base_url, args)
        if verbose:
            print(f"Fetching feature data from {url}", file=sys.stderr)
        doc = self._get_json(url)
        df = pd.DataFrame([feature["attributes"] for feature in doc["features"]])
        return df

    @staticmethod
    def _get_rtm5_url(location_type):
        if location_type == LOCATION_TYPE_HUB:
            return QUERY_RTM5_HUBS_URL
        elif location_type == LOCATION_TYPE_INTERFACE:
            return QUERY_RTM5_INTERFACES_URL
        else:
            raise NotSupported(
                f"Location type {location_type} is not supported for Real-Time Market",
            )

    @staticmethod
    def _get_dam_delta_url(location_type):
        if location_type == LOCATION_TYPE_HUB:
            return QUERY_DAM_DELTA_HUBS_URL
        elif location_type == LOCATION_TYPE_INTERFACE:
            return QUERY_DAM_DELTA_INTERFACES_URL
        else:
            raise NotSupported(
                f"Location type {location_type} is not supported for Day-Ahead Market Delta",
            )

    def _get_latest_rtm5_lmp(
        self,
        location_type: str = LOCATION_TYPE_HUB,
        verbose=False,
    ):
        """Fetch latest real-time market data (updated every 5 minutes)"""
        df = self._get_feature_data(SPP._get_rtm5_url(location_type), verbose=verbose)
        df["Location"] = df["SETTLEMENT_LOCATION"]
        df["Time"] = SPP._parse_gmt_interval_end(
            df,
            pd.Timedelta(minutes=5),
            self.default_timezone,
        )
        return df

    def _get_latest_dam_lmp(
        self,
        location_type: str = LOCATION_TYPE_HUB,
        verbose=False,
    ):
        """Calculate the Day-Ahead Market with real-time and day-ahead market delta data

        DELTA = RTM - DAM
        => DAM = RTM - DELTA
        """
        rtm_df = self._get_feature_data(
            SPP._get_rtm5_url(location_type),
            verbose=verbose,
        )
        dam_delta_df = self._get_feature_data(
            SPP._get_dam_delta_url(location_type),
            verbose=verbose,
        )
        df = pd.merge(
            rtm_df,
            dam_delta_df,
            on=["OBJECTID"],
            suffixes=["_DAM_DELTA", "_RTM"],
        )

        df["LMP"] = df["LMP"] - df["SL_LMP_DELTA"]
        df["MLC"] = df["MLC"] - df["SL_MLC_DELTA"]
        df["MCC"] = df["MCC"] - df["SL_MCC_DELTA"]
        df["MEC"] = df["MEC"] - df["SL_MEC_DELTA"]

        df["Time"] = SPP._parse_day_ahead_hour_end(df, self.default_timezone)
        df["Location"] = df["SETTLEMENT_LOCATION_RTM"]
        return df

    @staticmethod
    def _finalize_spp_df(df, locations):
        """
        Finalizes DataFrame by:
        - filtering by locations list
        - renaming and ordering columns
        - and resetting the index

        Parameters:
            df (DataFrame): DataFrame with SPP data
            locations (list): list of locations to filter by
        """
        df = df.rename(
            columns={
                "LMP": "LMP",  # for posterity
                "MLC": "Loss",
                "MCC": "Congestion",
                "MEC": "Energy",
            },
        )
        df = utils.filter_lmp_locations(df, locations)
        df = df[
            [
                "Time",
                "Market",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
        ]
        df = df.reset_index(drop=True)
        return df

    @staticmethod
    def _parse_gmt_interval_end(df, interval_duration: pd.Timedelta, timezone):
        return df["GMTINTERVALEND"].apply(
            lambda x: (
                pd.Timestamp(x, unit="ms", tz="UTC") - interval_duration
            ).tz_convert(timezone),
        )

    @staticmethod
    def _parse_day_ahead_hour_end(df, timezone):
        # 'DA_HOUREND': '12/26/2022 9:00:00 AM',
        return df["DA_HOUREND"].apply(
            lambda x: (pd.Timestamp(x, tz=timezone) - pd.Timedelta(hours=1)),
        )

    @staticmethod
    def _normalize_location_type(location_type):
        norm_location_type = location_type.upper()
        if norm_location_type in (LOCATION_TYPE_HUB, LOCATION_TYPE_INTERFACE):
            return norm_location_type
        else:
            raise NotSupported(f"Invalid location_type {location_type}")

    @staticmethod
    def _get_location_type_name(location_type):
        if location_type == LOCATION_TYPE_HUB:
            return "Hub"
        elif location_type == LOCATION_TYPE_INTERFACE:
            return "Interface"
        else:
            raise ValueError(f"Invalid location_type: {location_type}")


# historical generation mix
# https://marketplace.spp.org/pages/generation-mix-rolling-365
# https://marketplace.spp.org/chart-api/gen-mix-365/asFile
# 15mb file with five minute resolution
