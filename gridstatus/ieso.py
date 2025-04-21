import datetime
import http.client
import os
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from typing import Literal
from urllib.error import HTTPError

import pandas as pd
import requests
import xmltodict

from gridstatus import utils
from gridstatus.base import ISOBase, NotSupported
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import logger

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CERTIFICATES_CHAIN_FILE = os.path.join(
    CURRENT_DIR,
    "public_certificates/ieso/intermediate_and_root.pem",
)

"""LOAD CONSTANTS"""
# Load hourly files go back 30 days
MAXIMUM_DAYS_IN_PAST_FOR_LOAD = 30
LOAD_INDEX_URL = "https://reports-public.ieso.ca/public/RealtimeConstTotals"

# Each load file covers one hour. We have to use the xml instead of the csv because
# the csv does not have demand for Ontario.
LOAD_TEMPLATE_URL = f"{LOAD_INDEX_URL}/PUB_RealtimeConstTotals_YYYYMMDDHH.xml"


"""LOAD FORECAST CONSTANTS"""
# There's only one load forecast for Ontario. This data covers from 5 days ago
# through tomorrow
LOAD_FORECAST_URL = (
    "https://www.ieso.ca/-/media/Files/IESO/Power-Data/Ontario-Demand-multiday.ashx"
)

"""ZONAL LOAD FORECAST CONSTANTS"""
ZONAL_LOAD_FORECAST_INDEX_URL = (
    "https://reports-public.ieso.ca/public/OntarioZonalDemand"
)

# Each forecast file contains data from the day in the filename going forward for
# 34 days. The most recent file does not have a date in the filename.
ZONAL_LOAD_FORECAST_TEMPLATE_URL = (
    f"{ZONAL_LOAD_FORECAST_INDEX_URL}/PUB_OntarioZonalDemand_YYYYMMDD.xml"
)

# The farthest in the past that forecast files are available
MAXIMUM_DAYS_IN_PAST_FOR_ZONAL_LOAD_FORECAST = 90
# The farthest in the future that forecasts are available. Note that there are not
# files for these future forecasts, they are in the current day's file.
MAXIMUM_DAYS_IN_FUTURE_FOR_ZONAL_LOAD_FORECAST = 34

"""REAL TIME FUEL MIX CONSTANTS"""
FUEL_MIX_INDEX_URL = "https://reports-public.ieso.ca/public/GenOutputCapability/"

# Updated every hour and each file has data for one day.
# The most recent version does not have the date in the filename.
FUEL_MIX_TEMPLATE_URL = f"{FUEL_MIX_INDEX_URL}/PUB_GenOutputCapability_YYYYMMDD.xml"

# Number of past days for which the complete generator report is available.
# Before this date, only total by fuel type is available.
MAXIMUM_DAYS_IN_PAST_FOR_COMPLETE_GENERATOR_REPORT = 90

"""HISTORICAL FUEL MIX CONSTANTS"""
HISTORICAL_FUEL_MIX_INDEX_URL = (
    "https://reports-public.ieso.ca/public/GenOutputbyFuelHourly/"
)

# Updated once a day and each file contains data for an entire year.
HISTORICAL_FUEL_MIX_TEMPLATE_URL = (
    f"{HISTORICAL_FUEL_MIX_INDEX_URL}/PUB_GenOutputbyFuelHourly_YYYY.xml"
)


MINUTES_INTERVAL = 5
HOUR_INTERVAL = 1

# Default namespace used in the XML files
NAMESPACES_FOR_XML = {"": "http://www.ieso.ca/schema"}

# Maps abbreviations used in the MCP real time data to the full names
# used in the historical data
IESO_ZONE_MAPPING = {
    "MBSI": "Manitoba",
    "PQSK": "Manitoba SK",
    "MISI": "Michigan",
    "MNSI": "Minnesota",
    "NYSI": "New-York",
    "ONZN": "Ontario",
    "PQAT": "Quebec AT",
    "PQBE": "Quebec B5D.B31L",
    "PQDZ": "Quebec D4Z",
    "PQDA": "Quebec D5A",
    "PQHZ": "Quebec H4Z",
    "PQHA": "Quebec H9A",
    "PQPC": "Quebec P33C",
    "PQQC": "Quebec Q4C",
    "PQXY": "Quebec X2Y",
}


class SurplusState(str, Enum):
    """Enum for surplus baseload generation states.

    The state is determined by the Action field in the report:
    - No Action (empty/null) -> No Surplus (white)
    - Other -> Managed with exports/curtailments/VG dispatch (green)
    - Manoeuvre -> Potential to dispatch nuclear units (yellow)
    - Shutdown -> Potential to shutdown nuclear units (red)
    """

    NO_SURPLUS = "No Surplus"
    MANAGED_WITH_EXPORTS = "Managed with Exports"
    NUCLEAR_DISPATCH = "Nuclear Dispatch"
    NUCLEAR_SHUTDOWN = "Nuclear Shutdown"


class IESO(ISOBase):
    """Independent Electricity System Operator (IESO)"""

    name = "Independent Electricity System Operator"
    iso_id = "ieso"

    # All data is provided in EST, and does not change with DST. This means there are
    # no repeated or missing hours in the raw data and we can safely use tz_localize
    # without setting ambiguous or nonexistent times.
    # https://www.ieso.ca/-/media/Files/IESO/Document-Library/engage/ca/ca-Introduction-to-the-Capacity-Auction.ashx
    default_timezone = "EST"

    status_homepage = "https://www.ieso.ca/en/Power-Data"

    @support_date_range(frequency="HOUR_START")
    def get_load(
        self,
        date: str | datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime | None = None,
        verbose: bool = False,
    ):
        """
        Get 5-minute load for the Market and Ontario for a given date or from
        date to end date.

        Args:
            date (datetime.date | datetime.datetime | str): The date to get the load for
                Can be a `datetime.date` or `datetime.datetime` object, or a string
                with the values "today" or "latest". If `end` is None, returns
                only data for this date.
            end (datetime.date | datetime.datetime, optional): End date. Defaults None
                If provided, returns data from `date` to `end` date. The `end` can be a
                `datetime.date` or `datetime.datetime` object.
            verbose (bool, optional): Print verbose output. Defaults to False.
            frequency (str, optional): Frequency of data. Defaults to "5min".

        Returns:
            pd.DataFrame: zonal load as a wide table with columns for each zone
        """
        today = utils._handle_date("today", tz=self.default_timezone)

        if date != "latest":
            if date.date() > today.date():
                raise NotSupported("Load data is not available for future dates.")

            if date.date() < today.date() - pd.Timedelta(
                days=MAXIMUM_DAYS_IN_PAST_FOR_LOAD,
            ):
                raise NotSupported(
                    f"Load data is not available for dates more than "
                    f"{MAXIMUM_DAYS_IN_PAST_FOR_LOAD} days in the past.",
                )

            # Return an empty dataframe when the date exceeds the current timestamp
            # since there's no load available yet.
            if date > pd.Timestamp.now(tz=self.default_timezone):
                return pd.DataFrame()
        elif date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone)

        df = self._retrieve_5_minute_load(date, end, verbose)

        cols_to_keep = [
            "Interval Start",
            "Interval End",
            "Market Total Load",
            "Ontario Load",
        ]

        df["Market Total Load"] = df["Market Total Load"].astype(float)
        df["Ontario Load"] = df["Ontario Load"].astype(float)

        return utils.move_cols_to_front(df, cols_to_keep)[cols_to_keep].reset_index(
            drop=True,
        )

    def _retrieve_5_minute_load(
        self,
        date: datetime.datetime,
        end: datetime.datetime | None = None,
        verbose: bool = False,
    ):
        # We have to add 1 to the hour to get the file because the filename with
        # hour x contains data for hour x-1. For example, to get data for
        # 9:00 - 9:55, we need to request the file for hour 10.
        # The hour should be in the range 1-24
        hour = date.hour + 1

        url = LOAD_TEMPLATE_URL.replace(
            "YYYYMMDDHH",
            f"{(date).strftime('%Y%m%d')}{hour:02d}",
        )

        r = self._request(url, verbose)

        root = ET.fromstring(r.text)

        # Extracting all triples of Interval, Market Total Load, and Ontario Load
        interval_loads_and_demands = self._find_loads_at_each_interval_from_xml(root)

        df = pd.DataFrame(
            interval_loads_and_demands,
            columns=["Interval", "Market Total Load", "Ontario Load"],
        )

        delivery_date = root.find("DocBody/DeliveryDate", NAMESPACES_FOR_XML).text
        delivery_hour = int(root.find("DocBody/DeliveryHour", NAMESPACES_FOR_XML).text)

        df["Delivery Date"] = pd.Timestamp(delivery_date, tz=self.default_timezone)

        # The starting hour is 1, so we subtract 1 to get the hour in the range 0-23
        df["Delivery Hour Start"] = delivery_hour - 1
        # Multiply the interval minus 1 by 5 to get the minutes in the range 0-55
        df["Interval Minute Start"] = MINUTES_INTERVAL * (df["Interval"] - 1)

        df["Interval Start"] = (
            df["Delivery Date"]
            + pd.to_timedelta(df["Delivery Hour Start"], unit="h")
            + pd.to_timedelta(df["Interval Minute Start"], unit="m")
        )

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(
            minutes=MINUTES_INTERVAL,
        )

        if end:
            return df[df["Interval End"] <= pd.Timestamp(end)]

        return df

    def get_load_forecast(self, date: str, verbose: bool = False):
        """
        Get forecasted load for Ontario. Supports only "latest" and "today" because
        there is only one load forecast.

        Args:
            date (str): Either "today" or "latest"
            verbose (bool, optional): Print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: Ontario load forecast
        """
        if date not in ["today", "latest"]:
            raise NotSupported(
                "Only 'today' and 'latest' are supported for load forecasts.",
            )

        root = ET.fromstring(self._request(LOAD_FORECAST_URL, verbose).text)

        # Extract values from <DataSet Series="Projected">
        projected_values = []

        # Iterate through the XML to find the DataSet with Series="Projected"
        for dataset in root.iter("DataSet"):
            if dataset.attrib.get("Series") == "Projected":
                for data in dataset.iter("Data"):
                    for value in data.iter("Value"):
                        projected_values.append(value.text)

        created_at = pd.Timestamp(
            root.find(".//CreatedAt").text,
            tz=self.default_timezone,
        )
        start_date = pd.Timestamp(
            root.find(".//StartDate").text,
            tz=self.default_timezone,
        )

        # Create the range of interval starts based on the number of values at an
        # hourly frequency
        interval_starts = pd.date_range(
            start_date,
            periods=len(projected_values),
            freq="h",
            tz=self.default_timezone,
        )

        # Create a DataFrame with the projected values
        df_projected = pd.DataFrame(projected_values, columns=["Ontario Load Forecast"])
        df_projected["Ontario Load Forecast"] = df_projected[
            "Ontario Load Forecast"
        ].astype(float)
        df_projected["Publish Time"] = created_at
        df_projected["Interval Start"] = interval_starts
        df_projected["Interval End"] = df_projected["Interval Start"] + pd.Timedelta(
            hours=HOUR_INTERVAL,
        )

        return utils.move_cols_to_front(
            df_projected,
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Ontario Load Forecast",
            ],
        )

    @support_date_range(frequency="DAY_START")
    def get_zonal_load_forecast(
        self,
        date: str | datetime.date | tuple[datetime.date, datetime.date],
        end: datetime.date | datetime.datetime | None = None,
        verbose: bool = False,
    ):
        """
        Get forecasted load by forecast zone (Ontario, East, West) for a given date
        or from date to end date. This method supports future dates.

        Supports data 90 days into the past and up to 34 days into the future.

        Args:
            date (datetime.date | datetime.datetime | str): The date to get the load for
                Can be a `datetime.date` or `datetime.datetime` object, or a string
                with the values "today" or "latest". If `end` is None, returns
                only data for this date.
            end (datetime.date | datetime.datetime, optional): End date. Defaults None
                If provided, returns data from `date` to `end` date. The `end` can be a
                `datetime.date` or `datetime.datetime` object.
            verbose (bool, optional): Print verbose output. Defaults to False.


        Returns:
            pd.DataFrame: forecasted load as a wide table with columns for each zone
        """

        today = utils._handle_date("today", tz=self.default_timezone)

        if date != "latest":
            date = utils._handle_date(date, tz=self.default_timezone)

            if date.date() < today.date() - pd.Timedelta(
                days=MAXIMUM_DAYS_IN_PAST_FOR_ZONAL_LOAD_FORECAST,
            ):
                # Forecasts are not support for past dates
                raise NotSupported(
                    "Past dates are not support for load forecasts more than "
                    f"{MAXIMUM_DAYS_IN_PAST_FOR_ZONAL_LOAD_FORECAST} days in the past.",
                )

            if date.date() > today.date() + pd.Timedelta(
                days=MAXIMUM_DAYS_IN_FUTURE_FOR_ZONAL_LOAD_FORECAST,
            ):
                raise NotSupported(
                    f"Dates more than {MAXIMUM_DAYS_IN_FUTURE_FOR_ZONAL_LOAD_FORECAST} "
                    "days in the future are not supported for load forecasts.",
                )

        # For future dates, the most recent forecast is used
        if date == "latest" or date.date() > today.date():
            url = ZONAL_LOAD_FORECAST_TEMPLATE_URL.replace("_YYYYMMDD", "")
        else:
            url = ZONAL_LOAD_FORECAST_TEMPLATE_URL.replace(
                "YYYYMMDD",
                date.strftime("%Y%m%d"),
            )

        r = self._request(url, verbose)

        # Initialize a list to store the parsed data
        data = []

        # Parse the XML file
        root = ET.fromstring(r.content)

        published_time = root.find(".//CreatedAt", NAMESPACES_FOR_XML).text

        # Extracting data for each ZonalDemands within the Document
        for zonal_demands in root.findall(".//ZonalDemands", NAMESPACES_FOR_XML):
            delivery_date = zonal_demands.find(
                ".//DeliveryDate",
                NAMESPACES_FOR_XML,
            ).text

            for zonal_demand in zonal_demands.findall(
                ".//ZonalDemand/*",
                NAMESPACES_FOR_XML,
            ):
                # The zone name is the tag name without the namespace
                zone_name = zonal_demand.tag[(zonal_demand.tag.rfind("}") + 1) :]

                for demand in zonal_demand.findall(".//Demand", NAMESPACES_FOR_XML):
                    hour = demand.find(".//DeliveryHour", NAMESPACES_FOR_XML).text
                    energy_mw = demand.find(".//EnergyMW", NAMESPACES_FOR_XML).text

                    data.append(
                        {
                            "DeliveryDate": delivery_date,
                            "Zone": zone_name,
                            "DeliveryHour": hour,
                            "EnergyMW": energy_mw,
                        },
                    )

        df = pd.DataFrame(data)

        # Convert columns to appropriate data types
        df["DeliveryHour"] = df["DeliveryHour"].astype(int)
        df["EnergyMW"] = df["EnergyMW"].astype(float)
        df["DeliveryDate"] = pd.to_datetime(df["DeliveryDate"])

        df["Interval Start"] = (
            # Need to subtract 1 from the DeliveryHour since that represents the
            # ending hour of the interval. (1 represents 00:00 - 01:00)
            df["DeliveryDate"] + pd.to_timedelta(df["DeliveryHour"] - 1, unit="h")
        ).dt.tz_localize(self.default_timezone)

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=HOUR_INTERVAL)

        # Pivot the table to wide
        pivot_df = df.pivot_table(
            index=["Interval Start", "Interval End"],
            columns="Zone",
            values="EnergyMW",
            aggfunc="first",
        ).reset_index()

        pivot_df["Publish Time"] = pd.Timestamp(
            published_time,
            tz=self.default_timezone,
        )

        pivot_df = utils.move_cols_to_front(
            pivot_df,
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Ontario",
            ],
        )

        pivot_df.columns.name = None

        col_mapper = {
            col: f"{col} Load Forecast" for col in ["Ontario", "East", "West"]
        }

        pivot_df = pivot_df.rename(columns=col_mapper)

        # Return all the values from the latest forecast
        if date == "latest":
            return pivot_df

        # If no end is provided, return data from single date
        if not end:
            return pivot_df[pivot_df["Publish Time"].dt.date == date.date()]

        # Return data from date to end date
        end_date = utils._handle_date(end, tz=self.default_timezone)

        return pivot_df[
            (pivot_df["Publish Time"] >= date) & (pivot_df["Publish Time"] <= end_date)
        ]

    def get_fuel_mix(
        self,
        date: str | datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime | None = None,
        verbose: bool = False,
    ):
        """
        Hourly output and capability for each fuel type (summed over all generators)
        for a given date or from date to end. Variable generators (solar and wind)
        have a forecast.

        Args:
            date (datetime.date | datetime.datetime | str): The date to get the load for
                Can be a `datetime.date` or `datetime.datetime` object, or a string
                with the values "today" or "latest". If `end` is None, returns
                only data for this date.
            end (datetime.date | datetime.datetime, optional): End date. Defaults None
                If provided, returns data from `date` to `end` date. The `end` can be a
                `datetime.date` or `datetime.datetime` object.
            verbose (bool, optional): Print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: fuel mix
        """
        # Required because this method is not decorated with support_date_range
        if isinstance(date, tuple):
            date, end = date

        # Boolean for whether to use the historical fuel mix data
        use_historical = False

        if date != "latest":
            today = utils._handle_date("today", tz=self.default_timezone)
            date = utils._handle_date(date, tz=self.default_timezone)

            if date.date() < today.date() - pd.Timedelta(
                days=MAXIMUM_DAYS_IN_PAST_FOR_COMPLETE_GENERATOR_REPORT,
            ):
                use_historical = True
            elif date.date() > today.date():
                raise NotSupported("Fuel mix data is not available for future dates.")

        if use_historical:
            data = self._retrieve_historical_fuel_mix(date, end, verbose)
        else:
            data = (
                self._retrieve_fuel_mix(date, end, verbose)
                .groupby(["Fuel Type", "Interval Start", "Interval End"])
                .sum(numeric_only=True)
                .reset_index()
            )

            pivoted = data.pivot_table(
                index=["Interval Start", "Interval End"],
                columns="Fuel Type",
                values="Output MW",
            ).reset_index()

            pivoted.columns = [c.title() for c in pivoted.columns]
            pivoted.index.name = None

            data = pivoted.copy()

        # Older data does not have the other fuel type
        if "Other" not in data.columns:
            data["Other"] = pd.NA

        data = utils.move_cols_to_front(
            data,
            [
                "Interval Start",
                "Interval End",
                "Biofuel",
                "Gas",
                "Hydro",
                "Nuclear",
                "Solar",
                "Wind",
            ],
        )

        if end:
            end = utils._handle_date(end, tz=self.default_timezone)

            return data[
                (data["Interval Start"] >= date) & (data["Interval Start"] <= end)
            ].reset_index(drop=True)

        elif date == "latest":
            return data

        return data[data["Interval Start"] >= date].reset_index(drop=True)

    def get_generator_report_hourly(
        self,
        date: str | datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime | None = None,
        verbose: bool = False,
    ):
        """
        Hourly output for each generator for a given date or from date to end.
        Variable generators (solar and wind) have a forecast and available capacity.
        Non-variable generators have a capability.

        Args:
            date (datetime.date | datetime.datetime | str): The date to get the load for
                Can be a `datetime.date` or `datetime.datetime` object, or a string
                with the values "today" or "latest". If `end` is None, returns
                only data for this date.
            end (datetime.date | datetime.datetime, optional): End date. Defaults None
                If provided, returns data from `date` to `end` date. The `end` can be a
                `datetime.date` or `datetime.datetime` object.
            verbose (bool, optional): Print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: generator output and capability/available capacity
        """
        # Required because this method is not decorated with support_date_range
        if isinstance(date, tuple):
            date, end = date

        if date != "latest":
            today = utils._handle_date("today", tz=self.default_timezone)
            date = utils._handle_date(date, tz=self.default_timezone)

            if date.date() < today.date() - pd.Timedelta(
                days=MAXIMUM_DAYS_IN_PAST_FOR_COMPLETE_GENERATOR_REPORT,
            ):
                raise NotSupported(
                    f"Generator output and capability data is not available for dates "
                    f"more than {MAXIMUM_DAYS_IN_PAST_FOR_COMPLETE_GENERATOR_REPORT} "
                    "days in the past.",
                )
            elif date.date() > today.date():
                raise NotSupported(
                    "Generator output and capability data is not available for future "
                    "dates.",
                )

        data = self._retrieve_fuel_mix(date, end, verbose)

        data = utils.move_cols_to_front(
            data,
            [
                "Interval Start",
                "Interval End",
                "Generator Name",
                "Fuel Type",
                "Output MW",
                "Capability MW",
                "Available Capacity MW",
                "Forecast MW",
            ],
        ).sort_values(["Interval Start", "Fuel Type", "Generator Name"])

        if end:
            end = utils._handle_date(end, tz=self.default_timezone)

            return data[
                (data["Interval Start"] >= date) & (data["Interval Start"] <= end)
            ].reset_index(drop=True)

        if date == "latest":
            return data.reset_index(drop=True)

        return data[data["Interval Start"] >= date].reset_index(drop=True)

    @support_date_range(frequency="DAY_START")
    def _retrieve_fuel_mix(
        self,
        date: (
            str
            | datetime.date
            | datetime.datetime
            | tuple[datetime.date, datetime.date]
        ),
        end: datetime.date | datetime.datetime | None = None,
        verbose: bool = False,
    ):
        """Retrieve fuel mix data for a given date or date range.

            date (str | date | datetime | tuple[date, date]): The date or date range
                to retrieve fuel mix data for.
            end (date | datetime | None, optional): The end date of the date range.
                Defaults to None.
            verbose (bool, optional): Whether to print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: Fuel mix data
        """
        url = FUEL_MIX_TEMPLATE_URL.replace(
            "_YYYYMMDD",
            date.strftime("_%Y%m%d") if date != "latest" else "",
        )

        r = self._request(url, verbose)

        root = ET.fromstring(r.content)

        # Define the namespace map. This is different than all the other XML files
        ns = {"": "http://www.theIMO.com/schema"}

        date = root.find(".//Date", ns).text

        data = []

        for gen in root.findall(".//Generator", ns):
            generator_name = gen.find("GeneratorName", ns).text
            fuel_type = gen.find("FuelType", ns).text

            for output in gen.findall("Outputs/Output", ns):
                hour = output.find("Hour", ns).text
                energy_mw = (
                    output.find("EnergyMW", ns).text
                    if output.find(
                        "EnergyMW",
                        ns,
                    )
                    is not None
                    else None
                )

                # For SOLAR/WIND, the forecast is stored under the capability and these
                # Fuel types have an available capacity. See the schema definition:
                # http://reports.ieso.ca/docrefs/schema/GenOutputCapability_r3.xsd
                # There is no capability for these generators.
                if fuel_type in ["SOLAR", "WIND"]:
                    forecast_mw = (
                        gen.find(f".//Capabilities/Capability[Hour='{hour}']", ns)
                        .find("EnergyMW", ns)
                        .text
                    )

                    available_capacity_mw = (
                        gen.find(
                            f".//Capacities/AvailCapacity[Hour='{hour}']",
                            ns,
                        )
                        .find("EnergyMW", ns)
                        .text
                    )

                    capability_mw = None

                # For non-SOLAR/WIND, there is no forecast or available capacity.
                # Instead, there is a capability.
                else:
                    forecast_mw = None

                    capability_mw = (
                        gen.find(
                            f".//Capabilities/Capability[Hour='{hour}']",
                            ns,
                        )
                        .find("EnergyMW", ns)
                        .text
                    )

                    available_capacity_mw = None

                data.append(
                    [
                        date,
                        hour,
                        generator_name,
                        fuel_type,
                        energy_mw,
                        capability_mw,
                        available_capacity_mw,
                        forecast_mw,
                    ],
                )

        columns = [
            "Date",
            "Hour",
            "Generator Name",
            "Fuel Type",
            "Output MW",
            "Capability MW",
            "Available Capacity MW",
            "Forecast MW",
        ]

        # Creating the DataFrame with the correct date
        df = pd.DataFrame(data, columns=columns)
        df["Interval Start"] = (
            pd.to_datetime(df["Date"])
            + pd.to_timedelta(
                # Subtract 1 from the hour because hour 1 is from 00:00 - 01:00
                df["Hour"].astype(int) - 1,
                unit="h",
            )
        ).dt.tz_localize(self.default_timezone)

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)

        float_cols = [
            "Output MW",
            "Capability MW",
            "Available Capacity MW",
            "Forecast MW",
        ]

        df[float_cols] = df[float_cols].astype(float)

        return df.drop(columns=["Date", "Hour"])

    @support_date_range(frequency="YEAR_START")
    def _retrieve_historical_fuel_mix(
        self,
        date: (
            str
            | datetime.date
            | datetime.datetime
            | tuple[datetime.date, datetime.date]
        ),
        end: datetime.date | datetime.datetime | None = None,
        verbose: bool = False,
    ):
        date = utils._handle_date(date, tz=self.default_timezone)

        url = HISTORICAL_FUEL_MIX_TEMPLATE_URL.replace(
            "YYYY",
            str(date.year),
        )

        r = self._request(url, verbose)

        root = ET.fromstring(r.content)
        ns = NAMESPACES_FOR_XML
        data = []

        # Iterate through each day
        for day_data in root.findall(".//DailyData", ns):
            date = (
                day_data.find("Day", ns).text
                if day_data.find("Day", ns) is not None
                else None
            )

            # Iterate through each hour of the day
            for hourly_data in day_data.findall("HourlyData", ns):
                hour = (
                    hourly_data.find("Hour", ns).text
                    if hourly_data.find("Hour", ns) is not None
                    else None
                )

                # Initialize fuel type outputs
                fuel_outputs = {
                    "NUCLEAR": 0,
                    "GAS": 0,
                    "HYDRO": 0,
                    "WIND": 0,
                    "SOLAR": 0,
                    "BIOFUEL": 0,
                }

                # Extracting output for each fuel type
                for fuel_total in hourly_data.findall("FuelTotal", ns):
                    fuel_type = (
                        fuel_total.find("Fuel", ns).text
                        if fuel_total.find("Fuel", ns) is not None
                        else None
                    )
                    output = (
                        fuel_total.find(".//Output", ns).text
                        if fuel_total.find(".//Output", ns) is not None
                        else 0
                    )

                    if fuel_type in fuel_outputs:
                        fuel_outputs[fuel_type] = float(output)

                # Adding the row to the data list
                row = [date, hour] + list(fuel_outputs.values())
                data.append(row)

        columns = ["Date", "Hour"] + list(fuel_outputs.keys())
        columns = [c.title() for c in columns]

        # Creating the DataFrame with the adjusted parsing logic
        df = pd.DataFrame(data, columns=columns)
        df["Interval Start"] = (
            pd.to_datetime(df["Date"])
            + pd.to_timedelta(
                # Subtract 1 from the hour because hour 1 is from 00:00 - 01:00
                df["Hour"].astype(int) - 1,
                unit="h",
            )
        ).dt.tz_localize(self.default_timezone)

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)

        return utils.move_cols_to_front(
            df,
            [
                "Interval Start",
                "Interval End",
                "Nuclear",
                "Gas",
                "Hydro",
                "Wind",
                "Solar",
                "Biofuel",
            ],
        ).drop(columns=["Date", "Hour"])

    # Function to extract data for a specific Market Quantity considering namespace
    def _extract_load_in_market_quantity(
        self,
        market_quantity_element: ET.Element,
        market_quantity_name: str,
    ):
        for mq in market_quantity_element.findall("MQ", NAMESPACES_FOR_XML):
            market_quantity = mq.find("MarketQuantity", NAMESPACES_FOR_XML).text

            if market_quantity_name in market_quantity:
                return mq.find("EnergyMW", NAMESPACES_FOR_XML).text

        return None

    # Function to find all triples of 'Interval', 'Market Total Load', and
    # 'Ontario Load' in the XML file
    def _find_loads_at_each_interval_from_xml(self, root_element: ET.Element):
        interval_load_demand_triples = []

        for interval_energy in root_element.findall(
            "DocBody/Energies/IntervalEnergy",
            NAMESPACES_FOR_XML,
        ):
            interval = interval_energy.find("Interval", NAMESPACES_FOR_XML).text
            market_total_load = self._extract_load_in_market_quantity(
                interval_energy,
                "Total Energy",
            )
            ontario_load = self._extract_load_in_market_quantity(
                interval_energy,
                "ONTARIO DEMAND",
            )

            if market_total_load and ontario_load:
                interval_load_demand_triples.append(
                    [int(interval), float(market_total_load), float(ontario_load)],
                )

        return interval_load_demand_triples

    @support_date_range(frequency="HOUR_START")
    def get_mcp_real_time_5_min(
        self,
        date: str | datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime | None = None,
        verbose: bool = False,
    ):
        # This file always has the latest data for the current hour
        if date == "latest":
            url = "https://reports-public.ieso.ca/public/RealtimeMktPrice/PUB_RealtimeMktPrice.csv"  # noqa: E501
        else:
            hour = date.hour
            # The report has the hour ending in the filename so we need to add 1 to
            # get the file for the hour ending
            url = f"https://reports-public.ieso.ca/public/RealtimeMktPrice/PUB_RealtimeMktPrice_{date.strftime('%Y%m%d')}{hour + 1:02d}.csv"  # noqa: E501

        try:
            data = pd.read_csv(
                url,
                skiprows=4,
                header=None,
                usecols=[0, 1, 2, 3, 4],
                names=["Hour Ending", "Interval", "Component", "Location", "Price"],
            )
        except HTTPError as e:
            if e.code == 404:
                raise NotSupported(
                    f"MCP data is not available for the requested date {date}. Try using the historical method.",  # noqa: E501
                )

        data["Delivery Date"] = date.date()
        data["Location"] = data["Location"].map(IESO_ZONE_MAPPING)

        return self._handle_mcp_data(data)

    @support_date_range(frequency="YEAR_START")
    def get_mcp_historical_5_min(
        self,
        date: str | datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime | None = None,
        verbose: bool = False,
    ):
        url = f"https://reports-public.ieso.ca/public/RealtimeMktPriceYear/PUB_RealtimeMktPriceYear_{date.year}.csv"

        raw_data = pd.read_csv(url, skiprows=3, header=[0, 1])

        # Columns are multi-level
        data = raw_data.melt(
            id_vars=[
                ("Unnamed: 0_level_0", "DELIVERY_DATE"),
                ("Unnamed: 1_level_0", "DELIVERY_HOUR"),
                ("Unnamed: 2_level_0", "INTERVAL"),
            ],
        )

        data.columns = [
            "Delivery Date",
            "Hour Ending",
            "Interval",
            "Location",
            "Component",
            "Price",
        ]

        return self._handle_mcp_data(data)

    def _handle_mcp_data(self, data: pd.DataFrame) -> pd.DataFrame:
        data["Interval End"] = (
            pd.to_datetime(data["Delivery Date"])
            + pd.to_timedelta(data["Hour Ending"] - 1, unit="h")
            # Each interval is 5 minutes
            + (5 * pd.to_timedelta(data["Interval"], unit="m"))
        ).dt.tz_localize(self.default_timezone)

        data["Interval Start"] = data["Interval End"] - pd.Timedelta(minutes=5)

        # Pivot so each component is a column
        data = data.pivot_table(
            index=["Interval Start", "Interval End", "Location"],
            columns="Component",
            values="Price",
        ).reset_index()

        data = data.rename(
            columns={
                "10N": "Non-sync 10 Min",
                "10S": "Sync 10 Min",
                "30R": "Reserves 30 Min",
                "ENGY": "Energy",
            },
        )

        data = data[
            [
                "Interval Start",
                "Interval End",
                "Location",
                "Non-sync 10 Min",
                "Sync 10 Min",
                "Reserves 30 Min",
                "Energy",
            ]
        ]

        return data.sort_values(["Interval Start", "Location"])

    @support_date_range(frequency="DAY_START")
    def get_hoep_real_time_hourly(
        self,
        date: str | datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            return self.get_hoep_real_time_hourly("today", verbose=verbose)

        if utils.is_today(date, tz=self.default_timezone):
            # This file always contains the most recent file for today
            url = "https://reports-public.ieso.ca/public/DispUnconsHOEP/PUB_DispUnconsHOEP.csv"  # noqa: E501
        else:
            # The most recent file for a give date does not have a version number
            url = f"https://reports-public.ieso.ca/public/DispUnconsHOEP/PUB_DispUnconsHOEP_{date.strftime('%Y%m%d')}.csv"  # noqa: E501

        # Data is only available for a limited number of days through this method
        try:
            data = pd.read_csv(
                url,
                skiprows=4,
                usecols=[0, 1],
                header=None,
                names=["Hour Ending", "HOEP"],
            )
        except HTTPError as e:
            if e.code == 404:
                raise NotSupported(
                    f"HOEP data is not available for the requested date {date}. Try using the historical method.",  # noqa: E501
                )
            raise

        data["Interval End"] = (
            date.normalize().tz_localize(None)
            + pd.to_timedelta(data["Hour Ending"].astype(int), unit="h")
        ).dt.tz_localize(self.default_timezone)
        data["Interval Start"] = data["Interval End"] - pd.Timedelta(hours=1)

        data = data[["Interval Start", "Interval End", "HOEP"]]

        return data.sort_values("Interval Start")

    @support_date_range(frequency="YEAR_START")
    def get_hoep_historical_hourly(
        self,
        date: str | datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime | None = None,
        verbose: bool = False,
    ):
        url = f"https://reports-public.ieso.ca/public/PriceHOEPPredispOR/PUB_PriceHOEPPredispOR_{date.year}.csv"  # noqa: E501

        data = pd.read_csv(url, skiprows=1, header=2)

        data["Interval End"] = (
            pd.to_datetime(data["Date"]) + pd.to_timedelta(data["Hour"], unit="h")
        ).dt.tz_localize(self.default_timezone)

        data["Interval Start"] = data["Interval End"] - pd.Timedelta(hours=1)

        data = data[
            [
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
        ]

        return data.sort_values("Interval Start")

    def _request(self, url: str, verbose: bool = False):
        logger.info(f"Fetching URL: {url}")

        max_retries = 3
        retry_num = 0
        sleep = 5

        # This URL is missing a complete certificate chain. The browser knows how
        # to retrieve the intermediate certificates, but requests does not. Therefore,
        # we need to provide the certificate chain manually (intermediate and root).
        if "www.ieso.ca" in url:
            tls_verify = CERTIFICATES_CHAIN_FILE
        else:
            tls_verify = True

        while retry_num < max_retries:
            r = requests.get(url, verify=tls_verify)

            if r.ok:
                break

            retry_num += 1
            logger.info(f"Request failed. Error: {r.reason}. Retrying {retry_num}...")

            time.sleep(sleep)

            sleep *= 2

        if not r.ok:
            raise Exception(
                f"Failed to retrieve data from {url} in {max_retries} tries.",
            )

        return r

    @support_date_range(frequency="DAY_START")
    def get_resource_adequacy_report(
        self,
        date: str | datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime | None = None,
        vintage: Literal["all", "latest"] = "latest",
        last_modified: str | datetime.date | datetime.datetime | None = None,
    ) -> pd.DataFrame:
        """Retrieve and parse the Resource Adequacy Report for a given date.

        Args:
            date (str | datetime.date | datetime.datetime): The date for which to get the report
            end (datetime.date | datetime.datetime | None): The end date for the range of reports to get
            vintage (Literal["all", "latest"]): The version of the report to get
            last_modified (str | datetime.date | datetime.datetime | None): The last modified time after which to get report(s)

        Returns:
            pd.DataFrame: The Resource Adequacy Report df for the given date
        """
        if last_modified:
            last_modified = utils._handle_date(last_modified, tz=self.default_timezone)

        if vintage == "latest":
            json_data, file_last_modified = self._get_latest_resource_adequacy_json(
                date,
                last_modified,
            )
            df = self._parse_resource_adequacy_report(json_data)
            df["Last Modified"] = file_last_modified

        elif vintage == "all":
            json_data_with_times = self._get_all_resource_adequacy_jsons(
                date,
                last_modified,
            )
            dfs = []
            for json_data, file_last_modified in json_data_with_times:
                df = self._parse_resource_adequacy_report(json_data)
                df["Last Modified"] = file_last_modified
                dfs.append(df)
            df = pd.concat(dfs)

        df = utils.move_cols_to_front(
            df,
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Last Modified",
            ],
        )
        logger.debug(f"DataFrame Shape: {df.shape}")
        return df.sort_values(["Interval Start", "Publish Time", "Last Modified"])

    # Note(Kladar): This might be fairly generalizable to other XML reports from IESO
    def _get_latest_resource_adequacy_json(
        self,
        date: str | datetime.date | datetime.datetime,
        last_modified: pd.Timestamp | None = None,
    ) -> tuple[dict, datetime.datetime]:
        """Retrieve the Resource Adequacy Report for a given date and convert to JSON. There are often many
        files for a given date, so this function will return the file with the highest version number. It does
        not retrieve arbitrary files of lower version numbers.

        Args:
            date (str | datetime.date | datetime.datetime): The date for which to get the report
            last_modified (str | datetime.date | datetime.datetime | None): The last modified time after which to get report(s)

        Returns:
            tuple[dict, datetime.datetime]: The Resource Adequacy Report JSON and its last modified time
        """
        base_url = "https://reports-public.ieso.ca/public/Adequacy2"

        if isinstance(date, (datetime.datetime, datetime.date)):
            date_str = date.strftime("%Y%m%d")
        else:
            date_str = date.replace("-", "")

        file_prefix = f"PUB_Adequacy2_{date_str}"

        r = self._request(base_url)
        files = re.findall(f'href="({file_prefix}.*?.xml)"', r.text)
        last_modified_times = re.findall(r"(\d{2}-\w{3}-\d{4} \d{2}:\d{2})", r.text)
        files_and_times = zip(files, last_modified_times)

        if not files:
            raise FileNotFoundError(
                f"No resource adequacy files found for date {date_str}",
            )

        if last_modified:
            if last_modified.tz is None:
                last_modified = utils._handle_date(
                    last_modified,
                    tz=self.default_timezone,
                )
            filtered_files = [
                (file, time)
                for file, time in files_and_times
                if pd.Timestamp(time, tz=self.default_timezone) >= last_modified
            ]
            logger.info(
                f"Found {len(filtered_files)} files after last modified time {last_modified}",
            )
        else:
            filtered_files = list(files_and_times)

        if not filtered_files:
            raise FileNotFoundError(
                f"No files found for date {date_str} after last modified time {last_modified}",
            )

        unversioned_file = next(
            ((f, t) for f, t in filtered_files if "_v" not in f),
            None,
        )

        if unversioned_file:
            latest_file, file_time = unversioned_file
        else:
            latest_file, file_time = max(
                filtered_files,
                key=lambda x: int(x[0].split("_v")[-1].replace(".xml", "")),
            )

        logger.info(f"Latest file: {latest_file}")
        url = f"{base_url}/{latest_file}"
        r = self._request(url)
        json_data = xmltodict.parse(r.text)
        last_modified_time = pd.Timestamp(file_time, tz=self.default_timezone)

        return json_data, last_modified_time

    def _fetch_and_parse_file(self, base_url: str, file: str) -> dict:
        url = f"{base_url}/{file}"
        r = self._request(url)
        return xmltodict.parse(r.text)

    def _get_all_resource_adequacy_jsons(
        self,
        date: str | datetime.date | datetime.datetime,
        last_modified: pd.Timestamp | None = None,
    ) -> list[tuple[dict, datetime.datetime]]:
        """Retrieve all Resource Adequacy Report JSONs for a given date. There are often many
        files for a given date, so this function will return all files, the data of which may be separated
        by publish time.

        Args:
            date (str | datetime.date | datetime.datetime): The date for which to get the report
            last_modified (str | datetime.date | datetime.datetime | None): The last modified time after which to get report(s)

        Returns:
            dict: The Resource Adequacy Report JSON for the given date
        """
        base_url = "https://reports-public.ieso.ca/public/Adequacy2"

        if isinstance(date, (datetime.datetime, datetime.date)):
            date_str = date.strftime("%Y%m%d")
        else:
            date_str = date.replace("-", "")

        file_prefix = f"PUB_Adequacy2_{date_str}"

        r = self._request(base_url)

        pattern = '<a href="({}.*?.xml)">.*?</a>\\s+(\\d{{2}}-\\w{{3}}-\\d{{4}} \\d{{2}}:\\d{{2}})'
        file_rows = re.findall(pattern.format(file_prefix), r.text)

        if not file_rows:
            raise FileNotFoundError(
                f"No resource adequacy files found for date {date_str}",
            )

        if last_modified:
            if last_modified.tz is None:
                last_modified = utils._handle_date(
                    last_modified,
                    tz=self.default_timezone,
                )
            filtered_files = [
                (file, time)
                for file, time in file_rows
                if pd.Timestamp(time, tz=self.default_timezone) >= last_modified
            ]
            logger.info(
                f"Found {len(filtered_files)} files after last modified time {last_modified}",
            )
        else:
            filtered_files = file_rows

        if not filtered_files:
            raise FileNotFoundError(
                f"No files found for date {date_str} after last modified time {last_modified}",
            )

        json_data_with_times = []
        max_retries = 3
        retry_delay = 2

        with ThreadPoolExecutor(max_workers=min(10, len(filtered_files))) as executor:
            future_to_file = {
                executor.submit(self._fetch_and_parse_file, base_url, file): (
                    file,
                    time,
                )
                for file, time in filtered_files
            }

            for future in as_completed(future_to_file):
                file, time = future_to_file[future]
                retries = 0
                while retries < max_retries:
                    try:
                        logger.info(f"Processing file {file}...")
                        json_data = future.result()
                        json_data_with_times.append(
                            (json_data, pd.Timestamp(time, tz=self.default_timezone)),
                        )
                        break
                    except http.client.RemoteDisconnected as e:
                        retries += 1
                        if retries == max_retries:
                            logger.error(
                                f"Remote connection closed for file {file}: {str(e)}",
                            )
                            break
                        logger.warning(
                            f"Remote connection closed for file {file}: {str(e)}. Retrying in {retry_delay} seconds...",
                        )
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    except Exception as e:
                        logger.error(
                            f"Unexpected error processing file {file}: {str(e)}",
                        )
                        break

        return json_data_with_times

    def _parse_resource_adequacy_report(self, json_data: dict) -> pd.DataFrame:
        """Parse the Resource Adequacy Report JSON into DataFrames."""
        document_body = json_data["Document"]["DocBody"]
        report_data = []
        data_map = self._get_resource_adequacy_data_structure_map()

        # TODO(Kladar): this is clunky and could definitely be generalized to reduce
        # linecount, but it works for now. I kind of move around the report JSON to where I want
        # to extract data and then extract it, and that movement could be abstracted away
        # NOTE(kladar): suggested libraries that does this sort of thing are `dpath` and `glom` https://github.com/mahmoud/glom
        def get_nested_data(data: dict, path: list[str]) -> dict:
            """Helper function to traverse nested data using a path."""
            for key in path:
                data = data[key]
            return data

        logger.debug("Parsing resource adequacy report file json...")
        for section_name, section_data in data_map.items():
            if "hourly" in section_data:
                for metric_name, config in section_data["hourly"].items():
                    self._extract_hourly_values(
                        data=document_body,
                        path=config["path"],
                        column_name=metric_name,
                        value_key=config["value_key"],
                        report_data=report_data,
                    )

            if "fuel_type_hourly" in section_data:
                fuel_type_config = section_data["fuel_type_hourly"]
                resources = list(
                    get_nested_data(document_body, fuel_type_config["path"]),
                )

                for resource in resources:
                    fuel_type = resource.get("FuelType")
                    if fuel_type in fuel_type_config["resources"]:
                        metrics = fuel_type_config["resources"][fuel_type]
                        for metric, config in metrics.items():
                            self._extract_hourly_values(
                                data=resource,
                                path=config["path"],
                                column_name=f"{fuel_type} {metric}",
                                value_key=config["value_key"],
                                report_data=report_data,
                            )

            for zonal_section in ["zonal_import_hourly", "zonal_export_hourly"]:
                if zonal_section in section_data:
                    zonal_config = section_data[zonal_section]
                    zones = get_nested_data(document_body, zonal_config["path"])
                    if not isinstance(zones, list):
                        zones = [zones]

                    for zone in zones:
                        zone_name = zone.get("ZoneName")
                        if zone_name in zonal_config["zones"]:
                            metrics = zonal_config["zones"][zone_name]
                            for metric, config in metrics.items():
                                self._extract_hourly_values(
                                    data=zone,
                                    path=config["path"],
                                    column_name=f"{zone_name} {metric}",
                                    value_key=config["value_key"],
                                    report_data=report_data,
                                )

            if "total_internal_resources" in section_data:
                total_internal_resources_config = section_data[
                    "total_internal_resources"
                ]
                total_resources = get_nested_data(
                    document_body,
                    total_internal_resources_config["path"],
                )
                for section_name, config in total_internal_resources_config[
                    "sections"
                ].items():
                    self._extract_hourly_values(
                        data=total_resources,
                        path=config["path"],
                        column_name=section_name,
                        value_key=config["value_key"],
                        report_data=report_data,
                    )

            if "total_imports" in section_data:
                total_imports_config = section_data["total_imports"]
                total_imports = get_nested_data(
                    document_body,
                    total_imports_config["path"],
                )
                for metric, config in total_imports_config["metrics"].items():
                    self._extract_hourly_values(
                        data=total_imports,
                        path=config["path"],
                        column_name=f"Total Imports {metric}",
                        value_key=config["value_key"],
                        report_data=report_data,
                    )

            if "total_exports" in section_data:
                total_exports_config = section_data["total_exports"]
                total_exports = get_nested_data(
                    document_body,
                    total_exports_config["path"],
                )
                for metric, config in total_exports_config["metrics"].items():
                    self._extract_hourly_values(
                        data=total_exports,
                        path=config["path"],
                        column_name=f"Total Exports {metric}",
                        value_key=config["value_key"],
                        report_data=report_data,
                    )

            if "reserves" in section_data:
                reserves_config = section_data["reserves"]
                reserves = get_nested_data(document_body, reserves_config["path"])
                for section_name, config in reserves_config["sections"].items():
                    self._extract_hourly_values(
                        data=reserves,
                        path=config["path"],
                        column_name=section_name,
                        value_key=config["value_key"],
                        report_data=report_data,
                    )

            if "ontario_demand" in section_data:
                ontario_demand_config = section_data["ontario_demand"]
                ontario_demand = get_nested_data(
                    document_body,
                    ontario_demand_config["path"],
                )
                for section_name, config in ontario_demand_config["sections"].items():
                    if "sections" in config:
                        continue

                    self._extract_hourly_values(
                        data=ontario_demand,
                        path=config["path"],
                        column_name=section_name,
                        value_key=config["value_key"],
                        report_data=report_data,
                    )

                for ontario_demand_btm in [
                    "Dispatchable Load",
                    "Hourly Demand Response",
                ]:
                    btm_config = ontario_demand_config["sections"][ontario_demand_btm]
                    btm_data = ontario_demand[ontario_demand_btm.replace(" ", "")]
                    for section_name, config in btm_config["sections"].items():
                        self._extract_hourly_values(
                            data=btm_data,
                            path=config["path"],
                            column_name=section_name,
                            value_key=config["value_key"],
                            report_data=report_data,
                        )

        # NOTE(kladar): This is the first place where pandas is truly invoked, leaving it open for more modern
        # dataframe libraries to be swapped in in the future
        df = pd.DataFrame(report_data)

        publish_time = pd.Timestamp(
            json_data["Document"]["DocHeader"]["CreatedAt"],
            tz=self.default_timezone,
        )
        delivery_date = pd.Timestamp(
            document_body["DeliveryDate"],
            tz=self.default_timezone,
        )
        logger.debug(f"Publish Time: {publish_time}")
        logger.debug(f"Delivery Date: {delivery_date}")
        df["Interval Start"] = delivery_date + pd.to_timedelta(
            df["DeliveryHour"] - 1,
            unit="h",
        )
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)
        df["Publish Time"] = publish_time

        df = df.drop(columns=["DeliveryHour"])
        return df

    # TODO(Kladar): this could likely be developed from the XML structure, but this works for now
    # and is easier to modify and quite legible
    def _get_resource_adequacy_data_structure_map(self) -> dict:
        """Define mapping of hourly data locations and extraction rules"""
        return {
            "supply": {
                "hourly": {
                    "Forecast Supply Capacity": {
                        "path": ["ForecastSupply", "Capacities", "Capacity"],
                        "value_key": "EnergyMW",
                    },
                    "Forecast Supply Energy MWh": {
                        "path": ["ForecastSupply", "Energies", "Energy"],
                        "value_key": "EnergyMWhr",
                    },
                    "Forecast Supply Bottled Capacity": {
                        "path": ["ForecastSupply", "BottledCapacities", "Capacity"],
                        "value_key": "EnergyMW",
                    },
                    "Forecast Supply Regulation": {
                        "path": ["ForecastSupply", "Regulations", "Regulation"],
                        "value_key": "EnergyMW",
                    },
                    "Total Forecast Supply": {
                        "path": ["ForecastSupply", "TotalSupplies", "Supply"],
                        "value_key": "EnergyMW",
                    },
                    "Total Requirement": {
                        "path": ["ForecastDemand", "TotalRequirements", "Requirement"],
                        "value_key": "EnergyMW",
                    },
                    "Capacity Excess Shortfall": {
                        "path": ["ForecastDemand", "ExcessCapacities", "Capacity"],
                        "value_key": "EnergyMW",
                    },
                    "Energy Excess Shortfall MWh": {
                        "path": ["ForecastDemand", "ExcessEnergies", "Energy"],
                        "value_key": "EnergyMWhr",
                    },
                    "Offered Capacity Excess Shortfall": {
                        "path": [
                            "ForecastDemand",
                            "ExcessOfferedCapacities",
                            "Capacity",
                        ],
                        "value_key": "EnergyMW",
                    },
                    "Resources Not Scheduled": {
                        "path": [
                            "ForecastDemand",
                            "UnscheduledResources",
                            "UnscheduledResource",
                        ],
                        "value_key": "EnergyMW",
                    },
                    "Imports Not Scheduled": {
                        "path": [
                            "ForecastDemand",
                            "UnscheduledImports",
                            "UnscheduledImport",
                        ],
                        "value_key": "EnergyMW",
                    },
                },
                "fuel_type_hourly": {
                    "path": ["ForecastSupply", "InternalResources", "InternalResource"],
                    "resources": {
                        "Nuclear": {
                            "Capacity": {
                                "path": ["Capacities", "Capacity"],
                                "value_key": "EnergyMW",
                            },
                            "Outages": {
                                "path": ["Outages", "Outage"],
                                "value_key": "EnergyMW",
                            },
                            "Offered": {
                                "path": ["Offers", "Offer"],
                                "value_key": "EnergyMW",
                            },
                            "Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                        "Gas": {
                            "Capacity": {
                                "path": ["Capacities", "Capacity"],
                                "value_key": "EnergyMW",
                            },
                            "Outages": {
                                "path": ["Outages", "Outage"],
                                "value_key": "EnergyMW",
                            },
                            "Offered": {
                                "path": ["Offers", "Offer"],
                                "value_key": "EnergyMW",
                            },
                            "Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                        "Hydro": {
                            "Capacity": {
                                "path": ["Capacities", "Capacity"],
                                "value_key": "EnergyMW",
                            },
                            "Outages": {
                                "path": ["Outages", "Outage"],
                                "value_key": "EnergyMW",
                            },
                            "Forecasted MWh": {
                                "path": ["ForecastEnergies", "ForecastEnergy"],
                                "value_key": "EnergyMWhr",
                            },
                            "Offered": {
                                "path": ["Offers", "Offer"],
                                "value_key": "EnergyMW",
                            },
                            "Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                        "Wind": {
                            "Capacity": {
                                "path": ["Capacities", "Capacity"],
                                "value_key": "EnergyMW",
                            },
                            "Outages": {
                                "path": ["Outages", "Outage"],
                                "value_key": "EnergyMW",
                            },
                            "Forecasted": {
                                "path": ["Forecasts", "Forecast"],
                                "value_key": "EnergyMW",
                            },
                            "Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                        "Solar": {
                            "Capacity": {
                                "path": ["Capacities", "Capacity"],
                                "value_key": "EnergyMW",
                            },
                            "Outages": {
                                "path": ["Outages", "Outage"],
                                "value_key": "EnergyMW",
                            },
                            "Forecasted": {
                                "path": ["Forecasts", "Forecast"],
                                "value_key": "EnergyMW",
                            },
                            "Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                        "Biofuel": {
                            "Capacity": {
                                "path": ["Capacities", "Capacity"],
                                "value_key": "EnergyMW",
                            },
                            "Outages": {
                                "path": ["Outages", "Outage"],
                                "value_key": "EnergyMW",
                            },
                            "Offered": {
                                "path": ["Offers", "Offer"],
                                "value_key": "EnergyMW",
                            },
                            "Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                        "Other": {
                            "Capacity": {
                                "path": ["Capacities", "Capacity"],
                                "value_key": "EnergyMW",
                            },
                            "Outages": {
                                "path": ["Outages", "Outage"],
                                "value_key": "EnergyMW",
                            },
                            "Offered Forecasted": {
                                "path": ["OfferForecasts", "OfferForecast"],
                                "value_key": "EnergyMW",
                            },
                            "Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                    },
                },
                "total_internal_resources": {
                    "path": [
                        "ForecastSupply",
                        "InternalResources",
                        "TotalInternalResources",
                    ],
                    "sections": {
                        "Total Internal Resources Outages": {
                            "path": ["Outages", "Outage"],
                            "value_key": "EnergyMW",
                        },
                        "Total Internal Resources Offered Forecasted": {
                            "path": ["OfferForecasts", "OfferForecast"],
                            "value_key": "EnergyMW",
                        },
                        "Total Internal Resources Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                },
                "zonal_import_hourly": {
                    "path": ["ForecastSupply", "ZonalImports", "ZonalImport"],
                    "zones": {
                        "Manitoba": {
                            "Imports Offered": {
                                "path": ["Offers", "Offer"],
                                "value_key": "EnergyMW",
                            },
                            "Imports Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                        "Minnesota": {
                            "Imports Offered": {
                                "path": ["Offers", "Offer"],
                                "value_key": "EnergyMW",
                            },
                            "Imports Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                        "Michigan": {
                            "Imports Offered": {
                                "path": ["Offers", "Offer"],
                                "value_key": "EnergyMW",
                            },
                            "Imports Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                        "New York": {
                            "Imports Offered": {
                                "path": ["Offers", "Offer"],
                                "value_key": "EnergyMW",
                            },
                            "Imports Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                        "Quebec": {
                            "Imports Offered": {
                                "path": ["Offers", "Offer"],
                                "value_key": "EnergyMW",
                            },
                            "Imports Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                    },
                },
                "total_imports": {
                    "path": ["ForecastSupply", "ZonalImports", "TotalImports"],
                    "metrics": {
                        "Offers": {
                            "path": ["Offers", "Offer"],
                            "value_key": "EnergyMW",
                        },
                        "Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                        "Estimated": {
                            "path": ["Estimates", "Estimate"],
                            "value_key": "EnergyMW",
                        },
                        "Capacity": {
                            "path": ["Capacities", "Capacity"],
                            "value_key": "EnergyMW",
                        },
                    },
                },
            },
            "demand": {
                "ontario_demand": {
                    "path": ["ForecastDemand", "OntarioDemand"],
                    "sections": {
                        "Ontario Demand Forecast": {
                            "path": ["ForecastOntDemand", "Demand"],
                            "value_key": "EnergyMW",
                        },
                        "Ontario Peak Demand": {
                            "path": ["PeakDemand", "Demand"],
                            "value_key": "EnergyMW",
                        },
                        "Ontario Average Demand": {
                            "path": ["AverageDemand", "Demand"],
                            "value_key": "EnergyMW",
                        },
                        "Ontario Wind Embedded Forecast": {
                            "path": ["WindEmbedded", "Embedded"],
                            "value_key": "EnergyMW",
                        },
                        "Ontario Solar Embedded Forecast": {
                            "path": ["SolarEmbedded", "Embedded"],
                            "value_key": "EnergyMW",
                        },
                        "Dispatchable Load": {
                            "sections": {
                                "Ontario Dispatchable Load Capacity": {
                                    "path": ["Capacities", "Capacity"],
                                    "value_key": "EnergyMW",
                                },
                                "Ontario Dispatchable Load Bid Forecasted": {
                                    "path": ["BidForecasts", "BidForecast"],
                                    "value_key": "EnergyMW",
                                },
                                "Ontario Dispatchable Load Scheduled ON": {
                                    "path": ["ScheduledON", "Schedule"],
                                    "value_key": "EnergyMW",
                                },
                                "Ontario Dispatchable Load Scheduled OFF": {
                                    "path": ["ScheduledOFF", "Schedule"],
                                    "value_key": "EnergyMW",
                                },
                            },
                        },
                        "Hourly Demand Response": {
                            "sections": {
                                "Ontario Hourly Demand Response Bid Forecasted": {
                                    "path": ["Bids", "Bid"],
                                    "value_key": "EnergyMW",
                                },
                                "Ontario Hourly Demand Response Scheduled": {
                                    "path": ["Schedules", "Schedule"],
                                    "value_key": "EnergyMW",
                                },
                                "Ontario Hourly Demand Response Curtailed": {
                                    "path": ["Curtailed", "Curtail"],
                                    "value_key": "EnergyMW",
                                },
                            },
                        },
                    },
                },
                "zonal_export_hourly": {
                    "path": ["ForecastDemand", "ZonalExports", "ZonalExport"],
                    "zones": {
                        "Manitoba": {
                            "Exports Offered": {
                                "path": ["Bids", "Bid"],
                                "value_key": "EnergyMW",
                            },
                            "Exports Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                        "Minnesota": {
                            "Exports Offered": {
                                "path": ["Bids", "Bid"],
                                "value_key": "EnergyMW",
                            },
                            "Exports Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                        "Michigan": {
                            "Exports Offered": {
                                "path": ["Bids", "Bid"],
                                "value_key": "EnergyMW",
                            },
                            "Exports Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                        "New York": {
                            "Exports Offered": {
                                "path": ["Bids", "Bid"],
                                "value_key": "EnergyMW",
                            },
                            "Exports Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                        "Quebec": {
                            "Exports Offered": {
                                "path": ["Bids", "Bid"],
                                "value_key": "EnergyMW",
                            },
                            "Exports Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                    },
                },
                "total_exports": {
                    "path": ["ForecastDemand", "ZonalExports", "TotalExports"],
                    "metrics": {
                        "Bids": {
                            "path": ["Bids", "Bid"],
                            "value_key": "EnergyMW",
                        },
                        "Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                        "Capacity": {
                            "path": ["Capacities", "Capacity"],
                            "value_key": "EnergyMW",
                        },
                    },
                },
                "reserves": {
                    "path": ["ForecastDemand", "GenerationReserveHoldback"],
                    "sections": {
                        "Total Operating Reserve": {
                            "path": ["TotalORReserve", "ORReserve"],
                            "value_key": "EnergyMW",
                        },
                        "Minimum 10 Minute Operating Reserve": {
                            "path": ["Min10MinOR", "Min10OR"],
                            "value_key": "EnergyMW",
                        },
                        "Minimum 10 Minute Spin OR": {
                            "path": ["Min10MinSpinOR", "Min10SpinOR"],
                            "value_key": "EnergyMW",
                        },
                        "Load Forecast Uncertainties": {
                            "path": ["LoadForecastUncertainties", "Uncertainty"],
                            "value_key": "EnergyMW",
                        },
                        "Additional Contingency Allowances": {
                            "path": ["ContingencyAllowances", "Allowance"],
                            "value_key": "EnergyMW",
                        },
                    },
                },
            },
        }

    def _extract_hourly_values(
        self,
        data: dict,
        path: list[str],
        column_name: str,
        value_key: str,
        report_data: list[dict],
    ) -> None:
        """Extract hourly values from nested json data into report_data list, which becomes a dataframe later.

        Args:
            data: Source data dictionary
            path: List of keys to traverse to reach hourly data (e.g. ["Capacities", "Capacity"])
            column_name: Name for the extracted data column
            value_key: Key containing the value to extract (e.g. "EnergyMW")
            report_data: List to store extracted hourly data rows
        """

        current = data
        for key in path[:-1]:
            if key not in current:
                logger.debug(
                    f"Path segment {path} has no key '{key}' in the data structure. Investigate the report data map definition.",
                )
                return
            current = current[key]

        items = current.get(path[-1], [])
        if items is None:
            items = []
        elif not isinstance(items, list):
            items = [items]

        existing_hours = {row["DeliveryHour"] for row in report_data}

        for hour in range(1, 25):
            if hour not in existing_hours:
                report_data.append({"DeliveryHour": hour})

        hours_with_values = set()

        for item in items:
            if item is None:
                continue

            hour = int(item["DeliveryHour"])
            hours_with_values.add(hour)

            row = next(r for r in report_data if r["DeliveryHour"] == hour)
            try:
                value = item.get(value_key)
                row[column_name] = float(value) if value is not None else None
            except (ValueError, TypeError):
                row[column_name] = None

        missing_value_hours = set(range(1, 25)) - hours_with_values
        if missing_value_hours:
            logger.debug(
                f"Detected {len(missing_value_hours)} hours without values for column {column_name}. Filling in with None.",
            )
        for hour in range(1, 25):
            if hour not in hours_with_values:
                row = next(r for r in report_data if r["DeliveryHour"] == hour)
                row[column_name] = None

    @support_date_range(frequency="DAY_START")
    def get_forecast_surplus_baseload_generation(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get forecast surplus baseload generation.

        Args:
            date: The publish date to get data for. The forecast will be for the day after this date.
            end: The end date to get data for. If None, only get data for the start date.
            verbose: Whether to print verbose output.

        Returns:
            DataFrame with columns:
                - Interval Start: The start of the interval
                - Interval End: The end of the interval
                - Publish Time: The time the forecast was published
                - Surplus Baseload MW: The forecast surplus baseload generation in MW
                - Surplus State: The state of the surplus baseload generation
                - Action: The action taken for the surplus baseload generation
                - Export Forecast MW: The forecast export in MW
                - Minimum Generation Status: The minimum generation status
        """
        if date == "latest":
            yesterday = pd.Timestamp.now(
                tz=self.default_timezone,
            ).normalize() - pd.Timedelta(days=1)
            return self.get_forecast_surplus_baseload_generation(yesterday)

        if isinstance(date, tuple):
            publish_date_str = pd.Timestamp(date[0]).strftime("%Y%m%d")
        else:
            publish_date_str = pd.Timestamp(date).strftime("%Y%m%d")

        logger.info(
            f"Getting forecast surplus baseload for {pd.Timestamp(date).strftime('%Y-%m-%d')}",
        )
        url = f"https://www.ieso.ca/-/media/Files/IESO/uploaded/sbg/PUB_SurplusBaseloadGen_{publish_date_str}_v1"
        r = self._request(url)
        json_data = xmltodict.parse(r.text)

        publish_time = pd.Timestamp(
            json_data["Document"]["DocHeader"]["CreatedAt"],
            tz=self.default_timezone,
        )

        data = []
        for daily_forecast in json_data["Document"]["DocBody"]["DailyForecast"]:
            date_forecast = pd.Timestamp(daily_forecast["DateForecast"])
            export_forecast = float(daily_forecast["ExportForecast"])
            min_gen_status = daily_forecast["MinGenerationStatus"]

            for hourly_forecast in daily_forecast["HourlyForecast"]:
                hour = int(hourly_forecast["Hour"])
                energy_mw = (
                    float(hourly_forecast["EnergyMW"])
                    if hourly_forecast["EnergyMW"]
                    else None
                )
                action = hourly_forecast.get("Action")

                if not energy_mw or energy_mw == 0:
                    surplus_state = SurplusState.NO_SURPLUS.value
                elif action == "Manoeuvre":
                    surplus_state = SurplusState.NUCLEAR_DISPATCH.value
                elif action == "Shutdown":
                    surplus_state = SurplusState.NUCLEAR_SHUTDOWN.value
                elif action == "Other":
                    surplus_state = SurplusState.MANAGED_WITH_EXPORTS.value
                else:
                    surplus_state = SurplusState.NO_SURPLUS.value

                interval_start = (
                    date_forecast + pd.Timedelta(hours=hour - 1)
                ).tz_localize(self.default_timezone)
                interval_end = interval_start + pd.Timedelta(hours=1)

                data.append(
                    {
                        "Interval Start": interval_start,
                        "Interval End": interval_end,
                        "Publish Time": publish_time,
                        "Surplus Baseload MW": energy_mw,
                        "Surplus State": surplus_state,
                        "Action": action,
                        "Export Forecast MW": export_forecast,
                        "Minimum Generation Status": min_gen_status,
                    },
                )

        df = pd.DataFrame(data)
        df.sort_values("Interval Start", inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Surplus Baseload MW",
                "Surplus State",
                "Action",
                "Export Forecast MW",
                "Minimum Generation Status",
            ]
        ]

    @support_date_range(frequency="YEAR_START")
    def get_intertie_actual_schedule_flow_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
        vintage: Literal["all", "latest"] = "latest",
        last_modified: str | datetime.date | datetime.datetime | None = None,
    ) -> pd.DataFrame:
        """Get yearly intertie actual schedule flow hourly.
        Args:
            date: The date to get the data for.
            end: The end date to get the data for.
            verbose: Whether to print verbose output.
            vintage: Whether to get the latest version or all versions of the report.
            last_modified: Only return reports modified after this date.
        Returns:
            DataFrame with hourly intertie schedule flow data.
        """
        if last_modified:
            last_modified = utils._handle_date(last_modified, tz=self.default_timezone)

        today = utils._handle_date("today", tz=self.default_timezone)

        if date == "latest":
            target_year = today.year
            return self._get_intertie_schedule_flow_data(
                target_year,
                vintage=vintage,
                last_modified=last_modified,
                verbose=verbose,
            )

        if isinstance(date, str) and date not in ["today", "latest"]:
            date = utils._handle_date(date, tz=self.default_timezone)
        elif date == "today":
            date = today

        if isinstance(date, tuple):
            start_date, end_date = date
            start_year = pd.Timestamp(start_date).year
            end_year = pd.Timestamp(end_date).year

            all_data = []
            for year in range(start_year, end_year + 1):
                df = self._get_intertie_schedule_flow_data(
                    year,
                    vintage=vintage,
                    last_modified=last_modified,
                    verbose=verbose,
                )
                all_data.append(df)

            if not all_data:
                return pd.DataFrame()

            result_df = pd.concat(all_data)

            result_df = result_df[
                (result_df["Interval Start"] >= pd.Timestamp(start_date))
                & (result_df["Interval Start"] <= pd.Timestamp(end_date))
            ]

            return result_df.sort_values(["Interval Start", "Publish Time"])

        year = pd.Timestamp(date).year
        df = self._get_intertie_schedule_flow_data(
            year,
            vintage=vintage,
            last_modified=last_modified,
            verbose=verbose,
        )

        if end:
            end_date = utils._handle_date(end, tz=self.default_timezone)
            df = df[
                (df["Interval Start"] >= pd.Timestamp(date))
                & (df["Interval Start"] <= end_date)
            ]
        else:
            target_date = pd.Timestamp(date).date()
            df = df[df["Interval Start"].dt.date == target_date]

        return df

    def _get_intertie_schedule_flow_data(
        self,
        year: int,
        vintage: Literal["all", "latest"] = "latest",
        last_modified: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Fetch and parse intertie schedule flow data for a specific year.
        Args:
            year: The year to fetch data for
            vintage: Whether to fetch the latest version or all versions
            last_modified: Only return reports modified after this date
            verbose: Whether to print verbose output
        Returns:
            DataFrame containing the parsed data
        """
        base_url = "https://reports-public.ieso.ca/public/IntertieScheduleFlowYear"

        r = self._request(base_url, verbose)

        if vintage == "latest":
            file_name = f"PUB_IntertieScheduleFlowYear_{year}.csv"

            pattern = f'href="(PUB_IntertieScheduleFlowYear_{year}.*?.csv)"'
            files = re.findall(pattern, r.text)

            pattern_with_time = f'href="(PUB_IntertieScheduleFlowYear_{year}.*?.csv)">.*?</a>\\s+(\\d{{2}}-\\w{{3}}-\\d{{4}} \\d{{2}}:\\d{{2}})'
            files_with_times = re.findall(pattern_with_time, r.text)

            if file_name in files:
                for file, time in files_with_times:
                    if file == file_name:
                        file_time = time
                        break
                url = f"{base_url}/{file_name}"
                last_modified_time = pd.Timestamp(file_time, tz=self.default_timezone)
            else:
                versioned_files = [f for f in files if "_v" in f]
                latest_file = max(
                    versioned_files,
                    key=lambda x: int(x.split("_v")[1].replace(".csv", "")),
                )

                for file, time in files_with_times:
                    if file == latest_file:
                        file_time = time
                        break

                url = f"{base_url}/{latest_file}"
                last_modified_time = pd.Timestamp(file_time, tz=self.default_timezone)

            if last_modified and last_modified_time < last_modified:
                logger.info(f"No files for year {year} modified after {last_modified}")
                return pd.DataFrame()

            logger.info(f"Fetching intertie schedule flow data from {url}")
            return self._parse_intertie_schedule_flow_file(
                url,
                last_modified_time,
                verbose,
            )

        elif vintage == "all":
            pattern = f'href="(PUB_IntertieScheduleFlowYear_{year}.*?.csv)"'
            files = re.findall(pattern, r.text)
            logger.info(f"Found {len(files)} files for year {year}")
            pattern_with_time = f'href="(PUB_IntertieScheduleFlowYear_{year}.*?.csv)">.*?</a>\\s+(\\d{{2}}-\\w{{3}}-\\d{{4}} \\d{{2}}:\\d{{2}})'
            files_with_times = re.findall(pattern_with_time, r.text)

            if last_modified:
                filtered_files = [
                    (file, time)
                    for file, time in files_with_times
                    if pd.Timestamp(time, tz=self.default_timezone) >= last_modified
                ]
                logger.info(
                    f"Found {len(filtered_files)} files after last modified time {last_modified}",
                )
                files_with_times = filtered_files

            all_data = []
            for file, time in files_with_times:
                url = f"{base_url}/{file}"
                logger.info(f"Fetching intertie schedule flow data from {url}")
                modified_time = pd.Timestamp(time, tz=self.default_timezone)
                df = self._parse_intertie_schedule_flow_file(
                    url,
                    modified_time,
                    verbose,
                )
                all_data.append(df)
            df_final = pd.concat(all_data)
            logger.info(
                f"Dropping duplicates from vintage {vintage} concatenation of files",
            )
            df_final.drop_duplicates(inplace=True)
            return df_final.sort_values(["Interval Start", "Publish Time"]).reset_index(
                drop=True,
            )

    def _parse_intertie_schedule_flow_file(
        self,
        url: str,
        last_modified_time: pd.Timestamp,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Parse a single intertie schedule flow CSV file.
        Args:
            url: URL of the CSV file to parse
            last_modified_time: Last modified time of the file
            verbose: Whether to print verbose output
        Returns:
            DataFrame containing the parsed data
        """
        df = pd.read_csv(
            url,
            skiprows=3,
            header=[0, 1],
        )

        df = df.melt(
            id_vars=[("Unnamed: 0_level_0", "Date"), ("Unnamed: 1_level_0", "Hour")],
        )

        df.columns = ["Date", "Hour", "Zone", "Metric", "Value"]
        df["Metric"] = df["Metric"].replace({"Imp": "Import", "Exp": "Export"})

        df["Zone"] = df["Zone"].apply(
            lambda x: x.replace(".", "")
            if isinstance(x, str) and x.startswith("PQ")
            else (x.replace("-", " ").title() if isinstance(x, str) else x),
        )

        df = df.pivot_table(
            index=["Date", "Hour"],
            columns=["Zone", "Metric"],
            values="Value",
        ).reset_index()

        df.columns = [
            f"{col[0]} {col[1]}" if col[1] != "" else col[0] for col in df.columns
        ]

        flow_columns = [
            column
            for column in df.columns
            if any(x in column for x in ["Import", "Export", "Flow"])
        ]
        for column in flow_columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

        df["Hour"] = df["Hour"].astype(int)
        df["Interval Start"] = pd.to_datetime(df["Date"]) + pd.to_timedelta(
            df["Hour"] - 1,
            unit="h",
        )
        df["Interval Start"] = df["Interval Start"].dt.tz_localize(
            self.default_timezone,
        )
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)
        df["Publish Time"] = last_modified_time

        key_columns = ["Interval Start", "Interval End", "Publish Time"]
        total_columns = sorted([column for column in df.columns if "Total" in column])
        df = utils.move_cols_to_front(df, key_columns + total_columns)

        df.drop(columns=["Date", "Hour"], inplace=True)
        return df
