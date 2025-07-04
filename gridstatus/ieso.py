# flake8: noqa: E501

import datetime
import http.client
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from typing import Literal, Optional
from urllib.error import HTTPError
from warnings import warn
from xml.etree import ElementTree
from xml.etree.ElementTree import Element

import pandas as pd
import requests
import tqdm
import xmltodict
from bs4 import BeautifulSoup
from lxml import etree as lxml_etree

from gridstatus import utils
from gridstatus.base import ISOBase, NoDataFoundException, NotSupported
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import logger
from gridstatus.ieso_constants import (
    FUEL_MIX_TEMPLATE_URL,
    HISTORICAL_FUEL_MIX_TEMPLATE_URL,
    INTERTIE_ACTUAL_SCHEDULE_FLOW_HOURLY_COLUMNS,
    INTERTIE_FLOW_5_MIN_COLUMNS,
    MAXIMUM_DAYS_IN_PAST_FOR_COMPLETE_GENERATOR_REPORT,
    NAMESPACES_FOR_XML,
    ONTARIO_LOCATION,
    PUBLIC_REPORTS_URL_PREFIX,
    RESOURCE_ADEQUACY_REPORT_BASE_URL,
    RESOURCE_ADEQUACY_REPORT_DATA_STRUCTURE_MAP,
    ZONAL_LOAD_COLUMNS,
)

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CERTIFICATES_CHAIN_FILE = os.path.join(
    CURRENT_DIR,
    "public_certificates/ieso/intermediate_and_root.pem",
)

# Date when IESO switched to new market and retired several datasets
RETIRED_DATE = datetime.date(2025, 5, 1)


def retired_data_warning():
    warn(
        f"This dataset was retired on {RETIRED_DATE}. Only data prior to that date is available",
        UserWarning,
    )


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


def _safe_find_text(
    element: Optional[Element],
    tag: str,
    namespaces: Optional[dict[str, str]] = None,
    default: Optional[str] = None,
) -> Optional[str]:
    """Safely find and extract text from an XML element.

    Args:
        element: XML element to search within
        tag: Tag name to find
        namespaces: XML namespaces dict
        default: Default value to return if element not found or empty

    Returns:
        str or default: The text content or default value
    """
    if element is None:
        return default

    found = element.find(tag, namespaces)
    if found is None or found.text is None or found.text.strip() == "":
        return default

    return found.text


def _safe_find_int(
    element: Optional[Element],
    tag: str,
    namespaces: Optional[dict[str, str]] = None,
    default: Optional[int] = None,
) -> Optional[int]:
    """Safely find and extract integer from an XML element.

    Args:
        element: XML element to search within
        tag: Tag name to find
        namespaces: XML namespaces dict
        default: Default value to return if element not found or empty

    Returns:
        int or default: The integer value or default value
    """
    text = _safe_find_text(element, tag, namespaces)
    if text is None:
        return default

    try:
        return int(text)
    except (ValueError, TypeError):
        return default


def _safe_find_float(
    element: Optional[Element],
    tag: str,
    namespaces: Optional[dict[str, str]] = None,
    default: Optional[float] = None,
) -> Optional[float]:
    """Safely find and extract float from an XML element.

    Args:
        element: XML element to search within
        tag: Tag name to find
        namespaces: XML namespaces dict
        default: Default value to return if element not found or empty

    Returns:
        float or default: The float value or default value
    """
    text = _safe_find_text(element, tag, namespaces)
    if text is None:
        return default

    try:
        return float(text)
    except (ValueError, TypeError):
        return default


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
        raise NotSupported(
            f"With the IESO Market Renewal on {RETIRED_DATE}, this method is no longer supported. To get load data, use the `get_real_time_totals` method instead.",
        )

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
        raise NotSupported(
            f"With the IESO Market Renewal on {RETIRED_DATE}, this method is no longer supported. To get load forecast data, use the `get_resource_adequacy_report` method instead.",
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
        raise NotSupported(
            f"With the IESO Market Renewal on {RETIRED_DATE}, this method is no longer supported. To get zonal load forecast data, use the `get_resource_adequacy_report` method instead.",
        )

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

        root = ElementTree.fromstring(r.content)

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

        root = ElementTree.fromstring(r.content)
        ns = NAMESPACES_FOR_XML.copy()
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
        market_quantity_element: ElementTree.Element,
        market_quantity_name: str,
    ):
        for mq in market_quantity_element.findall("MQ", NAMESPACES_FOR_XML):
            market_quantity = mq.find("MarketQuantity", NAMESPACES_FOR_XML).text

            if market_quantity_name in market_quantity:
                return mq.find("EnergyMW", NAMESPACES_FOR_XML).text

        return None

    # Function to find all triples of 'Interval', 'Market Total Load', and
    # 'Ontario Load' in the XML file
    def _find_loads_at_each_interval_from_xml(self, root_element: ElementTree.Element):
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

    def get_mcp_real_time_5_min(self):
        raise NotSupported(
            "MCP data is no longer available. For real time pricing information, use the `get_lmp_real_time_5_min` method instead. For historical MCP data, use the `get_mcp_historical_5_min` method.",
        )

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
        retired_data_warning()

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
        retired_data_warning()

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

            # If the file is not found, there is no need to retry
            if r.status_code == 404:
                raise NoDataFoundException(
                    f"File not found at {url}. Please check the URL.",
                )

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
        base_url = RESOURCE_ADEQUACY_REPORT_BASE_URL

        if isinstance(date, (datetime.datetime, datetime.date)):
            date_str = date.strftime("%Y%m%d")
        else:
            date_str = date.replace("-", "")

        file_prefix = f"PUB_Adequacy3_{date_str}"

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
        base_url = RESOURCE_ADEQUACY_REPORT_BASE_URL

        if isinstance(date, (datetime.datetime, datetime.date)):
            date_str = date.strftime("%Y%m%d")
        else:
            date_str = date.replace("-", "")

        file_prefix = f"PUB_Adequacy3_{date_str}"

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
        data_map = RESOURCE_ADEQUACY_REPORT_DATA_STRUCTURE_MAP

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
    def get_yearly_intertie_actual_schedule_flow_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
        vintage: Literal["all", "latest"] = "latest",
        last_modified: str | datetime.date | datetime.datetime | None = None,
    ) -> pd.DataFrame:
        """Get yearly intertie actual schedule flow hourly. Since this is a yearly file
        it is updated less frequency than the daily files. These can be retrieved via
        the get_intertie_schedule_flow_hourly method.
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

        return df[INTERTIE_ACTUAL_SCHEDULE_FLOW_HOURLY_COLUMNS].reset_index(drop=True)

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

    @support_date_range(frequency="DAY_START")
    def get_intertie_actual_schedule_flow_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        return self._get_and_parse_intertie_schedule_flow(
            date,
            return_five_minute_data=False,
            verbose=verbose,
        )

    @support_date_range(frequency="DAY_START")
    def get_intertie_flow_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        return self._get_and_parse_intertie_schedule_flow(
            date,
            return_five_minute_data=True,
            verbose=verbose,
        )

    def _get_and_parse_intertie_schedule_flow(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        return_five_minute_data: bool = False,
        verbose: bool = False,
    ) -> pd.DataFrame:
        directory_path = "IntertieScheduleFlow"
        file_directory = f"{PUBLIC_REPORTS_URL_PREFIX}/{directory_path}"

        if date == "latest":
            url = f"{file_directory}/PUB_{directory_path}.xml"
        else:
            url = f"{file_directory}/PUB_{directory_path}_{date.strftime('%Y%m%d')}.xml"

        xml_content = self._request(url, verbose=verbose).text

        ns = {"": "http://www.theIMO.com/schema"}
        root = ElementTree.fromstring(xml_content)

        created_at = pd.Timestamp(
            root.find(".//CreatedAt", ns).text,
            tz=self.default_timezone,
        )

        base_datetime = pd.Timestamp(
            root.find(".//Date", ns).text,
            tz=self.default_timezone,
        )

        zones = root.findall(".//IntertieZone", ns)
        zone_interval_records = []

        for zone in zones:
            zone_name = zone.find(".//IntertieZoneName", ns).text

            schedules = zone.findall(".//Schedule", ns)
            schedule_data = {}

            for schedule in schedules:
                hour = _safe_find_int(schedule, ".//Hour", ns)
                imports = _safe_find_float(schedule, ".//Import", ns)
                exports = _safe_find_float(schedule, ".//Export", ns)

                # Skip if any required values are missing
                if any(val is None for val in [hour, imports, exports]):
                    continue

                schedule_data[hour] = {"import": imports, "export": exports}

            actuals = zone.findall(".//Actual", ns)

            for actual in actuals:
                hour = _safe_find_int(actual, ".//Hour", ns)
                interval = _safe_find_int(actual, ".//Interval", ns)
                flow = _safe_find_float(actual, ".//Flow", ns)

                # Skip if any required values are missing
                if any(val is None for val in [hour, interval, flow]):
                    continue

                hour_schedule = schedule_data.get(hour, {})

                zone_interval_records.append(
                    {
                        "Zone": zone_name,
                        "Hour": hour,
                        "Interval": interval,
                        "Flow": flow,
                        "Import": hour_schedule.get("import"),
                        "Export": hour_schedule.get("export"),
                    },
                )

        zone_five_minute_data = pd.DataFrame(zone_interval_records)
        zone_five_minute_data = zone_five_minute_data.pivot(
            columns="Zone",
            index=["Hour", "Interval"],
            values=["Import", "Export", "Flow"],
        )

        columns = []

        for metric, zone in zone_five_minute_data.columns:
            zone = zone.replace(".", "").replace("-", " ").title()

            if zone.startswith("Pq"):
                zone = zone.upper()

            columns.append(
                f"{zone} {metric.title()}",
            )

        zone_five_minute_data.columns = columns
        zone_five_minute_data = zone_five_minute_data.reset_index()

        totals = root.find(".//Totals", ns)
        total_schedules = totals.findall(".//Schedule", ns)
        total_hourly_schedule_records = []

        for schedule in total_schedules:
            hour = _safe_find_int(schedule, ".//Hour", ns)
            imports = _safe_find_float(schedule, ".//Import", ns)
            exports = _safe_find_float(schedule, ".//Export", ns)

            # Skip if any required values are missing
            if any(val is None for val in [hour, imports, exports]):
                continue
            total_hourly_schedule_records.append(
                {
                    "Hour": hour,
                    "Total Import": imports,
                    "Total Export": exports,
                },
            )

        total_hourly_schedule_data = pd.DataFrame(total_hourly_schedule_records)

        total_actuals = totals.findall(".//Actual", ns)
        total_five_minute_actuals_records = []

        for actual in total_actuals:
            hour = _safe_find_int(actual, ".//Hour", ns)
            interval = _safe_find_int(actual, ".//Interval", ns)
            flow = _safe_find_float(actual, ".//Flow", ns)

            # Skip if any required values are missing
            if any(val is None for val in [hour, interval, flow]):
                continue
            total_five_minute_actuals_records.append(
                {
                    "Hour": hour,
                    "Interval": interval,
                    "Total Flow": flow,
                },
            )

        total_five_minute_actuals_data = pd.DataFrame(total_five_minute_actuals_records)

        totals_five_minute_data = pd.merge(
            total_hourly_schedule_data,
            total_five_minute_actuals_data,
            on="Hour",
        )

        five_minute_data = pd.merge(
            zone_five_minute_data,
            totals_five_minute_data,
            on=["Hour", "Interval"],
        )

        if return_five_minute_data:
            five_minute_data["Interval Start"] = (
                base_datetime
                + pd.to_timedelta(five_minute_data["Hour"] - 1, unit="h")
                + pd.to_timedelta(
                    5 * (five_minute_data["Interval"] - 1),
                    unit="m",
                )
            )

            five_minute_data["Interval End"] = five_minute_data[
                "Interval Start"
            ] + pd.Timedelta(minutes=5)

            five_minute_data["Publish Time"] = created_at

            five_minute_data = (
                five_minute_data[INTERTIE_FLOW_5_MIN_COLUMNS]
                .sort_values(["Interval Start"])
                .reset_index(drop=True)
            )

            return five_minute_data

        hourly_data = (
            five_minute_data.drop(columns=["Interval"])
            .groupby(["Hour"])
            .mean()
            .reset_index()
        )
        hourly_data["Interval Start"] = base_datetime + pd.to_timedelta(
            hourly_data["Hour"] - 1,
            unit="h",
        )
        hourly_data["Interval End"] = hourly_data["Interval Start"] + pd.Timedelta(
            hours=1,
        )

        hourly_data["Publish Time"] = created_at

        hourly_data = (
            hourly_data[INTERTIE_ACTUAL_SCHEDULE_FLOW_HOURLY_COLUMNS]
            .sort_values(["Interval Start"])
            .reset_index(drop=True)
        )

        return hourly_data

    @support_date_range(frequency="HOUR_START")
    def get_lmp_real_time_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        directory_path = "RealtimeEnergyLMP"
        file_directory = f"{PUBLIC_REPORTS_URL_PREFIX}/{directory_path}"

        if date == "latest":
            url = f"{file_directory}/PUB_{directory_path}.csv"
            date = pd.Timestamp.now(tz=self.default_timezone)
        else:
            hour = date.hour
            # Hour numbers are 1-24, so we need to add 1
            file_hour = f"{hour + 1}".zfill(2)

            url = f"{file_directory}/PUB_{directory_path}_{date.strftime('%Y%m%d')}{file_hour}.csv"

        return self._get_lmp_csv_data(
            url,
            date,
            minutes_per_interval=5,
        )

    @support_date_range(frequency="DAY_START")
    def get_lmp_day_ahead_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get day-ahead LMP data.
        Args:
            date: The date to get the data for.
            end: The end date to get the data for.
            verbose: Whether to print verbose output.
        Returns:
            DataFrame with LMP data.
        """
        directory_path = "DAHourlyEnergyLMP"
        file_directory = f"{PUBLIC_REPORTS_URL_PREFIX}/{directory_path}"

        if date == "latest":
            url = f"{file_directory}/PUB_{directory_path}.csv"
            date = pd.Timestamp.now(tz=self.default_timezone)
        else:
            url = f"{file_directory}/PUB_{directory_path}_{date.strftime('%Y%m%d')}.csv"

        return self._get_lmp_csv_data(
            url,
            date,
            minutes_per_interval=60,
        )

    @support_date_range(frequency=None)
    def get_lmp_predispatch_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        directory_path = "PredispHourlyEnergyLMP"
        file_directory = f"{PUBLIC_REPORTS_URL_PREFIX}/{directory_path}"

        if date == "latest":
            urls = [f"{file_directory}/PUB_{directory_path}.csv"]
            date = pd.Timestamp.now(tz=self.default_timezone)
        else:
            files_and_times = self._get_directory_files_and_timestamps(
                file_directory,
                file_name_prefix=f"PUB_{directory_path}",
            )

            end = end or (date + pd.Timedelta(hours=1))

            urls = [
                f"{file_directory}/{file}"
                for file, file_time in files_and_times
                if date <= file_time < end
            ]

        if not urls:
            raise NoDataFoundException(
                f"No Predispatch Hourly LMP data found for {date} to {end}",
            )

        def process_url(url: str, verbose: bool = False) -> pd.DataFrame:
            # We need to get the file created date from the first line of the csv
            # Example: CREATED AT 2025/05/01 23:14:53 FOR 2025/05/02
            text = self._request(url, verbose=False).text
            first_line = text.splitlines()[0]

            match = re.search(
                r"CREATED AT (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})",
                first_line,
            )

            publish_timestamp_str = match.group(1)

            publish_time = pd.Timestamp(
                publish_timestamp_str,
                tz=self.default_timezone,
            )

            # Get the date the file is FOR to use as the base date
            match = re.search(r"FOR (\d{4}/\d{2}/\d{2})", first_line)
            delivery_date = pd.Timestamp(match.group(1), tz=self.default_timezone)

            file_data = self._get_lmp_csv_data(
                url,
                base_date=delivery_date,
                minutes_per_interval=60,
                verbose=verbose,
            )

            file_data["Publish Time"] = publish_time
            return file_data

        data = self._process_urls_with_threadpool(
            urls,
            process_url,
            f"No valid data found for Predispatch Hourly LMP for {date} to {end}",
            verbose=verbose,
        )

        data["Location"] = data["Location"].str.replace(":LMP", "")

        return data

    def _get_lmp_csv_data(
        self,
        url: str,
        base_date: pd.Timestamp,
        minutes_per_interval: Literal[5, 60] = 60,
        verbose: bool = False,
    ):
        """Common method to fetch and process LMP data.

        Args:
            url: The URL to fetch data from.
            base_date: The date to process data for.
            minutes_per_interval: Number of minutes per interval.

        Returns:
            DataFrame with processed LMP data.
        """
        if verbose:
            logger.info(f"Fetching LMP data from {url}")

        data = pd.read_csv(url, skiprows=1)

        if minutes_per_interval == 5:
            data["Interval Start"] = pd.to_datetime(
                base_date.normalize()
                # Need to subtract 1 from the hour because the hour is 1-indexed
                + pd.to_timedelta(data["Delivery Hour"] - 1, unit="hour")
                # The interval is 1-indexed, so we need to subtract 1 from the interval
                + pd.to_timedelta(
                    (data["Interval"] - 1) * minutes_per_interval,
                    unit="minute",
                ),
            )
        else:
            data["Interval Start"] = pd.to_datetime(
                base_date.normalize()
                + pd.to_timedelta(data["Delivery Hour"] - 1, unit="hour"),
            )

        data["Interval End"] = data["Interval Start"] + pd.Timedelta(
            minutes=minutes_per_interval,
        )

        data = data.rename(
            columns={
                "Energy Loss Price": "Loss",
                "Energy Congestion Price": "Congestion",
                "Pricing Location": "Location",
            },
        )

        numeric_columns = ["LMP", "Loss", "Congestion"]
        for col in numeric_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors="coerce")

        data["Energy"] = data["LMP"] - data["Loss"] - data["Congestion"]

        columns = [
            "Interval Start",
            "Interval End",
            "Location",
            "LMP",
            "Energy",
            "Congestion",
            "Loss",
        ]

        data = (
            data[columns]
            .sort_values(["Interval Start", "Location"])
            .reset_index(drop=True)
        )

        data["Location"] = data["Location"].str.replace(":LMP", "")

        return data

    @support_date_range(frequency="HOUR_START")
    def get_lmp_real_time_5_min_virtual_zonal(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        directory_path = "RealtimeZonalEnergyPrices"
        file_directory = f"{PUBLIC_REPORTS_URL_PREFIX}/{directory_path}"

        if date == "latest":
            url = f"{file_directory}/PUB_{directory_path}.xml"
        else:
            hour = date.hour
            # Hour numbers are 1-24, so we need to add 1
            file_hour = f"{hour + 1}".zfill(2)

            url = f"{file_directory}/PUB_{directory_path}_{date.strftime('%Y%m%d')}{file_hour}.xml"

        xml_content = self._request(url, verbose).text

        soup = BeautifulSoup(xml_content, "xml")

        delivery_date = soup.find("DELIVERYDATE").text
        delivery_hour = int(soup.find("DELIVERYHOUR").text)

        base_datetime = (
            pd.to_datetime(delivery_date) + pd.Timedelta(hours=delivery_hour - 1)
        ).tz_localize(self.default_timezone)

        data_rows = []

        for zone in soup.find_all("TransactionZone"):
            zone_name = zone.find("ZoneName").text

            for interval in zone.find_all("IntervalPrice"):
                interval_num = int(interval.find("Interval").text)

                zonal_price_elem = interval.find("ZonalPrice")
                loss_price_elem = interval.find("EnergyLossPrice")
                cong_price_elem = interval.find("EnergyCongPrice")

                # If any of the prices are null, skip to the next interval
                if (
                    not zonal_price_elem.text.strip()
                    or not loss_price_elem.text.strip()
                    or not cong_price_elem.text.strip()
                ):
                    continue

                zonal_price = float(zonal_price_elem.text)
                loss_price = float(loss_price_elem.text)
                cong_price = float(cong_price_elem.text)

                # Calculate energy price from definition
                energy_price = zonal_price - loss_price - cong_price

                # Subtract 1 from the interval number because it's 1-indexed
                interval_start = base_datetime + pd.Timedelta(
                    minutes=(interval_num - 1) * 5,
                )
                interval_end = interval_start + pd.Timedelta(minutes=5)

                data_rows.append(
                    {
                        "Interval Start": interval_start,
                        "Interval End": interval_end,
                        "Location": zone_name,
                        "LMP": zonal_price,
                        "Energy": energy_price,
                        "Congestion": cong_price,
                        "Loss": loss_price,
                    },
                )

        df = (
            pd.DataFrame(data_rows)
            .sort_values(["Interval Start", "Location"])
            .reset_index(drop=True)
        )

        # Strip out the :HUB from the location
        df["Location"] = df["Location"].str.replace(":HUB", "")

        return df

    @support_date_range(frequency="DAY_START")
    def get_lmp_day_ahead_hourly_virtual_zonal(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get day-ahead zonal virtual LMP data.
        Args:
            date: The date to get the data for.
            end: The end date to get the data for.
            verbose: Whether to print verbose output.
        Returns:
            DataFrame with LMP data.
        """
        directory_path = "DAHourlyZonal"
        file_directory = f"{PUBLIC_REPORTS_URL_PREFIX}/{directory_path}"

        if date == "latest":
            url = f"{file_directory}/PUB_{directory_path}.xml"
        else:
            url = f"{file_directory}/PUB_{directory_path}_{date.strftime('%Y%m%d')}.xml"

        return self._parse_lmp_hourly_virtual_zonal(
            url,
            verbose=verbose,
        )

    @support_date_range(frequency=None)
    def get_lmp_predispatch_hourly_virtual_zonal(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        directory_path = "PredispHourlyZonal"
        file_directory = f"{PUBLIC_REPORTS_URL_PREFIX}/{directory_path}"

        if date == "latest":
            urls = [f"{file_directory}/PUB_{directory_path}.xml"]
        else:
            end = end or (date + pd.Timedelta(hours=1))

            files_and_timestamps = self._get_directory_files_and_timestamps(
                file_directory,
                file_name_prefix=f"PUB_{directory_path}",
            )

            urls = [
                f"{file_directory}/{file}"
                for file, file_time in files_and_timestamps
                if date <= file_time < end
            ]

        if not urls:
            raise NoDataFoundException(
                f"No Predispatch Hourly Virtual Zonal LMP data found for {date} to {end}",
            )

        def process_url(url: str, verbose: bool = False) -> pd.DataFrame:
            return self._parse_lmp_hourly_virtual_zonal(
                url,
                verbose=verbose,
                predispatch=True,
            )

        return self._process_urls_with_threadpool(
            urls,
            process_url,
            f"No valid data found for Predispatch Hourly Virtual Zonal LMP for {date} to {end}",
            verbose=verbose,
        )

    def _parse_lmp_hourly_virtual_zonal(
        self,
        url: str,
        verbose: bool = False,
        predispatch: bool = False,
    ) -> pd.DataFrame:
        xml_content = self._request(url, verbose).text
        soup = BeautifulSoup(xml_content, "xml")

        delivery_date = soup.find("DeliveryDate").text
        base_datetime = (pd.to_datetime(delivery_date)).tz_localize(
            self.default_timezone,
        )

        created_at = pd.Timestamp(soup.find("CreatedAt").text, tz=self.default_timezone)

        data_rows = []

        for zone in soup.find_all("TransactionZone"):
            zone_name = zone.find("ZoneName").text

            components = zone.find_all("Components")

            zonal_prices = {}
            loss_prices = {}
            congestion_prices = {}

            for component in components:
                component_type = component.find("PriceComponent").text

                # Predispatch xml has slightly different tags
                for hour in component.find_all(
                    "DeliveryHour" if not predispatch else "DeliveryHourLMP",
                ):
                    hour_num = int(
                        hour.find("Hour" if not predispatch else "DELIVERY_HOUR").text,
                    )
                    price = float(hour.find("LMP").text)

                    if component_type == "Zonal Price":
                        zonal_prices[hour_num] = price
                    elif component_type == "Energy Loss Price":
                        loss_prices[hour_num] = price
                    elif component_type == "Energy Congestion Price":
                        congestion_prices[hour_num] = price

            # Hours are 1-indexed, so we loop from 1 to 24
            for hour_num in range(1, 25):
                if (
                    hour_num in zonal_prices
                    and hour_num in loss_prices
                    and hour_num in congestion_prices
                ):
                    interval_start = base_datetime + pd.Timedelta(hours=hour_num - 1)
                    interval_end = interval_start + pd.Timedelta(hours=1)

                    lmp = zonal_prices[hour_num]
                    loss = loss_prices[hour_num]
                    congestion = congestion_prices[hour_num]

                    # Calculate energy component from definition
                    energy = lmp - loss - congestion

                    data_rows.append(
                        {
                            "Interval Start": interval_start,
                            "Interval End": interval_end,
                            "Location": zone_name,
                            "LMP": lmp,
                            "Energy": energy,
                            "Congestion": congestion,
                            "Loss": loss,
                        },
                    )

        df = (
            pd.DataFrame(data_rows)
            .sort_values(["Interval Start", "Location"])
            .reset_index(drop=True)
        )

        if predispatch:
            df["Publish Time"] = created_at

            df = utils.move_cols_to_front(
                df,
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "Location",
                ],
            )

        # Strip out the :HUB from the location
        df["Location"] = df["Location"].str.replace(":HUB", "")

        return df

    @support_date_range(frequency="HOUR_START")
    def get_lmp_real_time_5_min_intertie(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        directory_path = "RealTimeIntertieLMP"
        file_directory = f"{PUBLIC_REPORTS_URL_PREFIX}/{directory_path}"

        if date == "latest":
            url = f"{file_directory}/PUB_{directory_path}.xml"
        else:
            hour = date.hour
            # Hour numbers are 1-24, so we need to add 1
            file_hour = f"{hour + 1}".zfill(2)

            url = f"{file_directory}/PUB_{directory_path}_{date.strftime('%Y%m%d')}{file_hour}.xml"

        xml_content = self._request(url, verbose).text

        root = ElementTree.fromstring(xml_content)

        ns = {"": "http://www.ieso.ca/schema"}

        delivery_date = root.find(".//DeliveryDate", ns).text
        delivery_hour = int(root.find(".//DeliveryHour", ns).text)

        base_datetime = (
            pd.to_datetime(delivery_date) + pd.Timedelta(hours=delivery_hour - 1)
        ).tz_localize(self.default_timezone)

        data_rows = []

        intertie_prices = root.findall(".//IntertieLMPrice", ns)

        for intertie_price in intertie_prices:
            location = intertie_price.find("IntertiePLName", ns).text

            components = intertie_price.findall("Components", ns)

            lmp_values = {}
            loss_values = {}
            energy_congestion_values = {}
            external_congestion_values = {}
            # Net Interchange Scheduling Limit
            nisl_values = {}

            for component in components:
                component_type = component.find("LMPComponent", ns).text
                intervals = component.findall("IntervalLMP", ns)

                for interval in intervals:
                    interval_num = interval.find("Interval", ns).text
                    lmp_value_elem = interval.find("LMP", ns)

                    if (
                        lmp_value_elem is None
                        or lmp_value_elem.text is None
                        or lmp_value_elem.text.strip() == ""
                    ):
                        continue

                    lmp_value = float(lmp_value_elem.text)

                    if component_type == "Intertie LMP":
                        lmp_values[interval_num] = lmp_value
                    elif component_type == "Energy Loss Price":
                        loss_values[interval_num] = lmp_value
                    elif component_type == "Energy Congestion Price":
                        energy_congestion_values[interval_num] = lmp_value
                    elif component_type == "External Congestion Price":
                        external_congestion_values[interval_num] = lmp_value
                    elif (
                        component_type
                        == "Net Interchange Scheduling Limit (NISL) Price"
                    ):
                        nisl_values[interval_num] = lmp_value

            for interval_num in lmp_values.keys():
                if (
                    interval_num in loss_values
                    and interval_num in energy_congestion_values
                    and interval_num in external_congestion_values
                    and interval_num in nisl_values
                ):
                    interval_start = base_datetime + pd.Timedelta(
                        minutes=(int(interval_num) - 1) * 5,
                    )
                    interval_end = interval_start + pd.Timedelta(minutes=5)

                    lmp = lmp_values[interval_num]
                    congestion = energy_congestion_values[interval_num]
                    loss = loss_values[interval_num]
                    external_congestion = external_congestion_values[interval_num]
                    nisl_value = nisl_values[interval_num]

                    # Note that inertie LMP includes external congestion and NISL
                    energy = lmp - congestion - loss - external_congestion - nisl_value

                    row = {
                        "Interval Start": interval_start,
                        "Interval End": interval_end,
                        "Location": location,
                        "LMP": lmp,
                        "Energy": energy,
                        "Congestion": congestion,
                        "Loss": loss,
                        "External Congestion": external_congestion,
                        "Interchange Scheduling Limit Price": nisl_value,
                    }

                    data_rows.append(row)

        df = (
            pd.DataFrame(data_rows)
            .sort_values(["Interval Start", "Location"])
            .reset_index(drop=True)
        )

        # Strip out the :LMP from the location
        df["Location"] = df["Location"].str.replace(":LMP", "")

        return df

    @support_date_range(frequency="DAY_START")
    def get_lmp_day_ahead_hourly_intertie(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        directory_path = "DAHourlyIntertieLMP"
        file_directory = f"{PUBLIC_REPORTS_URL_PREFIX}/{directory_path}"

        if date == "latest":
            url = f"{file_directory}/PUB_{directory_path}.xml"
        else:
            url = f"{file_directory}/PUB_{directory_path}_{date.strftime('%Y%m%d')}.xml"

        return self._parse_lmp_hourly_intertie(url, verbose=verbose)

    @support_date_range(frequency=None)
    def get_lmp_predispatch_hourly_intertie(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        directory_path = "PredispHourlyIntertieLMP"
        file_directory = f"{PUBLIC_REPORTS_URL_PREFIX}/{directory_path}"

        if date == "latest":
            urls = [f"{file_directory}/PUB_{directory_path}.xml"]
        else:
            end = end or (date + pd.Timedelta(hours=1))

            # Get all links matching the date and there corresponding last modified time
            files_and_datetimes = self._get_directory_files_and_timestamps(
                file_directory,
                file_name_prefix=f"PUB_{directory_path}",
            )

            urls = [
                f"{file_directory}/{file}"
                for file, date_time in files_and_datetimes
                if date <= date_time <= end
            ]

        if not urls:
            raise NoDataFoundException(
                f"No Predispatch Hourly Intertie LMP data found for {date} to {end}",
            )

        def process_url(url: str, verbose: bool = False) -> pd.DataFrame:
            return self._parse_lmp_hourly_intertie(
                url,
                verbose=verbose,
                predispatch=True,
            )

        return self._process_urls_with_threadpool(
            urls,
            process_url,
            f"No valid data found for Predispatch Hourly Intertie LMP for {date} to {end}",
            verbose=verbose,
        )

    def _parse_lmp_hourly_intertie(
        self,
        url: str,
        verbose: bool = False,
        predispatch: bool = False,
    ) -> pd.DataFrame:
        xml_content = self._request(url, verbose).text
        root = ElementTree.fromstring(xml_content)

        ns = NAMESPACES_FOR_XML.copy()

        delivery_date = root.find(".//DeliveryDate", ns).text
        base_date = pd.Timestamp(delivery_date).tz_localize(self.default_timezone)

        created_at = pd.Timestamp(root.find(".//CreatedAt", ns).text).tz_localize(
            self.default_timezone,
        )

        data_rows = []

        intertie_prices = root.findall(".//IntertieLMPrice", ns)

        for intertie in intertie_prices:
            location = intertie.find("IntertiePLName", ns).text
            components = intertie.findall("Components", ns)

            hourly_lmp = {}
            hourly_loss = {}
            hourly_congestion = {}
            hourly_external_congestion = {}
            hourly_nisl = {}  # Net Interchange Scheduling Limit

            # Process each component group
            for comp in components:
                component_type = comp.find("LMPComponent", ns).text
                hourly_values = comp.findall("HourlyLMP", ns)

                for hour_data in hourly_values:
                    # Note the slight discrepancy between the XML
                    hour_str = "DeliveryHour" if not predispatch else "Hour"

                    hour = int(hour_data.find(hour_str, ns).text)
                    lmp_elem = hour_data.find("LMP", ns)
                    if (
                        lmp_elem is None
                        or lmp_elem.text is None
                        or lmp_elem.text.strip() == ""
                    ):
                        continue

                    value = float(lmp_elem.text)

                    if component_type == "Intertie LMP":
                        hourly_lmp[hour] = value
                    elif component_type == "Energy Loss Price":
                        hourly_loss[hour] = value
                    elif component_type == "Energy Congestion Price":
                        hourly_congestion[hour] = value
                    elif component_type == "External Congestion Price":
                        hourly_external_congestion[hour] = value
                    elif (
                        component_type
                        == "Net Interchange Scheduling Limit (NISL) Price"
                    ):
                        hourly_nisl[hour] = value

            for hour in range(1, 25):
                if hour in hourly_lmp:
                    interval_start = base_date + pd.Timedelta(hours=hour - 1)
                    interval_end = interval_start + pd.Timedelta(hours=1)

                    lmp = hourly_lmp.get(hour, 0)
                    congestion = hourly_congestion.get(hour, 0)
                    loss = hourly_loss.get(hour, 0)
                    external_congestion = hourly_external_congestion.get(hour, 0)
                    nisl_value = hourly_nisl.get(hour, 0)

                    # Note that inertie LMP includes external congestion and NISL
                    energy = lmp - congestion - loss - external_congestion - nisl_value

                    data_rows.append(
                        {
                            "Interval Start": interval_start,
                            "Interval End": interval_end,
                            "Location": location,
                            "LMP": lmp,
                            "Energy": energy,
                            "Congestion": congestion,
                            "Loss": loss,
                            "External Congestion": external_congestion,
                            "Interchange Scheduling Limit Price": nisl_value,
                        },
                    )

        df = (
            pd.DataFrame(data_rows)
            .sort_values(["Interval Start", "Location"])
            .reset_index(drop=True)
        )

        if predispatch:
            # For pre-dispatch, we need to add the publish time
            df["Publish Time"] = created_at
            df = utils.move_cols_to_front(
                df,
                ["Interval Start", "Interval End", "Publish Time", "Location"],
            )

        # Strip out the :LMP from the location
        df["Location"] = df["Location"].str.replace(":LMP", "")

        return df

    @support_date_range(frequency="HOUR_START")
    def get_lmp_real_time_5_min_ontario_zonal(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        directory_path = "RealtimeOntarioZonalPrice"
        file_directory = f"{PUBLIC_REPORTS_URL_PREFIX}/{directory_path}"

        if date == "latest":
            url = f"{file_directory}/PUB_{directory_path}.xml"
        else:
            hour = date.hour
            # Hour numbers are 1-24, so we need to add 1
            file_hour = f"{hour + 1}".zfill(2)

            url = f"{file_directory}/PUB_{directory_path}_{date.strftime('%Y%m%d')}{file_hour}.xml"

        xml_content = self._request(url, verbose).text
        root = ElementTree.fromstring(xml_content)

        ns = NAMESPACES_FOR_XML.copy()

        delivery_date_text = root.find(".//DeliveryDate", ns).text

        # Extract date and hour from the text (e.g., "For 2025-04-23 - Hour 12")
        delivery_date = pd.Timestamp(
            delivery_date_text.split(" - ")[0].replace("For ", ""),
        )
        delivery_hour = int(delivery_date_text.split(" - ")[1].replace("Hour ", ""))

        base_datetime = (
            pd.to_datetime(delivery_date) + pd.Timedelta(hours=delivery_hour - 1)
        ).tz_localize(self.default_timezone)

        price_components = root.findall(".//RealTimePriceComponents", ns)

        zonal_prices = {}
        loss_prices = {}
        congestion_prices = {}

        for component in price_components:
            component_type = component.find("OntarioZonalPrice", ns).text

            # Intervals are 1-indexed, so we loop from 1 to 12
            for interval in range(1, 13):
                interval_element_name = f"OntarioZonalPriceInterval{interval}"
                interval_value_name = f"Interval{interval}"

                interval_element = component.find(interval_element_name, ns)
                if interval_element is not None:
                    interval_value_elem = interval_element.find(interval_value_name, ns)
                    if interval_value_elem is not None and interval_value_elem.text:
                        value = float(interval_value_elem.text)

                        if component_type == "Zonal Price":
                            zonal_prices[interval] = value
                        elif component_type == "Energy Loss Price":
                            loss_prices[interval] = value
                        elif component_type == "Energy Congestion Price":
                            congestion_prices[interval] = value
        data_rows = []

        for interval in range(1, 13):
            if interval in zonal_prices:
                minutes_offset = (interval - 1) * 5
                interval_start = base_datetime + pd.Timedelta(minutes=minutes_offset)
                interval_end = interval_start + pd.Timedelta(minutes=5)

                lmp = zonal_prices.get(interval, 0)
                loss = loss_prices.get(interval, 0)
                congestion = congestion_prices.get(interval, 0)

                energy = lmp - congestion - loss

                data_rows.append(
                    {
                        "Interval Start": interval_start,
                        "Interval End": interval_end,
                        "Location": ONTARIO_LOCATION,
                        "LMP": lmp,
                        "Energy": energy,
                        "Congestion": congestion,
                        "Loss": loss,
                    },
                )

        df = (
            pd.DataFrame(data_rows)
            .sort_values(["Interval Start"])
            .reset_index(drop=True)
        )

        return df

    @support_date_range(frequency="DAY_START")
    def get_lmp_day_ahead_hourly_ontario_zonal(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        directory_path = "DAHourlyOntarioZonalPrice"
        file_directory = f"{PUBLIC_REPORTS_URL_PREFIX}/{directory_path}"

        if date == "latest":
            url = f"{file_directory}/PUB_{directory_path}.xml"
        else:
            url = f"{file_directory}/PUB_{directory_path}_{date.strftime('%Y%m%d')}.xml"

        return self._process_lmp_hourly_ontario_zonal(url, verbose)

    @support_date_range(frequency=None)
    def get_lmp_predispatch_hourly_ontario_zonal(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        directory_path = "PredispHourlyOntarioZonalPrice"
        file_directory = f"{PUBLIC_REPORTS_URL_PREFIX}/{directory_path}"

        if date == "latest":
            urls = [f"{file_directory}/PUB_{directory_path}.xml"]
        else:
            files_and_datetimes = self._get_directory_files_and_timestamps(
                file_directory,
                file_name_prefix=f"PUB_{directory_path}",
            )

            # Default to using 1 hour if no end is provided
            end = end or (date + pd.Timedelta(hours=1))

            urls = [
                f"{file_directory}/{file}"
                for file, date_time in files_and_datetimes
                if date <= date_time <= end
            ]

        if not urls:
            raise NoDataFoundException(
                f"No Predispatch Hourly Ontario Zonal LMP data found for {date} to {end}",
            )

        def process_url(url: str, verbose: bool = False) -> pd.DataFrame:
            return self._process_lmp_hourly_ontario_zonal(
                url,
                verbose,
                predispatch=True,
            )

        return self._process_urls_with_threadpool(
            urls,
            process_url,
            f"No valid data found for Predispatch Hourly Ontario Zonal LMP for {date} to {end}",
            verbose=verbose,
        )

    def _process_lmp_hourly_ontario_zonal(
        self,
        url: str,
        verbose: bool = False,
        predispatch: bool = False,
    ) -> pd.DataFrame:
        xml_content = self._request(url, verbose).text

        root = ElementTree.fromstring(xml_content)
        ns = NAMESPACES_FOR_XML.copy()

        created_at = pd.Timestamp(
            root.find(".//CreatedAt", ns).text,
        ).tz_localize(self.default_timezone)

        delivery_date = root.find(".//DeliveryDate", ns).text

        base_datetime = pd.Timestamp(
            delivery_date,
        ).tz_localize(self.default_timezone)

        data_rows = []

        hourly_components = root.findall(".//HourlyPriceComponents", ns)

        for component in hourly_components:
            hour = _safe_find_int(component, "PricingHour", ns)
            lmp = _safe_find_float(component, "ZonalPrice", ns)
            loss_price = _safe_find_float(component, "LossPriceCapped", ns)
            congestion_price = _safe_find_float(component, "CongestionPriceCapped", ns)

            # Skip if any required values are missing
            if any(val is None for val in [hour, lmp, loss_price, congestion_price]):
                continue

            # Definition of LMP
            energy = lmp - loss_price - congestion_price

            interval_start = base_datetime + pd.Timedelta(hours=hour - 1)
            interval_end = interval_start + pd.Timedelta(hours=1)

            data_rows.append(
                {
                    "Interval Start": interval_start,
                    "Interval End": interval_end,
                    "Location": ONTARIO_LOCATION,
                    "LMP": lmp,
                    "Energy": energy,
                    "Congestion": congestion_price,
                    "Loss": loss_price,
                },
            )

        df = (
            pd.DataFrame(data_rows)
            .sort_values(["Interval Start"])
            .reset_index(drop=True)
        )

        if predispatch:
            df["Publish Time"] = created_at
            df = utils.move_cols_to_front(
                df,
                ["Interval Start", "Interval End", "Publish Time", "Location"],
            )

        return df

    def _process_urls_with_threadpool(
        self,
        urls: list[str],
        process_func: callable,
        error_message: str,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Generic helper to process multiple URLs using ThreadPoolExecutor.

        Args:
            urls: List of URLs to process
            process_func: Function to process each URL (should take url and verbose as args)
            error_message: Error message to show if no data is found
            verbose: Whether to print verbose output

        Returns:
            DataFrame with concatenated results from all URLs
        """
        if not urls:
            raise NoDataFoundException(error_message)

        data_list = []
        with ThreadPoolExecutor(max_workers=min(10, len(urls))) as executor:
            future_to_url = {
                executor.submit(process_func, url, verbose): url for url in urls
            }

            for future in tqdm.tqdm(as_completed(future_to_url), total=len(urls)):
                url = future_to_url[future]
                try:
                    file_data = future.result()
                    data_list.append(file_data)
                except Exception as e:
                    logger.error(f"Error processing {url}: {str(e)}")
                    continue

        if not data_list:
            raise NoDataFoundException(error_message)

        data = pd.concat(data_list)

        # It's possible we may have duplicates since some of the files are the same.
        # We remove these by dropping duplicate rows based on a subset
        data = data.drop_duplicates(
            subset=["Interval Start", "Location", "Publish Time"],
        )

        data = (
            utils.move_cols_to_front(
                data,
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "Location",
                ],
            )
            .sort_values(
                ["Interval Start", "Location", "Publish Time"],
            )
            .reset_index(drop=True)
        )

        return data

    def _get_directory_files_and_timestamps(
        self,
        file_directory: str,
        file_name_prefix: str,
    ):
        html_content = self._request(file_directory, verbose=False).text
        soup = BeautifulSoup(html_content, "html.parser")
        files = []

        for a_tag in soup.find_all("a"):
            href = a_tag.get("href")
            if href and href.startswith(file_name_prefix):
                parent_tr = a_tag.parent
                if parent_tr:
                    # Extract the "Last modified" datetime
                    date_time_text = a_tag.next_sibling
                    if date_time_text:
                        date_time_match = re.search(
                            r"(\d{2}-\w{3}-\d{4} \d{2}:\d{2})",
                            date_time_text,
                        )
                        if date_time_match:
                            date_time_str = date_time_match.group(1)
                            date_time = pd.Timestamp(date_time_str).tz_localize(
                                self.default_timezone,
                            )
                            files.append((href, date_time))

        return sorted(files, key=lambda x: x[1], reverse=True)

    @support_date_range(frequency="DAY_START")
    def get_transmission_outages_planned(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        if date == "latest":
            urls = [
                f"{PUBLIC_REPORTS_URL_PREFIX}/TxOutagesTodayAll/PUB_TxOutagesTodayAll.xml",
                f"{PUBLIC_REPORTS_URL_PREFIX}/TxOutages1to30DaysPlanned/PUB_TxOutages1to30DaysPlanned.xml",
                f"{PUBLIC_REPORTS_URL_PREFIX}/TxOutages31to90DaysPlanned/PUB_TxOutages31to90DaysPlanned.xml",
                f"{PUBLIC_REPORTS_URL_PREFIX}/TxOutages91to180DaysPlanned/PUB_TxOutages91to180DaysPlanned.xml",
                f"{PUBLIC_REPORTS_URL_PREFIX}/TxOutages181to730DaysPlanned/PUB_TxOutages181to730DaysPlanned.xml",
            ]
        else:
            date_fmt = "%Y%m%d"
            urls = [
                # The offset for each file is the minimum days - 1. So the file for
                # 31 to 90 days planned is 30 days from the date, and so on.
                f"{PUBLIC_REPORTS_URL_PREFIX}/TxOutagesTodayAll/PUB_TxOutagesTodayAll_{date.strftime(date_fmt)}.xml",
                f"{PUBLIC_REPORTS_URL_PREFIX}/TxOutages1to30DaysPlanned/PUB_TxOutages1to30DaysPlanned_{date.strftime(date_fmt)}.xml",
                f"{PUBLIC_REPORTS_URL_PREFIX}/TxOutages31to90DaysPlanned/PUB_TxOutages31to90DaysPlanned_{(date + pd.DateOffset(days=30)).strftime(date_fmt)}.xml",
                f"{PUBLIC_REPORTS_URL_PREFIX}/TxOutages91to180DaysPlanned/PUB_TxOutages91to180DaysPlanned_{(date + pd.DateOffset(days=90)).strftime(date_fmt)}.xml",
                f"{PUBLIC_REPORTS_URL_PREFIX}/TxOutages181to730DaysPlanned/PUB_TxOutages181to730DaysPlanned_{(date + pd.DateOffset(days=180)).strftime(date_fmt)}.xml",
            ]

        outage_data = []

        for url in urls:
            xml_content = self._request(url, verbose).text
            root = ElementTree.fromstring(xml_content)

            ns = NAMESPACES_FOR_XML.copy()

            publish_time = pd.Timestamp(root.find(".//CreatedAt", ns).text).tz_localize(
                self.default_timezone,
            )
            outage_requests = root.findall(".//OutageRequest", ns)

            for outage in outage_requests:
                outage_id = outage.find("OutageID", ns).text

                planned_start = pd.Timestamp(
                    outage.find("PlannedStart", ns).text,
                ).tz_localize(
                    self.default_timezone,
                )
                planned_end = pd.Timestamp(
                    outage.find("PlannedEnd", ns).text,
                ).tz_localize(
                    self.default_timezone,
                )

                priority = outage.find("Priority", ns).text
                recurrence = outage.find("Recurrence", ns).text
                recall_time = outage.find("EquipmentRecallTime", ns).text
                status = outage.find("OutageRequestStatus", ns).text

                # Get equipment details
                equipment_list = outage.findall("EquipmentRequested", ns)

                for equipment in equipment_list:
                    name = equipment.find("EquipmentName", ns).text
                    eq_type = equipment.find("EquipmentType", ns).text
                    voltage = equipment.find("EquipmentVoltage", ns).text
                    constraint = equipment.find("ConstraintType", ns).text

                    # Add to data list
                    outage_data.append(
                        {
                            "Interval Start": planned_start,
                            "Interval End": planned_end,
                            "Publish Time": publish_time,
                            "Outage ID": outage_id,
                            "Name": name,
                            "Priority": priority,
                            "Recurrence": recurrence,
                            "Type": eq_type,
                            "Voltage": voltage,
                            "Constraint": constraint,
                            "Recall Time": recall_time,
                            "Status": status,
                        },
                    )

        data = pd.DataFrame(outage_data)

        # There will be overlap between the reports so we need to drop duplicates,
        # keeping the latest publish time
        data = data.sort_values(["Interval Start", "Outage ID", "Publish Time"])

        data = data.drop_duplicates(
            subset=[c for c in data.columns if c != "Publish Time"],
            keep="last",
        ).reset_index(drop=True)

        return data

    @support_date_range(frequency="DAY_START")
    def get_in_service_transmission_limits(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        if date == "latest":
            url = f"{PUBLIC_REPORTS_URL_PREFIX}/TxLimitsAllInService0to34Days/PUB_TxLimitsAllInService0to34Days.xml"
            date = pd.Timestamp.now(tz=self.default_timezone)
        else:
            url = f"{PUBLIC_REPORTS_URL_PREFIX}/TxLimitsAllInService0to34Days/PUB_TxLimitsAllInService0to34Days_{date.strftime('%Y%m%d')}.xml"

        xml_content = self._request(url, verbose).text

        return self._process_transmission_limits(
            xml_content,
        )

    @support_date_range(frequency="DAY_START")
    def get_outage_transmission_limits(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        if date == "latest":
            urls = [
                f"{PUBLIC_REPORTS_URL_PREFIX}/TxLimitsOutage0to2Days/PUB_TxLimitsOutage0to2Days.xml",
                f"{PUBLIC_REPORTS_URL_PREFIX}/TxLimitsOutage3to34Days/PUB_TxLimitsOutage3to34Days.xml",
            ]
        else:
            urls = [
                f"{PUBLIC_REPORTS_URL_PREFIX}/TxLimitsOutage0to2Days/PUB_TxLimitsOutage0to2Days_{date.strftime('%Y%m%d')}.xml",
                f"{PUBLIC_REPORTS_URL_PREFIX}/TxLimitsOutage3to34Days/PUB_TxLimitsOutage3to34Days_{date.strftime('%Y%m%d')}.xml",
            ]

        data_list = []
        for url in urls:
            xml_content = self._request(url, verbose).text
            data_list.append(self._process_transmission_limits(xml_content))

        data = pd.concat(data_list)

        # Drop rows duplicated on every column except Publish Time
        data = data.drop_duplicates(
            subset=[c for c in data.columns if c != "Publish Time"],
            keep="last",
        ).reset_index(drop=True)

        return data

    def _process_transmission_limits(self, xml_content: str) -> pd.DataFrame:
        parser = lxml_etree.XMLParser(remove_blank_text=True)
        tree = lxml_etree.fromstring(xml_content.encode(), parser)

        ns = {"ns": "http://www.ieso.ca/schema"}

        created_at = pd.Timestamp(
            str(tree.xpath("//ns:CreatedAt/text()", namespaces=ns)[0]),
        ).tz_localize(
            self.default_timezone,
        )

        data = []

        facility_types = ["Internal", "Intertie"]

        for facility_type in facility_types:
            xpath = f"//ns:TransmissionFacilityData[ns:TransmissionFacility='{facility_type}']"
            facilities = tree.xpath(xpath, namespaces=ns)

            for facility in facilities:
                # Find all interface data within this facility
                interfaces = facility.xpath("./ns:InterfaceData", namespaces=ns)

                for interface in interfaces:
                    # Extract field values
                    name = interface.xpath("./ns:InterfaceName/text()", namespaces=ns)[
                        0
                    ]
                    issued = pd.Timestamp(
                        str(interface.xpath("./ns:IssueDate/text()", namespaces=ns)[0]),
                    ).tz_localize(
                        self.default_timezone,
                    )
                    start = pd.Timestamp(
                        str(interface.xpath("./ns:StartDate/text()", namespaces=ns)[0]),
                    ).tz_localize(
                        self.default_timezone,
                    )
                    end = pd.Timestamp(
                        str(interface.xpath("./ns:EndDate/text()", namespaces=ns)[0]),
                    ).tz_localize(
                        self.default_timezone,
                    )
                    limit = interface.xpath(
                        "./ns:OperatingLimit/text()",
                        namespaces=ns,
                    )[0]

                    comments_text = interface.xpath(
                        "./ns:Comments/text()",
                        namespaces=ns,
                    )

                    # Explicitly use a string here so we can use comments for the
                    # primary key
                    comments = comments_text[0] if comments_text else "None"

                    data.append(
                        {
                            "Interval Start": start,
                            "Interval End": end,
                            "Publish Time": created_at,
                            "Issue Time": issued,
                            "Type": facility_type,
                            "Facility": name,
                            "Operating Limit": int(limit),
                            "Comments": comments,
                        },
                    )

        df = (
            pd.DataFrame(data)
            .sort_values(["Interval Start", "Publish Time", "Facility"])
            .reset_index(drop=True)
        )

        return df

    @support_date_range(frequency=None)
    def get_load_zonal_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            url = f"{PUBLIC_REPORTS_URL_PREFIX}/RealtimeDemandZonal/PUB_RealtimeDemandZonal.csv"
        else:
            url = f"{PUBLIC_REPORTS_URL_PREFIX}/RealtimeDemandZonal/PUB_RealtimeDemandZonal_{date.year}.csv"

        return self._parse_load_zonal_data(url, date, end)

    @support_date_range(frequency=None)
    def get_load_zonal_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            url = f"{PUBLIC_REPORTS_URL_PREFIX}/DemandZonal/PUB_DemandZonal.csv"
        else:
            url = f"{PUBLIC_REPORTS_URL_PREFIX}/DemandZonal/PUB_DemandZonal_{date.year}.csv"
        return self._parse_load_zonal_data(url, date, end)

    def _parse_load_zonal_data(
        self,
        url: str,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        df = pd.read_csv(url, skiprows=3, parse_dates=["Date"])

        if "Interval" in df.columns:
            df["Interval Start"] = (
                df["Date"]
                + pd.to_timedelta(df["Hour"] - 1, unit="h")
                + pd.to_timedelta((df["Interval"] - 1) * 5, unit="m")
            ).dt.tz_localize(self.default_timezone)
            df["Interval End"] = df["Interval Start"] + pd.Timedelta(minutes=5)
        else:
            df["Interval Start"] = (
                df["Date"] + pd.to_timedelta(df["Hour"] - 1, unit="h")
            ).dt.tz_localize(self.default_timezone)
            df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)
            df.rename(columns={"Zone Total": "Zones Total"}, inplace=True)
        df.columns = df.columns.str.title()
        if date == "latest":
            latest_date = df["Interval Start"].dt.date.max()
            df = df[df["Interval Start"].dt.date == latest_date]
        else:
            if isinstance(date, str):
                date = pd.Timestamp(date, tz=self.default_timezone)
            if end is None:
                mask = (df["Interval Start"] >= date) & (
                    df["Interval Start"] < (date + pd.DateOffset(days=1))
                )
                df = df[mask]
            else:
                mask = (df["Interval Start"] >= date) & (df["Interval Start"] < end)
                df = df[mask]
        return (
            df[ZONAL_LOAD_COLUMNS]
            .sort_values(["Interval Start"])
            .reset_index(drop=True)
        )

    @support_date_range(frequency="HOUR_START")
    def get_real_time_totals(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            url = f"{PUBLIC_REPORTS_URL_PREFIX}/RealtimeTotals/PUB_RealtimeTotals.xml"
        else:
            hour = date.hour
            # Hour numbers are 1-24, so we need to add 1
            file_hour = f"{hour + 1}".zfill(2)

            url = f"{PUBLIC_REPORTS_URL_PREFIX}/RealtimeTotals/PUB_RealtimeTotals_{date.strftime('%Y%m%d')}{file_hour}.xml"

        xml_content = self._request(url, verbose).text

        root = ElementTree.fromstring(xml_content)

        ns = NAMESPACES_FOR_XML.copy()

        # Extract delivery date and hour
        delivery_date = root.find(".//DeliveryDate", ns).text
        delivery_hour = int(root.find(".//DeliveryHour", ns).text)

        base_datetime = (
            pd.to_datetime(delivery_date) + pd.Timedelta(hours=delivery_hour - 1)
        ).tz_localize(self.default_timezone)

        data = []

        for interval_energy in root.findall(".//IntervalEnergy", ns):
            interval = int(interval_energy.find("Interval", ns).text)

            interval_start = base_datetime + pd.Timedelta(minutes=(interval - 1) * 5)
            interval_end = interval_start + pd.Timedelta(minutes=5)

            row = {"Interval Start": interval_start, "Interval End": interval_end}

            for mq in interval_energy.findall("MQ", ns):
                quantity_name = mq.find("MarketQuantity", ns).text
                energy_mw = float(mq.find("EnergyMW", ns).text)

                if quantity_name == "Total Energy":
                    row["Total Energy"] = energy_mw
                elif quantity_name == "Total Loss":
                    row["Total Loss"] = energy_mw
                elif quantity_name == "Total Load":
                    row["Market Total Load"] = energy_mw
                elif quantity_name == "Total Dispatch Load Scheduled OFF":
                    row["Total Dispatchable Load Scheduled Off"] = energy_mw
                elif quantity_name == "Total 10S":
                    row["Total 10S"] = energy_mw
                elif quantity_name == "Total 10N":
                    row["Total 10N"] = energy_mw
                elif quantity_name == "Total 30R":
                    row["Total 30R"] = energy_mw
                elif quantity_name == "ONTARIO DEMAND":
                    row["Ontario Load"] = energy_mw

            # Extract flag
            flag = interval_energy.find("Flag", ns).text
            row["Flag"] = flag

            data.append(row)

        columns = [
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

        # Create DataFrame
        data = (
            pd.DataFrame(data)[columns]
            .sort_values(["Interval Start"])
            .reset_index(drop=True)
        )

        return data

    @support_date_range(frequency="DAY_START")
    def get_solar_embedded_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        vintage: Literal["latest", "all"] = "latest",
        verbose: bool = False,
    ) -> pd.DataFrame:
        json_data_with_times = self._get_variable_generation_forecast_json(
            date,
            end,
            vintage,
        )

        dfs = [
            self._parse_variable_generation_forecast(json_data, last_modified_time)
            for json_data, last_modified_time in json_data_with_times
        ]
        df = pd.concat(dfs).reset_index(drop=True)
        df.drop_duplicates(inplace=True)
        df = df[
            (df["Organization Type"] == "Embedded") & (df["Type"] == "Solar")
        ].reset_index(drop=True)
        df.drop(columns=["Organization Type", "Type"], inplace=True)
        return df

    @support_date_range(frequency="DAY_START")
    def get_wind_embedded_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        vintage: Literal["latest", "all"] = "latest",
        verbose: bool = False,
    ) -> pd.DataFrame:
        json_data_with_times = self._get_variable_generation_forecast_json(
            date,
            end,
            vintage,
        )

        dfs = [
            self._parse_variable_generation_forecast(json_data, last_modified_time)
            for json_data, last_modified_time in json_data_with_times
        ]
        df = pd.concat(dfs).reset_index(drop=True)
        df.drop_duplicates(inplace=True)
        df = df[
            (df["Organization Type"] == "Embedded") & (df["Type"] == "Wind")
        ].reset_index(drop=True)
        df.drop(columns=["Organization Type", "Type"], inplace=True)
        return df

    @support_date_range(frequency="DAY_START")
    def get_solar_market_participant_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        vintage: Literal["latest", "all"] = "latest",
        verbose: bool = False,
    ) -> pd.DataFrame:
        json_data_with_times = self._get_variable_generation_forecast_json(
            date,
            end,
            vintage,
        )

        dfs = [
            self._parse_variable_generation_forecast(json_data, last_modified_time)
            for json_data, last_modified_time in json_data_with_times
        ]
        df = pd.concat(dfs).reset_index(drop=True)
        df.drop_duplicates(inplace=True)
        df = df[
            (df["Organization Type"] == "Market Participant") & (df["Type"] == "Solar")
        ].reset_index(drop=True)
        df.drop(columns=["Organization Type", "Type"], inplace=True)
        return df

    @support_date_range(frequency="DAY_START")
    def get_wind_market_participant_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        vintage: Literal["latest", "all"] = "latest",
        verbose: bool = False,
    ) -> pd.DataFrame:
        json_data_with_times = self._get_variable_generation_forecast_json(
            date,
            end,
            vintage,
        )

        dfs = [
            self._parse_variable_generation_forecast(json_data, last_modified_time)
            for json_data, last_modified_time in json_data_with_times
        ]
        df = pd.concat(dfs).reset_index(drop=True)
        df = df[
            (df["Organization Type"] == "Market Participant") & (df["Type"] == "Wind")
        ].reset_index(drop=True)
        df.drop(columns=["Organization Type", "Type"], inplace=True)
        return df

    def _get_variable_generation_forecast_json(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        vintage: Literal["latest", "all"] = "latest",
    ) -> list[tuple[dict, pd.Timestamp]]:
        """Get variable generation forecast JSON data.

        Args:
            date: The date to get data for
            end: The end date to get data for
            vintage: Whether to get latest or all versions
            verbose: Whether to print verbose output

        Returns:
            List of tuples containing (json_data, last_modified_time)
        """
        logger.info(
            f"Getting variable generation forecast for {date} to {end} for {vintage} vintage...",
        )
        base_url = f"{PUBLIC_REPORTS_URL_PREFIX}/VGForecastSummary"
        if date == "latest":
            file_prefix = "PUB_VGForecastSummary"
        else:
            if isinstance(date, (pd.Timestamp, pd.Timestamp)):
                date_str = date.strftime("%Y%m%d")
            else:
                date_str = date.replace("-", "")

            file_prefix = f"PUB_VGForecastSummary_{date_str}"

        r = self._request(base_url)

        pattern = f'href="({file_prefix}.*?.xml)">.*?</a>\\s+(\\d{{2}}-\\w{{3}}-\\d{{4}} \\d{{2}}:\\d{{2}})'
        files_with_times = re.findall(pattern, r.text)

        if not files_with_times:
            raise FileNotFoundError(
                f"No variable generation forecast files found for date {date_str}",
            )

        if vintage == "latest":
            unversioned_file = next(
                ((f, t) for f, t in files_with_times if "_v" not in f),
                None,
            )

            if unversioned_file:
                file_name, file_time = unversioned_file
            else:
                file_name, file_time = max(
                    files_with_times,
                    key=lambda x: int(x[0].split("_v")[-1].replace(".xml", "")),
                )

            url = f"{base_url}/{file_name}"
            logger.info(f"Getting latest variable generation forecast from {url}...")
            r = self._request(url)
            json_data = xmltodict.parse(r.text)
            last_modified_time = pd.Timestamp(file_time, tz=self.default_timezone)

            return [(json_data, last_modified_time)]

        else:
            json_data_with_times = []

            with ThreadPoolExecutor(
                max_workers=min(10, len(files_with_times)),
            ) as executor:
                future_to_file = {
                    executor.submit(self._fetch_and_parse_file, base_url, file): (
                        file,
                        time,
                    )
                    for file, time in files_with_times
                }

                for future in as_completed(future_to_file):
                    file, time = future_to_file[future]
                    try:
                        json_data = future.result()
                        json_data_with_times.append(
                            (json_data, pd.Timestamp(time, tz=self.default_timezone)),
                        )
                    except Exception as e:
                        logger.error(f"Error processing file {file}: {str(e)}")
            logger.info(
                f"Found {len(json_data_with_times)} variable generation forecast files for {date_str}",
            )
            return json_data_with_times

    def _parse_variable_generation_forecast(
        self,
        json_data: dict,
        last_modified_time: pd.Timestamp,
    ) -> pd.DataFrame:
        document_body = json_data["Document"]["DocBody"]
        publish_time = pd.Timestamp(document_body["ForecastTimeStamp"]).tz_localize(
            self.default_timezone,
        )

        data = []

        for org in document_body["OrganizationData"]:
            org_type = org["OrganizationType"].title()

            for fuel_data in org["FuelData"]:
                fuel_type = fuel_data["FuelType"].title()

                for resource in fuel_data["ResourceData"]:
                    zone = resource["ZoneName"]
                    if zone == "OntarioTotal":
                        zone = "Ontario Total"
                    else:
                        zone = zone.replace("-", " ").title()

                    for forecast in resource["EnergyForecast"]:
                        forecast_date = pd.Timestamp(
                            forecast["ForecastDate"],
                        ).tz_localize(self.default_timezone)

                        intervals = forecast["ForecastInterval"]
                        if not isinstance(intervals, list):
                            intervals = [intervals]

                        for interval in intervals:
                            try:
                                hour = int(interval["ForecastHour"])
                                output = float(interval["MWOutput"])
                            except KeyError:
                                # NB: This logs the error once per file, rather than for each element in the file
                                if not hasattr(self, "_logged_invalid_intervals"):
                                    self._logged_invalid_intervals = set()

                                file_key = f"{publish_time}_{last_modified_time}"
                                if file_key not in self._logged_invalid_intervals:
                                    logger.warning(
                                        f"These files are known to be missing the occasional interval. File published at {publish_time} has a missing interval at {interval}. Continuing with data pull and parse...",
                                    )
                                    self._logged_invalid_intervals.add(file_key)
                                continue

                            interval_start = forecast_date + pd.Timedelta(
                                hours=hour - 1,
                            )
                            interval_end = interval_start + pd.Timedelta(hours=1)

                            data.append(
                                {
                                    "Interval Start": interval_start,
                                    "Interval End": interval_end,
                                    "Publish Time": publish_time,
                                    "Last Modified": last_modified_time,
                                    "Organization Type": org_type,
                                    "Type": fuel_type,
                                    "Zone": zone,
                                    "Generation Forecast": output,
                                },
                            )

        df = pd.DataFrame(data)
        return df.sort_values(
            ["Interval Start", "Publish Time", "Last Modified", "Zone"],
        ).reset_index(drop=True)

    @support_date_range(frequency="HOUR_START")
    def get_lmp_real_time_operating_reserves(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        file_directory = "RealtimeORLMP"

        if date == "latest":
            url = (
                f"{PUBLIC_REPORTS_URL_PREFIX}/{file_directory}/PUB_{file_directory}.csv"
            )
            date = pd.Timestamp.now(tz=self.default_timezone)
        else:
            hour = date.hour
            # Hour numbers are 1-24, so we need to add 1
            file_hour = f"{hour + 1}".zfill(2)

            url = f"{PUBLIC_REPORTS_URL_PREFIX}/{file_directory}/PUB_{file_directory}_{date.strftime('%Y%m%d')}{file_hour}.csv"

        data = pd.read_csv(url, skiprows=1)

        base_datetime = pd.to_datetime(date).normalize()
        data["Interval Start"] = (
            base_datetime
            + pd.to_timedelta(data["Delivery Hour"] - 1, unit="h")
            + 5
            * pd.to_timedelta(
                data["Interval"] - 1,
                unit="m",
            )
        )
        data["Interval End"] = data["Interval Start"] + pd.Timedelta(minutes=5)

        data = data.rename(
            columns={
                "Pricing Location": "Location",
                "Congestion Price 10S": "Congestion 10S",
                "Congestion Price 10N": "Congestion 10N",
                "Congestion Price 30R": "Congestion 30R",
            },
        ).drop(
            columns=[
                "Delivery Hour",
                "Interval",
            ],
        )

        data = (
            utils.move_cols_to_front(
                data,
                ["Interval Start", "Interval End", "Location"],
            )
            .sort_values(
                ["Interval Start", "Location"],
            )
            .reset_index(drop=True)
        )

        return data

    @support_date_range(frequency="DAY_START")
    def get_shadow_prices_real_time_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
        last_modified: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        if last_modified:
            last_modified = utils._handle_date(last_modified, tz=self.default_timezone)
        if date == "latest":
            base_url = f"{PUBLIC_REPORTS_URL_PREFIX}/RealtimeConstrShadowPrices"
            file = "PUB_RealtimeConstrShadowPrices.xml"
            json_data = self._fetch_and_parse_shadow_prices_file(base_url, file)
            df = self._parse_real_time_shadow_prices_report(json_data)
            df.sort_values(
                ["Interval Start", "Publish Time", "Constraint"],
                inplace=True,
            )
            return df[
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "Constraint",
                    "Shadow Price",
                ]
            ].reset_index(drop=True)

        json_data_with_times = self._get_all_shadow_prices_jsons(
            date,
            market="Realtime",
            last_modified=last_modified,
        )
        dfs = []
        for json_data, file_last_modified in json_data_with_times:
            df = self._parse_real_time_shadow_prices_report(json_data)
            df["Last Modified"] = file_last_modified
            dfs.append(df)
        df = pd.concat(dfs)
        df = utils.move_cols_to_front(
            df,
            ["Interval Start", "Interval End", "Publish Time"],
        )
        df.sort_values(
            ["Interval Start", "Publish Time", "Constraint"],
            inplace=True,
        )
        df.drop_duplicates(
            subset=["Interval Start", "Publish Time", "Constraint"],
            inplace=True,
            keep="last",
        )
        return df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Constraint",
                "Shadow Price",
            ]
        ].reset_index(drop=True)

    @support_date_range(frequency="DAY_START")
    def get_shadow_prices_day_ahead_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
        last_modified: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        if last_modified:
            last_modified = utils._handle_date(last_modified, tz=self.default_timezone)
        if date == "latest":
            base_url = f"{PUBLIC_REPORTS_URL_PREFIX}/DAConstrShadowPrices"
            file = "PUB_DAConstrShadowPrices.xml"
            json_data = self._fetch_and_parse_shadow_prices_file(base_url, file)
            df = self._parse_day_ahead_shadow_prices_report(json_data)
            df.sort_values(
                ["Interval Start", "Publish Time", "Constraint"],
                inplace=True,
            )
            return df[
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "Constraint",
                    "Shadow Price",
                ]
            ].reset_index(drop=True)

        json_data_with_times = self._get_all_shadow_prices_jsons(
            date,
            market="DA",
            last_modified=last_modified,
        )
        dfs = []
        for json_data, _ in json_data_with_times:
            df = self._parse_day_ahead_shadow_prices_report(json_data)
            dfs.append(df)
        df = pd.concat(dfs)
        df = utils.move_cols_to_front(
            df,
            ["Interval Start", "Interval End", "Publish Time"],
        )
        df.sort_values(
            ["Interval Start", "Publish Time", "Constraint"],
            inplace=True,
        )
        df.drop_duplicates(
            subset=["Interval Start", "Publish Time", "Constraint"],
            inplace=True,
            keep="last",
        )
        return df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Constraint",
                "Shadow Price",
            ]
        ].reset_index(drop=True)

    def _fetch_and_parse_shadow_prices_file(self, base_url: str, file: str) -> dict:
        url = f"{base_url}/{file}"
        r = self._request(url)
        json_data = xmltodict.parse(r.text)
        return json_data

    def _get_all_shadow_prices_jsons(
        self,
        date: str | datetime.date | datetime.datetime,
        market: Literal["Realtime", "DA"] = "Realtime",
        last_modified: pd.Timestamp | None = None,
    ) -> list[tuple[dict, datetime.datetime]]:
        if market == "Realtime":
            base_url = f"{PUBLIC_REPORTS_URL_PREFIX}/RealtimeConstrShadowPrices"
        else:
            base_url = f"{PUBLIC_REPORTS_URL_PREFIX}/DAConstrShadowPrices"

        if isinstance(date, (datetime.datetime, datetime.date)):
            date_str = date.strftime("%Y%m%d")
        else:
            date_str = date.replace("-", "")
        file_prefix = f"PUB_{market}ConstrShadowPrices_{date_str}"
        r = self._request(base_url)
        pattern = '<a href="({}.*?.xml)">.*?</a>\\s+(\\d{{2}}-\\w{{3}}-\\d{{4}} \\d{{2}}:\\d{{2}})'
        file_rows = re.findall(pattern.format(file_prefix), r.text)
        if not file_rows:
            raise FileNotFoundError(f"No shadow price files found for date {date_str}")
        if last_modified:
            filtered_files = [
                (file, time)
                for file, time in file_rows
                if pd.Timestamp(time, tz=self.default_timezone) >= last_modified
            ]
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
                executor.submit(
                    self._fetch_and_parse_shadow_prices_file,
                    base_url,
                    file,
                ): (file, time)
                for file, time in filtered_files
            }
            for future in as_completed(future_to_file):
                file, time = future_to_file[future]
                retries = 0
                while retries < max_retries:
                    try:
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

    def _parse_day_ahead_shadow_prices_report(self, json_data: dict) -> pd.DataFrame:
        doc_header = json_data["Document"]["DocHeader"]
        doc_body = json_data["Document"]["DocBody"]
        shadow_prices = doc_body["HourlyPrice"]
        publish_time = pd.Timestamp(doc_header["CreatedAt"], tz=self.default_timezone)
        delivery_date = pd.Timestamp(doc_body["DELIVERYDATE"], tz=self.default_timezone)

        rows = []
        for shadow_price in shadow_prices:
            constraint = " ".join(shadow_price["ConstraintName"].split())
            hours = shadow_price["ShadowPrices"]["Hour"]
            prices = shadow_price["ShadowPrices"]["ShadowPrice"]

            for hour, price in zip(hours, prices):
                interval_start = delivery_date + pd.Timedelta(hours=int(hour) - 1)
                interval_end = interval_start + pd.Timedelta(hours=1)
                rows.append(
                    {
                        "Interval Start": interval_start,
                        "Interval End": interval_end,
                        "Publish Time": publish_time,
                        "Constraint": constraint,
                        "Shadow Price": float(price),
                    },
                )
        return pd.DataFrame(rows)

    def _parse_real_time_shadow_prices_report(self, json_data: dict) -> pd.DataFrame:
        doc_header = json_data["Document"]["DocHeader"]
        doc_body = json_data["Document"]["DocBody"]
        publish_time = pd.Timestamp(doc_header["CreatedAt"], tz=self.default_timezone)
        delivery_date = pd.Timestamp(doc_body["DELIVERYDATE"], tz=self.default_timezone)
        rows = []

        # NB: Handle the case where there is no hourly price data in the report
        if "HourlyPrice" not in doc_body or not doc_body["HourlyPrice"]:
            logger.debug(f"No hourly price data in report for {delivery_date}")
            return pd.DataFrame(
                {
                    "Interval Start": pd.Series(dtype="datetime64[ns, EST]"),
                    "Interval End": pd.Series(dtype="datetime64[ns, EST]"),
                    "Publish Time": pd.Series(dtype="datetime64[ns, EST]"),
                    "Constraint": pd.Series(dtype="string"),
                    "Shadow Price": pd.Series(dtype="float64"),
                },
            )

        for hourly in doc_body["HourlyPrice"]:
            constraint = " ".join(hourly["ConstraintName"].split())
            hour = int(hourly["DeliveryHour"])
            intervals = hourly["IntervalShadowPrices"]["Interval"]
            prices = hourly["IntervalShadowPrices"]["ShadowPrice"]
            for interval, price in zip(intervals, prices):
                interval_num = int(interval)
                interval_start = (
                    delivery_date
                    + pd.Timedelta(hours=hour - 1)
                    + pd.Timedelta(minutes=(interval_num - 1) * 5)
                )
                interval_end = interval_start + pd.Timedelta(minutes=5)
                rows.append(
                    {
                        "Interval Start": interval_start,
                        "Interval End": interval_end,
                        "Publish Time": publish_time,
                        "Constraint": constraint,
                        "Shadow Price": float(price),
                    },
                )
        df = pd.DataFrame(rows)
        return df
