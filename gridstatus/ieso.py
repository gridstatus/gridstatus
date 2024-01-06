import time
import xml.etree.ElementTree as ET

import pandas as pd
import requests

from gridstatus import utils
from gridstatus.base import ISOBase, NotSupported
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import log

"""LOAD CONSTANTS"""
# Load hourly files go back 30 days
MAXIMUM_DAYS_IN_PAST_FOR_LOAD = 30
LOAD_INDEX_URL = "http://reports.ieso.ca/public/RealtimeConstTotals"

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
ZONAL_LOAD_FORECAST_INDEX_URL = "http://reports.ieso.ca/public/OntarioZonalDemand"

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

"""FUEL MIX CONSTANTS"""
FUEL_MIX_INDEX_URL = "http://reports.ieso.ca/public/GenOutputCapability/"

# Updated every hour. The most recent version does not have the date in the filename.\
FUEL_MIX_TEMPLATE_URL = f"{FUEL_MIX_INDEX_URL}/PUB_GenOutputCapability_YYYYMMDD.xml"


MINUTES_INTERVAL = 5
HOUR_INTERVAL = 1

# Default namespace used in the XML files
NAMESPACES_FOR_XML = {"": "http://www.ieso.ca/schema"}


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
    def get_load(self, date, end=None, verbose=False):
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

    def _retrieve_5_minute_load(self, date, end=None, verbose=False):
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

    def get_load_forecast(self, date, verbose=False):
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
            freq="H",
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
    def get_zonal_load_forecast(self, date, end=None, verbose=False):
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
            df["DeliveryDate"]
            + pd.to_timedelta(df["DeliveryHour"] - 1, unit="h")
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

    @support_date_range(frequency="DAY_START")
    def get_fuel_mix(self, date, end=None, verbose=False):
        url = FUEL_MIX_TEMPLATE_URL.replace(
            "YYYYMMDD",
            date.strftime("%Y%m%d"),
        )

        r = self._request(url, verbose)

        root = ET.fromstring(r.content)

        # Define the namespace map
        ns = {"": "http://www.theIMO.com/schema"}

        # Extracting the 'date' from the XML file
        # (it is a separate element outside the 'Generator' elements)
        date = root.find(".//Date", ns).text

        # Re-parsing the 'Generator' elements with the correct 'date'
        data = []

        for gen in root.findall(".//Generator", ns):
            generator_name = (
                gen.find("GeneratorName", ns).text
                if gen.find("GeneratorName", ns) is not None
                else None
            )
            fuel_type = (
                gen.find("FuelType", ns).text
                if gen.find("FuelType", ns) is not None
                else None
            )

            # Extracting 'Output' and 'Capability' data
            for output in gen.findall("Outputs/Output", ns):
                hour = (
                    output.find("Hour", ns).text
                    if output.find("Hour", ns) is not None
                    else None
                )
                energy_mw = (
                    output.find("EnergyMW", ns).text
                    if output.find("EnergyMW", ns) is not None
                    else None
                )

                # For SOLAR/WIND, 'Capability' data is under 'AvailCapacity' tag
                if fuel_type in ["SOLAR", "WIND"]:
                    capability = gen.find(
                        f".//Capacities/AvailCapacity[Hour='{hour}']",
                        ns,
                    )
                    forecast_energy_mw = (
                        gen.find(f".//Capabilities/Capability[Hour='{hour}']", ns)
                        .find("EnergyMW", ns)
                        .text
                    )
                else:
                    capability = gen.find(
                        f".//Capabilities/Capability[Hour='{hour}']",
                        ns,
                    )
                    forecast_energy_mw = None

                capability_energy_mw = (
                    capability.find("EnergyMW", ns).text
                    if capability is not None
                    else None
                )

                data.append(
                    [
                        date,
                        hour,
                        generator_name,
                        fuel_type,
                        energy_mw,
                        capability_energy_mw,
                        forecast_energy_mw,
                    ],
                )

        columns = [
            "Date",
            "Hour",
            "Generator Name",
            "Fuel Type",
            "Output MW",
            "Capability MW",
            "Forecast MW",
        ]

        # Creating the DataFrame with the correct date
        df = pd.DataFrame(data, columns=columns)
        df.head()

    # Function to extract data for a specific Market Quantity considering namespace
    def _extract_load_in_market_quantity(
        self,
        market_quantity_element,
        market_quantity_name,
    ):
        for mq in market_quantity_element.findall("MQ", NAMESPACES_FOR_XML):
            market_quantity = mq.find("MarketQuantity", NAMESPACES_FOR_XML).text

            if market_quantity_name in market_quantity:
                return mq.find("EnergyMW", NAMESPACES_FOR_XML).text

        return None

    # Function to find all triples of 'Interval', 'Market Total Load', and
    # 'Ontario Load' in the XML file
    def _find_loads_at_each_interval_from_xml(self, root_element):
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

    def _request(self, url, verbose):
        msg = f"Fetching URL: {url}"
        log(msg, verbose)

        max_retries = 3
        retry_num = 0
        sleep = 5

        while retry_num < max_retries:
            r = requests.get(url)

            if r.ok:
                break

            retry_num += 1
            print(f"Request failed. Error: {r.reason}. Retrying {retry_num}...")

            time.sleep(sleep)

            # Exponential backoff
            sleep *= 2

        if not r.ok:
            raise Exception(
                f"Failed to retrieve data from {url} in {max_retries} tries.",
            )

        return r
