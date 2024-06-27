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

"""REAL TIME FUEL MIX CONSTANTS"""
FUEL_MIX_INDEX_URL = "http://reports.ieso.ca/public/GenOutputCapability/"

# Updated every hour and each file has data for one day.
# The most recent version does not have the date in the filename.
FUEL_MIX_TEMPLATE_URL = f"{FUEL_MIX_INDEX_URL}/PUB_GenOutputCapability_YYYYMMDD.xml"

# Number of past days for which the complete generator report is available.
# Before this date, only total by fuel type is available.
MAXIMUM_DAYS_IN_PAST_FOR_COMPLETE_GENERATOR_REPORT = 90

"""HISTORICAL FUEL MIX CONSTANTS"""
HISTORICAL_FUEL_MIX_INDEX_URL = "http://reports.ieso.ca/public/GenOutputbyFuelHourly/"

# Updated once a day and each file contains data for an entire year.
HISTORICAL_FUEL_MIX_TEMPLATE_URL = (
    f"{HISTORICAL_FUEL_MIX_INDEX_URL}/PUB_GenOutputbyFuelHourly_YYYY.xml"
)


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

    def get_fuel_mix(self, date, end=None, verbose=False):
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

    def get_generator_report_hourly(self, date, end=None, verbose=False):
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
    def _retrieve_fuel_mix(self, date, end=None, verbose=False):
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
    def _retrieve_historical_fuel_mix(self, date, end=None, verbose=False):
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
            r = requests.get(url, verify=False)

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
