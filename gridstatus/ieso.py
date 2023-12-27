import datetime
import time
import xml.etree.ElementTree as ET

import pandas as pd
import requests

from gridstatus import utils
from gridstatus.base import ISOBase, NotSupported
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import log

# Load hourly files go back 30 days
MAXIMUM_DAYS_IN_PAST_FOR_LOAD = 30
LOAD_INDEX_URL = "http://reports.ieso.ca/public/RealtimeConstTotals/"

# Each load file covers one hour. We have to use the xml instead of the csv because
# the csv does not have demand for Ontario.
LOAD_TEMPLATE_URL = f"{LOAD_INDEX_URL}/PUB_RealtimeConstTotals_YYYYMMDDHH.xml"


LOAD_FORECAST_INDEX_URL = "http://reports.ieso.ca/public/OntarioZonalDemand/"

# Each forecast file contains data from the day in the filename going forward for
# 34 days. The most recent file does not have a date in the filename.
LOAD_FORECAST_TEMPLATE_URL = (
    f"{LOAD_FORECAST_INDEX_URL}/PUB_OntarioZonalDemand_YYYYMMDD.xml"
)

# The farthest in the past that forecast files are available
MAXIMUM_DAYS_IN_PAST_FOR_LOAD_FORECAST = 90
# The farthest in the future that forecasts are available. Note that there are not
# files for these future forecasts, they are in the current day's file.
MAXIMUM_DAYS_IN_FUTURE_FOR_LOAD_FORECAST = 34


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

    def get_5_min_load(self, date, end=None, verbose=False):
        """
        Get 5 minute load for a given date or from date to end date.

        Args:
            date (datetime.date | datetime.datetime | str): The date to get the load for
                Can be a `datetime.date` or `datetime.datetime` object, or a string
                with the values "today" or "latest". If `end` is None, returns
                only data for this date.
            end (datetime.date | datetime.datetime, optional): End date. Defaults None
                If provided, returns data from `date` to `end` date. The `end` can be a
                `datetime.date` or `datetime.datetime` object.
            verbose (bool, optional): Print verbose output. Defaults to False.

        """
        # Return data from the earliest interval today to the latest interval today
        if date in ["today", "latest"]:
            date = pd.Timestamp(self._today()).replace(hour=0, minute=0)
            end = pd.Timestamp.now(tz=self.default_timezone)

        # If given a date string or plain date set date to the earliest interval
        # and end to the latest interval on the date
        if isinstance(date, str) or (
            isinstance(date, datetime.date) and not isinstance(date, datetime.datetime)
        ):
            date = pd.to_datetime(date).replace(hour=0, minute=0)
            end = pd.to_datetime(date).replace(hour=23, minute=59)

        if date.date() > self._today():
            raise NotSupported(
                "Load data is not available for future dates.",
            )

        if date.date() < self._today() - pd.Timedelta(
            days=MAXIMUM_DAYS_IN_PAST_FOR_LOAD,
        ):
            raise NotSupported(
                f"Load data is not available for dates more than "
                f"{MAXIMUM_DAYS_IN_PAST_FOR_LOAD} days in the past.",
            )

        return self._retrieve_5_minute_load(date, end, verbose)

    @support_date_range(frequency="HOUR_START")
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
        created_at = root.find("DocHeader/CreatedAt", NAMESPACES_FOR_XML).text

        df["Delivery Date"] = pd.Timestamp(delivery_date, tz=self.default_timezone)

        # The starting hour is 1, so we subtract 1 to get the hour in the range 0-23
        df["Delivery Hour Start"] = delivery_hour - 1
        # Multiply the interval minus 1 by 5 to get the minutes in the range 0-55
        df["Interval Minute Start"] = MINUTES_INTERVAL * (df["Interval"] - 1)
        df["Published Time"] = pd.Timestamp(created_at, tz=self.default_timezone)

        df["Interval Start"] = (
            df["Delivery Date"]
            + pd.to_timedelta(df["Delivery Hour Start"], unit="h")
            + pd.to_timedelta(df["Interval Minute Start"], unit="m")
        )

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(
            minutes=MINUTES_INTERVAL,
        )

        cols_to_keep_in_order = [
            "Interval Start",
            "Interval End",
            "Published Time",
            "Market Total Load",
            "Ontario Load",
        ]

        df = utils.move_cols_to_front(df, cols_to_keep_in_order)[cols_to_keep_in_order]

        if end:
            return df[df["Interval End"] <= pd.Timestamp(end)]

        return df

    def get_load(self, date, end=None, verbose=False):
        """
        Get hourly load for the Market and Ontario for a given date or from
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


        Returns:
            pd.DataFrame: zonal load as a wide table with columns for each zone
        """

        data_five_minutes = self.get_5_min_load(date, end, verbose)

        # Hourly demand is the average over the 5 minute intervals within each hour
        df = data_five_minutes.groupby(
            [
                data_five_minutes["Interval Start"].dt.date,
                data_five_minutes["Interval Start"].dt.hour,
            ],
        ).agg(
            {
                "Market Total Load": "mean",
                "Ontario Load": "mean",
                "Interval Start": "min",
            },
        )

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=HOUR_INTERVAL)

        df = utils.move_cols_to_front(
            df,
            [
                "Interval Start",
                "Interval End",
                "Market Total Load",
                "Ontario Load",
            ],
        )

        return df.reset_index(drop=True)

    @support_date_range(frequency="DAY_START")
    def get_load_forecast(self, date, end=None, verbose=False):
        """
        Get forecasted load by forecast zone (Ontario, East, West) for a given date
        or from date to end date.

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
        today = self._today()
        date = self._handle_input_date(date)

        date_only = date.date()

        if date_only < today - pd.Timedelta(
            days=MAXIMUM_DAYS_IN_PAST_FOR_LOAD_FORECAST,
        ):
            # Forecasts are not support for past dates
            raise NotSupported(
                "Past dates are not support for load forecasts more than"
                f"{MAXIMUM_DAYS_IN_PAST_FOR_LOAD_FORECAST} days in the past.",
            )

        if date_only > today + pd.Timedelta(
            days=MAXIMUM_DAYS_IN_FUTURE_FOR_LOAD_FORECAST,
        ):
            raise NotSupported(
                f"Dates more than {MAXIMUM_DAYS_IN_FUTURE_FOR_LOAD_FORECAST}"
                "days in the future are not supported for load forecasts.",
            )

        # For future dates, the most recent forecast is used
        if date_only > today:
            url = LOAD_FORECAST_TEMPLATE_URL.replace("_YYYYMMDD", "")
        else:
            url = LOAD_FORECAST_TEMPLATE_URL.replace(
                "YYYYMMDD",
                date.strftime("%Y%m%d"),
            )

        r = self._request(url, verbose)

        # Define the NAMESPACES_FOR_XML used in the XML document
        NAMESPACES_FOR_XML = {"": "http://www.ieso.ca/schema"}

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
            df["DeliveryDate"] + pd.to_timedelta(df["DeliveryHour"], unit="h")
        ).dt.tz_localize(self.default_timezone)

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=HOUR_INTERVAL)

        # Pivot the table to wide
        pivot_df = df.pivot_table(
            index=["Interval Start", "Interval End"],
            columns="Zone",
            values="EnergyMW",
            aggfunc="first",
        ).reset_index()

        pivot_df["Published Time"] = pd.Timestamp(
            published_time,
            tz=self.default_timezone,
        )

        pivot_df = utils.move_cols_to_front(
            pivot_df,
            [
                "Interval Start",
                "Interval End",
                "Published Time",
                "Ontario",
            ],
        )

        pivot_df.columns.name = None

        col_mapper = {
            col: f"{col} Load Forecast" for col in ["Ontario", "East", "West"]
        }

        pivot_df = pivot_df.rename(columns=col_mapper)

        # If no end is provided, return data from single date
        if not end:
            return pivot_df[
                pivot_df["Interval Start"].dt.date == date_only
            ].reset_index(drop=True)

        end_date_only = pd.to_datetime(end).date()

        return pivot_df[
            (pivot_df["Interval Start"].dt.date >= date_only)
            & (pivot_df["Interval End"].dt.date <= end_date_only)
        ].reset_index(drop=True)

    # TODO add fuel mix. http://reports.ieso.ca/public/GenOutputbyFuelHourly/
    def get_fuel_mix(self, date, end=None, verbose=False):
        pass

    def _today(self):
        return pd.Timestamp.now(tz=self.default_timezone).date()

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

    def _handle_input_date(self, date):
        if date in ["latest", "today"]:
            return pd.to_datetime(self._today())
        else:
            try:
                date = pd.to_datetime(date)

            except ValueError:
                raise ValueError(
                    f"Invalid date: {date}. Must be a valid date string, a "
                    "valid datetime, or one of'latest', 'today'.",
                )
            return pd.to_datetime(date)

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
