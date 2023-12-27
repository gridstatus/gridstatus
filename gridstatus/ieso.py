import io
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


MINUTES_INTERVAL_DURATION = pd.Timedelta(minutes=5)
INTERVAL_DURATION = pd.Timedelta(hours=1)


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
        r = self._request(
            LOAD_TEMPLATE_URL.replace("YYYYMMDDHH", date.strftime("%Y%m%d%H")), verbose
        )

        root = ET.fromstring(r.text)

        # Namespace handling
        namespaces = {"ns": "http://www.ieso.ca/schema"}

        # Function to extract data for a specific Market Quantity considering namespace
        def extract_data_for_market_quantity_with_ns(mq_element, market_quantity_name):
            for mq in mq_element.findall("ns:MQ", namespaces):
                market_quantity = mq.find("ns:MarketQuantity", namespaces).text
                if market_quantity_name in market_quantity:
                    return mq.find("ns:EnergyMW", namespaces).text
            return None

        # Function to find all triples of 'Interval', 'Market Total Load', and
        # 'Ontario Demand' in the XML file
        def find_all_intervals_loads_and_demands(root_element):
            interval_load_demand_triples = []
            for interval_energy in root_element.findall(
                "ns:DocBody/ns:Energies/ns:IntervalEnergy", namespaces
            ):
                interval = interval_energy.find("ns:Interval", namespaces).text
                market_total_load = extract_data_for_market_quantity_with_ns(
                    interval_energy, "Total Energy"
                )
                ontario_demand = extract_data_for_market_quantity_with_ns(
                    interval_energy, "ONTARIO DEMAND"
                )

                if market_total_load and ontario_demand:
                    interval_load_demand_triples.append(
                        [int(interval), float(market_total_load), float(ontario_demand)]
                    )

            return interval_load_demand_triples

        # Extracting all triples of Interval, Market Total Load, and Ontario Demand
        all_intervals_loads_and_demands = find_all_intervals_loads_and_demands(root)

        # Creating a DataFrame from the triples
        df = pd.DataFrame(
            all_intervals_loads_and_demands,
            columns=["Interval", "Market Total Load", "Ontario Load"],
        )

        # Extracting the 'Delivery Date', 'Delivery Hour', and 'Created At' values
        delivery_date = root.find("ns:DocBody/ns:DeliveryDate", namespaces).text
        delivery_hour = int(root.find("ns:DocBody/ns:DeliveryHour", namespaces).text)
        created_at = root.find("ns:DocHeader/ns:CreatedAt", namespaces).text

        # Adding 'Delivery Date', 'Delivery Hour', and 'Created At' columns
        df["Delivery Date"] = pd.Timestamp(delivery_date)
        df["Delivery Hour"] = delivery_hour - 1
        df["Published Time"] = pd.Timestamp(created_at, tz=self.default_timezone)

        df["Interval Start"] = (
            df["Delivery Date"]
            + pd.to_timedelta(df["Delivery Hour"], unit="h")
            + 5 * pd.to_timedelta(df["Interval"] - 1, unit="min")
        )

        df["Interval End"] = df["Interval Start"] + MINUTES_INTERVAL_DURATION

        return df

    @support_date_range(frequency="YEAR_START")
    def get_load(self, date, end=None, verbose=False):
        """
        Get hourly load by zone for a given date or from date to end date.

        Args:
            date (datetime.date): date to get load for. If end is None, returns
                only data for this date.
            end (datetime.date, optional): end date. Defaults to None. If provided,
                returns data from date to end date.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: zonal load as a wide table with columns for each zone
        """

        data_five_minutes = self.get_5_min_load(date, end, verbose)

        data = data_five_minutes.groupby("Delivery Hour")[
            "Market Total Load", "Ontario Load"
        ].mean()

        data["Interval Start"] = data_five_minutes["Interval Start"].min()
        data["Interval End"] = data_five_minutes["Interval End"].max()

        today = self._today()
        earliest_date_with_load = today - pd.Timedelta(
            days=MAXIMUM_DAYS_IN_PAST_FOR_LOAD,
        )
        date = self._handle_input_date(date)

        date_only = date.date()

        if date_only < earliest_date_with_load:
            raise NotSupported(
                f"Load is not available before {earliest_date_with_load}."
            )

        if date_only > today:
            raise NotSupported("Load is not available for future dates.")

        year = date.year
        url = LOAD_TEMPLATE_URL.replace("YYYY", str(year))

        r = self._request(url, verbose)

        # Create a dataframe from the CSV file, skipping lines starting with \\
        df = pd.read_csv(io.StringIO(r.text), comment="\\")

        df["Interval Start"] = (
            pd.to_datetime(df["Date"]) + pd.to_timedelta(df["Hour"], unit="h")
        ).dt.tz_localize(self.default_timezone)

        df["Interval End"] = df["Interval Start"] + INTERVAL_DURATION

        df = utils.move_cols_to_front(df, ["Interval Start", "Interval End"])
        cols_to_drop = ["Date", "Hour", "Diff"]
        df = df.drop(cols_to_drop, axis=1)

        col_mapper = {
            col: f"{col} Load"
            for col in df.columns
            if col not in ["Interval Start", "Interval End"]
        }

        df = df.rename(columns=col_mapper)

        # If no end is provided, return data from single date
        if not end:
            return df[df["Interval Start"].dt.date == date_only].reset_index(drop=True)

        end_date_only = pd.to_datetime(end).date()

        return df[
            (df["Interval Start"].dt.date >= date_only)
            & (df["Interval End"].dt.date <= end_date_only)
        ].reset_index(drop=True)

    @support_date_range(frequency="DAY_START")
    def get_load_forecast(self, date, end=None, verbose=False):
        """
        Get forecasted load by forecast zone (Ontario, East, West) for a given date
        or from date to end date.

        Supports data 90 days into the past and up to 34 days into the future.

        Args:
            date (datetime.date): date to get load for. If end is None, returns
                only data for this date.
            end (datetime.date, optional): end date. Defaults to None. If provided,
                returns data from date to end date.
            verbose (bool, optional): print verbose output. Defaults to False.

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

        # Define the namespaces used in the XML document
        namespaces = {"": "http://www.ieso.ca/schema"}

        # Initialize a list to store the parsed data
        data = []

        # Parse the XML file
        root = ET.fromstring(r.content)

        published_time = root.find(".//CreatedAt", namespaces).text

        # Extracting data for each ZonalDemands within the Document
        for zonal_demands in root.findall(".//ZonalDemands", namespaces):
            delivery_date = zonal_demands.find(".//DeliveryDate", namespaces).text

            for zonal_demand in zonal_demands.findall(".//ZonalDemand/*", namespaces):
                # The zone name is the tag name without the namespace
                zone_name = zonal_demand.tag[(zonal_demand.tag.rfind("}") + 1) :]

                for demand in zonal_demand.findall(".//Demand", namespaces):
                    hour = demand.find(".//DeliveryHour", namespaces).text
                    energy_mw = demand.find(".//EnergyMW", namespaces).text

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

        df["Interval End"] = df["Interval Start"] + INTERVAL_DURATION

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
