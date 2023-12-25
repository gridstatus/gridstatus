import io
import time
import xml.etree.ElementTree as ET

import pandas as pd
import requests

from gridstatus import utils
from gridstatus.base import ISOBase, NotSupported
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import log

FORECAST_DEMAND_INDEX_URL = "http://reports.ieso.ca/public/OntarioZonalDemand/"

# Each forecast file contains data from the day forward 34 days.
# The most recent file does not have a date in the filename.
FORECASTED_DEMAND_TEMPLATE_URL = (
    f"{FORECAST_DEMAND_INDEX_URL}/PUB_OntarioZonalDemand_YYYYMMDD.xml"
)

MAXIMUM_DAYS_IN_PAST_FOR_FORECAST = 90
MAXIMUM_DAYS_IN_FUTURE_FOR_FORECAST = 34

# Actual demand goes back several decades, with each year in one file.
EARLIEST_ACTUAL_DEMAND = "2002-05-01"
ACTUAL_DEMAND_INDEX_URL = "http://reports.ieso.ca/public/DemandZonal/"

# Specifying the year is enough because the yearly file is updated daily.
ACTUAL_DEMAND_TEMPLATE_URL = f"{ACTUAL_DEMAND_INDEX_URL}/PUB_DemandZonal_YYYY.csv"


class Ieso(ISOBase):
    """Independent Electricity System Operator (IESO)"""

    name = "Independent Electricity System Operator"
    iso_id = "ieso"

    # All data is provided in EST, and does not change with DST. This means there are
    # no repeated or missing hours in the raw data and we can safely use tz_localize
    # without setting ambiguous or nonexistent times.
    # https://www.ieso.ca/-/media/Files/IESO/Document-Library/engage/ca/ca-Introduction-to-the-Capacity-Auction.ashx
    default_timezone = "EST"

    @support_date_range(frequency="DAY_START")
    def get_hourly_forecasted_demand(self, date, end=None, verbose=False):
        """
        Get load by forecast zone for a given date.
        Supports data 90 days into the past and up to 34 days into the future.

        Args:
            date (datetime.date): date to get load for
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: load for given date
        """
        today = pd.Timestamp.now(tz=self.default_timezone).date()

        if date in ["latest", "today"]:
            date = pd.to_datetime(today)
        else:
            date = pd.to_datetime(date)

        date_only = date.date()

        if date_only < today - pd.Timedelta(days=MAXIMUM_DAYS_IN_PAST_FOR_FORECAST):
            # Forecasts are not support for past dates
            raise NotSupported(
                "Past dates are not support for forecasted demand more than"
                f"{MAXIMUM_DAYS_IN_PAST_FOR_FORECAST} days in the past.",
            )

        if date_only > today + pd.Timedelta(days=MAXIMUM_DAYS_IN_FUTURE_FOR_FORECAST):
            raise NotSupported(
                f"Dates more than {MAXIMUM_DAYS_IN_FUTURE_FOR_FORECAST}"
                "days in the future are not supported.",
            )

        # For future dates, the most recent forecast is used
        if date_only > today:
            url = FORECASTED_DEMAND_TEMPLATE_URL.replace("_YYYYMMDD", "")
        else:
            url = FORECASTED_DEMAND_TEMPLATE_URL.replace(
                "YYYYMMDD",
                date.strftime("%Y%m%d"),
            )

        msg = f"Fetching URL: {url}"
        log(msg, verbose)

        retry_num = 0
        sleep = 5

        while retry_num < 3:
            r = requests.get(url)

            if r.ok:
                break

            retry_num += 1
            print(
                f"Failed to get data from CAISO. Error: {r.reason}. "
                f"Retrying {retry_num}...",
            )
            time.sleep(sleep)

            # Exponential backoff
            sleep *= 2

        if not r.ok:
            raise Exception(f"Failed to get data from IESO. Error: {r.reason}")

        # Define the namespaces used in the XML document
        namespaces = {"": "http://www.ieso.ca/schema"}

        # Initialize a list to store the parsed data
        data = []

        # Parse the XML file
        root = ET.fromstring(r.content)

        published_time = root.findall(".//CreatedAt", namespaces)[0].text

        # Extracting data for each ZonalDemands within the Document
        for zonal_demands in root.findall(".//ZonalDemands", namespaces):
            # Extract the DeliveryDate for the ZonalDemands
            delivery_date = zonal_demands.find(".//DeliveryDate", namespaces).text
            # Loop through each ZonalDemand within ZonalDemands
            for zonal_demand in zonal_demands.findall(".//ZonalDemand/*", namespaces):
                # The zone name is the tag name without the namespace
                zone_name = zonal_demand.tag[
                    zonal_demand.tag.rfind("}") + 1 :
                ]  # Extract the local name of the zone
                # Now, loop through each Demand element within the ZonalDemand
                for demand in zonal_demand.findall(".//Demand", namespaces):
                    hour = demand.find(".//DeliveryHour", namespaces).text
                    energy_mw = demand.find(".//EnergyMW", namespaces).text
                    # Append the extracted data to the list
                    data.append(
                        {
                            "DeliveryDate": delivery_date,
                            "Zone": zone_name,
                            "DeliveryHour": hour,
                            "EnergyMW": energy_mw,
                        },
                    )

        # Create a DataFrame from the list of data
        df = pd.DataFrame(data)

        # Convert columns to appropriate data types
        df["DeliveryHour"] = df["DeliveryHour"].astype(int)
        df["EnergyMW"] = df["EnergyMW"].astype(float)
        df["DeliveryDate"] = pd.to_datetime(df["DeliveryDate"])

        df["Interval Start"] = (
            df["DeliveryDate"] + pd.to_timedelta(df["DeliveryHour"], unit="h")
        ).dt.tz_localize(self.default_timezone)

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)

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

    @support_date_range(frequency="YEAR_START")
    def get_hourly_zonal_demand(self, date, end=None, verbose=False):
        """
        Get hourly zonal demand for a given date.

        Args:
            date (datetime.date): date to get load for
            end (datetime.date, optional): end date. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        """
        today = pd.Timestamp.now(tz=self.default_timezone).date()

        if date in ["latest", "today"]:
            date = pd.to_datetime(today)
        else:
            date = pd.to_datetime(date)

        date_only = date.date()

        if date_only < pd.to_datetime(EARLIEST_ACTUAL_DEMAND).date():
            raise NotSupported(
                f"Actual demand data is not available before {EARLIEST_ACTUAL_DEMAND}.",
            )

        if date_only > today:
            raise NotSupported("Actual demand is not available for future dates.")

        year = date.year
        url = ACTUAL_DEMAND_TEMPLATE_URL.replace("YYYY", str(year))

        msg = f"Fetching URL: {url}"
        log(msg, verbose)

        retry_num = 0
        sleep = 5

        while retry_num < 3:
            r = requests.get(url)

            if r.ok:
                break

            retry_num += 1
            print(
                f"Failed to get data from IESO. Error: {r.reason}."
                f"Retrying {retry_num}...",
            )

            time.sleep(sleep)

            # Exponential backoff
            sleep *= 2

        if not r.ok:
            raise Exception(f"Failed to get data from IESO. Error: {r.reason}")

        # Create a dataframe from the CSV file, skipping lines starting with \\
        df = pd.read_csv(io.StringIO(r.text), comment="\\")

        df["Interval Start"] = (
            pd.to_datetime(df["Date"]) + pd.to_timedelta(df["Hour"], unit="h")
        ).dt.tz_localize(self.default_timezone)

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)

        df = utils.move_cols_to_front(df, ["Interval Start", "Interval End"])

        cols_to_drop = ["Date", "Hour", "Diff"]

        df = df.drop(cols_to_drop, axis=1)

        # If no end is provided, return data from single date
        if not end:
            return df[df["Interval Start"].dt.date == date_only]

        end_date_only = pd.to_datetime(end).date()

        return df[
            (df["Interval Start"].dt.date >= date_only)
            & (df["Interval End"].dt.date <= end_date_only)
        ]
