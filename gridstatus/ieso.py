import io
import time
from dataclasses import dataclass
from enum import Enum
from zipfile import ZipFile
from pytz import AmbiguousTimeError

import pandas as pd
import requests
import tqdm

import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from pytz.exceptions import NonExistentTimeError

from gridstatus import utils
from gridstatus.base import (
    GridStatus,
    InterconnectionQueueStatus,
    ISOBase,
    Markets,
    NotSupported,
)
from gridstatus.decorators import support_date_range
from gridstatus.ercot_60d_utils import (
    process_dam_gen,
    process_dam_load,
    process_dam_load_as_offers,
    process_sced_gen,
    process_sced_load,
)
from gridstatus.gs_logging import log
from gridstatus.lmp_config import lmp_config

FORECAST_DEMAND_INDEX_URL = "http://reports.ieso.ca/public/OntarioZonalDemand/"

# The most recent file does not have a date in the filename.
FORECASTED_DEMAND_TEMPLATE_URL = (
    f"{FORECAST_DEMAND_INDEX_URL}/PUB_OntarioZonalDemand_YYYYMMDD.xml"
)

MAXIMUM_DAYS_IN_PAST_FOR_FORECAST = 90
MAXIMUM_DAYS_IN_FUTURE_FOR_FORECAST = 34


ACTUAL_DEMAND_INDEX_URL = "http://reports.ieso.ca/public/DemandZonal/"

# Specifying the year is enough because the yearly file is updated daily.
ACTUAL_DEMAND_TEMPLATE_URL = f"{ACTUAL_DEMAND_INDEX_URL}/PUB_DemandZonal_YYYY.csv"


class Ieso(ISOBase):
    """Independent Electricity System Operator (IESO)"""

    name = "Independent Electricity System Operator"
    iso_id = "ieso"

    # All data is provided in EST, and does not change with DST
    # See https://www.ieso.ca/-/media/Files/IESO/Document-Library/engage/ca/ca-Introduction-to-the-Capacity-Auction.ashx
    default_timezone = "America/EST"

    @support_date_range("DAY_START")
    def get_load(self, date, end=None, verbose=False):
        """Get load for a given date

        Args:
            date (datetime.date): date to get load for
            end (datetime.date, optional): end date for range. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: load for given date
        """
        if date == "latest":
            date = self.get_load("today", verbose=verbose)

        elif utils.is_today(date, tz=self.default_timezone):
            return self._get_latest_load(verbose=verbose)

    def get_hourly_forecasted_demand(self, date, end=None, verbose=False):
        """Get load by forecast zone for a given date. Supports data 90 days into the past
        and up to 34 days into the future.

        Args:
            date (datetime.date): date to get load for
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: load for given date
        """
        today = pd.Timestamp.now(
            tz=self.default_timezone,
        ).normalize()
        if date == "latest":
            date = self.get_load("today", verbose=verbose)

        elif utils.is_today(date, tz=self.default_timezone):
            return self._get_latest_load(verbose=verbose)

        else:
            raise NotSupported(
                "IESO does not provide load by forecast zone for historical dates"
            )

        url = FORECASTED_DEMAND_TEMPLATE_URL.replace(
            "YYYYMMDD", date.strftime("%Y%m%d")
        )

        msg = f"Fetching URL: {url}"
        log(msg, verbose)

        retry_num = 0
        sleep = 5

        while retry_num < 3:
            r = requests.get(url)

            if r.status_code == 200:
                break

            retry_num += 1
            print(f"Failed to get data from CAISO. Error: {r.status_code}")
            print(f"Retrying {retry_num}...")
            time.sleep(sleep)

        # Define the namespaces used in the XML document
        namespaces = {
            "": "http://www.ieso.ca/schema",  # Define the default namespace
        }

        # Initialize a list to store the parsed data
        data = []

        # Parse the XML file
        root = ET.fromstring(r.content)

        published_at = root.findall(".//CreatedAt", namespaces)[0].text

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
                        }
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

        pivot_df["Time"] = pivot_df["Interval Start"]
        pivot_df["Published At"] = pd.Timestamp(published_at, tz=self.default_timezone)

        pivot_df = utils.move_cols_to_front(
            pivot_df,
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Published At",
            ],
        )

        return pivot_df

    def get_hourly_zonal_demand(self, date):
        year = date.year

        url = ACTUAL_DEMAND_TEMPLATE_URL.replace("YYYY", str(year))

        msg = f"Fetching URL: {url}"
        log(msg)

        retry_num = 0
        sleep = 5

        while retry_num < 3:
            r = requests.get(url)

            if r.status_code == 200:
                break

            retry_num += 1
            print(f"Failed to get data from CAISO. Error: {r.status_code}")
            print(f"Retrying {retry_num}...")
            time.sleep(sleep)

        # Create a dataframe from the CSV file, skipping lines starting with \\
        df = pd.read_csv(io.StringIO(r.text), comment="\\")

        df["Interval Start"] = (
            pd.to_datetime(df["Date"]) + pd.to_timedelta(df["Hour"], unit="h")
        ).dt.tz_localize(self.default_timezone)

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)
        df["Time"] = df["Interval Start"]

        # move time columns front
        df = utils.move_cols_to_front(
            df,
            ["Time", "Interval Start", "Interval End"],
        )

        cols_to_drop = ["Date", "Hour", "Diff"]
        df = df.drop(cols_to_drop, axis=1)

        return df
