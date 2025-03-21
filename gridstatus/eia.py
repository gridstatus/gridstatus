import concurrent.futures
import datetime
import json
import os
import re
import warnings
from pathlib import Path
from typing import Dict
from zipfile import BadZipFile

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

import gridstatus
from gridstatus import NoDataFoundException, utils
from gridstatus.eia_constants import (
    CANCELED_OR_POSTPONED_GENERATOR_COLUMNS,
    EIA_FUEL_MIX_COLUMNS,
    EIA_FUEL_TYPES,
    GENERATOR_FLOAT_COLUMNS,
    GENERATOR_INT_COLUMNS,
    OPERATING_GENERATOR_COLUMNS,
    PLANNED_GENERATOR_COLUMNS,
    RETIRED_GENERATOR_COLUMNS,
)
from gridstatus.gs_logging import setup_gs_logger

logger = setup_gs_logger()

HENRY_HUB_NATURAL_GAS_SPOT_PRICES_PATH = "natural-gas/pri/fut"
# Physical location of Henry Hub is Louisiana
HENRY_HUB_TIMEZONE = "US/Central"


class EIA:
    BASE_URL = "https://api.eia.gov/v2/"
    default_timezone = HENRY_HUB_TIMEZONE

    def __init__(self, api_key=None):
        """Initialize EIA API object

        Args:
            api_key (str, optional): EIA API key.
                If not provided, will look for EIA_API_KEY environment variable.

        """
        if api_key is None:
            api_key = os.environ.get("EIA_API_KEY")
        self.api_key = api_key

        if api_key is None:
            raise ValueError(
                "API key not provided and EIA_API_KEY \
                not found in environment variables.",
            )
        self.api_key = api_key
        self.session = requests.Session()

    def list_facets(self, route="/"):
        """List all available facets and facet options for a dataset."""
        response = self.list_routes(route=route)
        try:
            facet_list = response["facets"]
        except KeyError:
            msg = (
                f"'facets' not found in keys. \nData route: {route} is not an endpoint."
            )
            warnings.warn(msg, UserWarning)
            return
        facet_info = {}
        for facet in facet_list:
            id = facet["id"]
            facet_url = f"{route}/facet/{id}"
            facet_info[id] = self.list_routes(facet_url)
        return facet_info

    def list_routes(self, route="/"):
        """List all available routes"""
        url = f"{self.BASE_URL}{route}"
        params = {
            "api_key": self.api_key,
        }
        try:
            data = self.session.get(url, params=params)
            data.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise err
        response = data.json()["response"]
        return response

    def _fetch_page(self, url, headers):
        try:
            data = self.session.get(url, headers=headers)
            data.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise err
        response = data.json()["response"]
        df = pd.DataFrame(response["data"])
        return df, int(response["total"])

    def _facet_handler(self, facets):
        """Ensures facets are properly formatted."""

        for k, v in facets.items():
            if not isinstance(v, list):
                facets[k] = [v]

        return facets

    def get_dataset(
        self,
        dataset,
        start,
        end,
        frequency="hourly",
        facets=None,
        n_workers=1,
        verbose=False,
    ):
        """Get data from a dataset

        Currently supports the following datasets:

        - "electricity/rto/interchange-data"
        - "electricity/rto/region-data"
        - "electricity/rto/region-sub-ba-data"
        - "electricity/rto/fuel-type-data"

        Args:
            dataset (str): Dataset path
            start (str or pd.Timestamp): Start date
            end (str or pd.Timestamp): End date
            frequency (str): Specifies the data frequency.
                Accepts [`hourly`, `local-hourly`]. Where `hourly` is refers
                to the UTC time and local-hourly is the local time.
            Default is `hourly`.
            facets (dict, optional): Facets to
                add to the request header. Defaults to None.
            n_workers (int, optional): Number of
                workers to use for fetching data. Defaults to 1.
            verbose (bool, optional): Whether
                to print progress. Defaults to False.

        Returns:
            pd.DataFrame: Dataframe with data from the dataset

        """
        start = gridstatus.utils._handle_date(start, "UTC")
        start_str = start.strftime("%Y-%m-%dT%H")

        end_str = None
        if end:
            end = gridstatus.utils._handle_date(end, "UTC")
            end_str = end.strftime("%Y-%m-%dT%H")

        url = f"{self.BASE_URL}{dataset}/data/"

        if facets is None:
            facets = {}
        else:
            facets = self._facet_handler(facets)

        params = {
            "start": start_str,
            "end": end_str,
            "frequency": frequency,
            "data": [
                "value",
            ],
            "facets": facets,
            "offset": 0,
            "length": 5000,
            # pagination breaks if not sorted because
            # api doesn't return in stable order across requests
            "sort": [
                {"column": col, "direction": "asc"}
                for col in DATASET_CONFIG[dataset]["index"]
            ],
        }

        headers = {
            "X-Api-Key": self.api_key,
            "X-Params": json.dumps(params),
        }

        if verbose:
            logger.info(f"Fetching data from {url}")
            logger.info(f"Params: {params}")
            logger.info(f"Concurrent workers: {n_workers}")

        raw_df, total_records = self._fetch_page(url, headers)

        # Calculate the number of pages
        page_size = 5000
        total_pages = (total_records + page_size - 1) // page_size

        if verbose:
            print(f"Total records: {total_records}")
            print(f"Total pages: {total_pages}")
            print("Fetching data:")

        # Fetch the remaining pages if necessary
        def fetch_page_wrapper(url, headers, page, page_size):
            params = json.loads(headers["X-Params"])
            params["offset"] = page * page_size
            headers["X-Params"] = json.dumps(params)
            page_df, _ = self._fetch_page(url, headers)
            return page_df

        if total_pages > 1:
            pages = range(1, total_pages)
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=n_workers,
            ) as executor:  # noqa
                args = ((url, headers.copy(), page, page_size) for page in pages)
                futures = [executor.submit(fetch_page_wrapper, *arg) for arg in args]

                if verbose:
                    with tqdm(total=total_pages, ncols=80) as progress_bar:
                        # for first page done at beginning
                        progress_bar.update(1)
                        for future, page in zip(
                            concurrent.futures.as_completed(futures),
                            pages,
                        ):  # noqa
                            progress_bar.update(1)

                page_dfs = [future.result() for future in futures]

            raw_df = pd.concat([raw_df, *page_dfs], ignore_index=True)

        df = raw_df.copy()

        if dataset in DATASET_CONFIG:
            df = DATASET_CONFIG[dataset]["handler"](df)

        return df

    def get_grid_monitor(
        self,
        area_id=None,
        area_type=None,
        n_workers=4,
        verbose=False,
    ):
        """
        Retrieves grid monitor data including generation and emissions.

        This function cannot filter by time and fetches all available data. It may
        be slow if fetching data for all areas.

        Args:
            area_id (str, optional): ID of area to fetch data for. If provided,
                fetches data for this area only, ignoring area_type. If both are
                not provided, fetches data for all areas. Defaults to None.

            area_type (str, optional): Type of areas ('Region' or 'BA') to fetch
                data for. Used only if area_id is not provided. If provided,
                fetches data for all areas of given type. If both are not
                provided, fetches data for all areas. Defaults to None.

            n_workers (int, optional): Number of workers to use for fetching data. Only
                used if multiple areas are being fetched. Defaults to 4.

            verbose (bool, optional): If True, prints progress. Defaults to False.

        Returns:
            dict: Grid monitor data for specified area(s).
        """

        config_path = Path(__file__).parent / "eia_data" / "grid_monitor_files.json"
        with open(config_path, "r") as f:
            GRID_MONITOR_FILES = json.load(f)

        areas_to_fetch = GRID_MONITOR_FILES.keys()
        if area_id:
            areas_to_fetch = [area_id]
        elif area_type:
            areas_to_fetch = [
                area_id
                for area_id in areas_to_fetch
                if GRID_MONITOR_FILES[area_id]["Type"].lower() == area_type.lower()
            ]

        def fetch_grid_monitor(grid_monitor):
            url = grid_monitor["URL"]
            if verbose:
                logger.info(f"Fetching data from {url}")
            df = pd.read_excel(url, sheet_name="Published Hourly Data")

            rename = {
                "Demand forecast": "Demand Forecast",
                "Net generation": "Net Generation",
                "Total interchange": "Total Interchange",
            }

            df = df.rename(columns=rename)

            df["Area Id"] = grid_monitor["ID"]
            df["Area Type"] = grid_monitor["Type"]
            df["Area Name"] = grid_monitor["Name"]

            df.insert(0, "Interval End", pd.to_datetime(df["UTC time"], utc=True))
            df.insert(0, "Interval Start", df["Interval End"] - pd.Timedelta("1h"))

            cols = [
                "Interval Start",
                "Interval End",
                "Area Id",
                "Area Name",
                "Area Type",
                "Demand",
                "Demand Forecast",
                "Net Generation",
                "Total Interchange",
                "NG: COL",
                "NG: NG",
                "NG: NUC",
                "NG: OIL",
                "NG: WAT",
                "NG: SUN",
                "NG: WND",
                "NG: UNK",
                "NG: OTH",
                "Positive Generation",
                "Consumed Electricity",
                "CO2 Factor: COL",
                "CO2 Factor: NG",
                "CO2 Factor: OIL",
                "CO2 Emissions: COL",
                "CO2 Emissions: NG",
                "CO2 Emissions: OIL",
                "CO2 Emissions: Other",
                "CO2 Emissions Generated",
                "CO2 Emissions Imported",
                "CO2 Emissions Exported",
                "CO2 Emissions Consumed",
                "CO2 Emissions Intensity for Generated Electricity",
                "CO2 Emissions Intensity for Consumed Electricity",
            ]

            df = df[cols]

            return df

        # Set the number of workers you want
        futures = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
            for area in areas_to_fetch:
                future = executor.submit(fetch_grid_monitor, GRID_MONITOR_FILES[area])
                futures.append(future)

            if verbose:
                with tqdm(total=len(areas_to_fetch), ncols=80) as progress_bar:
                    for future in futures:
                        future.result()  # Wait for each future to complete
                        progress_bar.update(1)

        # Combine all the dataframes (assuming you want to do this)
        all_dfs = [future.result() for future in futures]
        df = pd.concat(all_dfs, ignore_index=True)

        return df

    def get_daily_spots_and_futures(self, verbose=False):
        """
        Retrieves daily spots and futures for select energy products.

        Includes Wholesale Spot and Retail Petroleum, Natural Gas.
        Prompt-Month Futures, broken on EIA side,
        for Crude, Gasoline, Heating Oil, Natural Gas, Coal, Ethanol.

        They are published daily and not persisted, so this should be run once daily.

        Returns:
            d: dictionary of DataFrames for each table of values."""

        url = "https://www.eia.gov/todayinenergy/prices.php"

        df_petrol = pd.DataFrame(columns=["product", "area", "price", "percent_change"])
        df_ng = pd.DataFrame(
            columns=[
                "region",
                "natural_gas_price",
                "natural_gas_percent_change",
                "electricity_price",
                "electricity_percent_change",
                "spark_spread",
            ],
        )

        def contains_wholesale_petroleum(text):
            return text and "Wholesale Spot Petroleum Prices" in text

        if verbose:
            logger.info(f"Downloading {url}", verbose)

        with requests.get(url) as response:
            content = response.content
            soup = BeautifulSoup(content, "html.parser")

            close_date = soup.find("b", string=contains_wholesale_petroleum).text

            pattern = r"\b\d{1,2}/\d{1,2}/\d{2}\b"
            close_date = re.findall(pattern=pattern, string=close_date)[0]

            wholesale_petroleum = soup.select_one(
                "table[summary='Spot Petroleum Prices']",
            )

            rowspan_sum = 0
            directions = ["up", "dn", "nc"]
            for s1 in wholesale_petroleum.select("td.s1"):
                text = s1.text
                parent = s1.find_parent("tr").find_parent("table")

                if text == "Commodity Price Index":
                    break
                try:
                    rowspan = int(s1.get("rowspan"))
                    if s1.select("a", class_="lbox"):
                        rowspan -= 1  # down index by one (crack spread)
                        s2 = s1.find_next_sibling("td", class_="s2").text
                        d1 = s1.find_next_sibling("td", class_="d1").text
                        direction = float(
                            s1.find_next_sibling("td", class_=directions).text,
                        )
                        df_petrol.loc[len(df_petrol)] = (
                            text,
                            s2,
                            float(d1) if d1 != "NA" else np.nan,
                            float(direction) if direction != "NA" else np.nan,
                        )
                    else:
                        for i in range(rowspan_sum, rowspan + rowspan_sum):
                            s2_elements = parent.select("td.s2")
                            d1_elements = parent.select("td.d1")
                            direction_elements = parent.find_all(class_=directions)
                            df_petrol.loc[len(df_petrol)] = (
                                text,
                                s2_elements[i].text,
                                (
                                    float(d1_elements[i].text)
                                    if d1_elements[i].text != "NA"
                                    else np.nan
                                ),
                                (
                                    float(direction_elements[i].text)
                                    if direction_elements[i].text != "NA"
                                    else np.nan
                                ),
                            )

                    rowspan_sum += rowspan
                except TypeError:
                    s2 = s1.find_next_sibling("td", class_="s2").text
                    d1 = s1.find_next_sibling("td", class_="d1").text
                    direction = float(
                        s1.find_next_sibling("td", class_=directions).text,
                    )
                    df_petrol.loc[len(df_petrol)] = (
                        text,
                        s2,
                        float(d1) if d1 != "NA" else np.nan,
                        float(direction) if direction != "NA" else np.nan,
                    )

            natural_gas_spots = soup.select_one(
                "table[summary='Spot Natural Gas and Electric Power Prices']",
            )

            for s1 in natural_gas_spots.select("td.s1"):
                price_siblings = s1.find_next_siblings("td", class_="d1")
                direction_siblings = s1.find_next_siblings("td", class_=directions)
                df_ng.loc[len(df_ng)] = (
                    s1.text,
                    (
                        float(price_siblings[0].text)
                        if price_siblings[0].text != "NA"
                        else np.nan
                    ),
                    (
                        float(direction_siblings[0].text)
                        if direction_siblings[0].text != "NA"
                        else np.nan
                    ),
                    (
                        float(price_siblings[1].text)
                        if price_siblings[1].text != "NA"
                        else np.nan
                    ),
                    (
                        float(direction_siblings[1].text)
                        if direction_siblings[1].text != "NA"
                        else np.nan
                    ),
                    (
                        float(price_siblings[2].text)
                        if price_siblings[2].text != "NA"
                        else np.nan
                    ),
                )

        df_ng["date"] = pd.to_datetime(close_date)
        df_petrol["date"] = pd.to_datetime(close_date)

        df_ng = utils.move_cols_to_front(df_ng, cols_to_move=["date"])
        df_petrol = utils.move_cols_to_front(df_petrol, cols_to_move=["date"])

        d = {
            "petroleum": df_petrol,
            "natural_gas": df_ng,
        }

        return d

    def get_coal_spots(self, verbose=False):
        """
        Retrieve weekly coal commodity spot prices.
        TODO: add functionality to grab historicals from
        https://www.eia.gov/coal/markets/coal_markets_archive_json.php
        """

        url = "https://www.eia.gov/coal/markets/coal_markets_json.php"

        spot_price_keys = [
            "week_ending_date",
            "central_appalachia_price",
            "northern_appalachia_price",
            "illinois_basin_price",
            "powder_river_basin_price",
            "uinta_basin_price",
        ]
        coal_export_keys = [
            "delivery_month",
            "coal_min",
            "coal_max",
            "coal_exports",
        ]
        coke_export_keys = [
            "delivery_month",
            "coke_min",
            "coke_max",
            "coke_exports",
        ]

        spot_prices = {key: [] for key in spot_price_keys}
        coal_exports = {key: [] for key in coal_export_keys}
        coke_exports = {key: [] for key in coke_export_keys}

        if verbose:
            logger.info(f"Downloading {url}")

        with requests.get(url) as r:
            json = r.json()

        for key, value in json["data"][0].items():
            if key in ["snl_dpst", "snl_mmbtu"]:
                for item in value:
                    spot_prices["week_ending_date"].append(item["WEEK_ENDING_DATE"])
                    spot_prices["central_appalachia_price"].append(item["CENTRAL_APP"])
                    spot_prices["northern_appalachia_price"].append(
                        item["NORTHERN_APP"],
                    )
                    spot_prices["illinois_basin_price"].append(item["ILLIOIS_BASIN"])
                    spot_prices["powder_river_basin_price"].append(
                        item["POWDER_RIVER_BASIN"],
                    )
                    spot_prices["uinta_basin_price"].append(item["UINTA_BASIN"])
            elif key == "coal_exports":
                for item in value:
                    coal_exports["delivery_month"].append(item["ID"])
                    coal_exports["coal_min"].append(item["COAL_MIN"])
                    coal_exports["coal_max"].append(item["COAL_MAX"])
                    coal_exports["coal_exports"].append(item["COAL_EXPORTS"])
            elif key == "coke_exports":
                for item in value:
                    coke_exports["delivery_month"].append(item["ID"])
                    coke_exports["coke_min"].append(item["COKE_MIN"])
                    coke_exports["coke_max"].append(item["COKE_MAX"])
                    coke_exports["coke_exports"].append(item["COAL_COKE_EXPORTS"])
            else:
                pass

        weekly_spots = pd.DataFrame(spot_prices)
        weekly_spots = weekly_spots.loc[weekly_spots["week_ending_date"] != "change"]
        weekly_spots["week_ending_date"] = weekly_spots["week_ending_date"].map(
            pd.to_datetime,
        )
        weekly_spots = pd.merge(
            weekly_spots.drop_duplicates("week_ending_date", keep="first"),
            weekly_spots.drop_duplicates("week_ending_date", keep="last"),
            on="week_ending_date",
            suffixes=("_short_ton", "_mmbtu"),
        )

        coal_exports = pd.DataFrame(coal_exports)
        coal_exports["delivery_month"] = coal_exports["delivery_month"].map(
            lambda x: datetime.datetime.strptime(str(x), "%Y%m"),
        )
        coke_exports = pd.DataFrame(coke_exports)
        coke_exports["delivery_month"] = coke_exports["delivery_month"].map(
            lambda x: datetime.datetime.strptime(str(x), "%Y%m"),
        )

        return {
            "weekly_spots": weekly_spots,
            "coal_exports": coal_exports,
            "coke_exports": coke_exports,
        }

    def get_henry_hub_natural_gas_spot_prices(self, date, end=None, verbose=False):
        """
        Retrieve Henry Hub natural gas spot prices.

        https://www.eia.gov/dnav/ng/hist/rngwhhdD.htm

        Args:
            date (str or pd.Timestamp): Date to fetch data for.
            end (str or pd.Timestamp): End date to fetch data for.

        Returns:
            pd.DataFrame: DataFrame with Henry Hub natural gas spot prices.
        """

        data = self.get_dataset(
            HENRY_HUB_NATURAL_GAS_SPOT_PRICES_PATH,
            start=date,
            end=end,
            frequency="daily",
            verbose=verbose,
        )

        return data

    def get_generators(
        self,
        date: str | datetime.datetime,
        end: str | datetime.datetime = None,
        verbose: bool = False,
    ) -> Dict[str, pd.DataFrame]:
        date = utils._handle_date(date, "UTC")
        month_name = date.strftime("%B").lower()
        year = date.year
        # The most recent file doesn't have "archive" in the path. Since we don't know
        # exactly when a file comes out, use a try and except approach to the URL.
        url = f"https://www.eia.gov/electricity/data/eia860m/archive/xls/{month_name}_generator{year}.xlsx"

        if verbose:
            logger.info(f"Downloading EIA generator data from {url}")

        # Test if the file exists
        try:
            file = pd.ExcelFile(url, engine="openpyxl")
        except BadZipFile:
            url = url.replace("archive/", "")
            try:
                if verbose:
                    logger.info(f"Downloading EIA generator data from {url}")
                file = pd.ExcelFile(url, engine="openpyxl")
            except BadZipFile:
                raise NoDataFoundException(
                    f"EIA generator data not found for {date}",
                ) from None

        updated_at = pd.to_datetime(file.book.properties.modified, utc=True)

        # Beginning of the month as a date
        period = date.replace(day=1).date()

        # Some files we have to skip 2 rows, others only 1. We test for the first
        # column to determine the rows to skip. For the footer, we drop NAs while
        # processing the data
        skiprows = 1
        shared_args = {"skiprows": skiprows, "skipfooter": 1}
        operating_data = file.parse("Operating", **shared_args)

        if operating_data.columns[0] == "Unnamed: 0":
            operating_data.columns = operating_data.iloc[0].values
            operating_data = operating_data.iloc[1:]
            skiprows = 2
            shared_args.update({"skiprows": skiprows})

        planned_data = file.parse("Planned", **shared_args)
        retired_data = file.parse("Retired", **shared_args)
        canceled_or_postponed_data = file.parse("Canceled or Postponed", **shared_args)

        return {
            key: self._handle_generator_data(
                df=dataset,
                period=period,
                updated_at=updated_at,
                columns=columns,
                generator_status=key,
                verbose=verbose,
            )
            for key, dataset, columns in zip(
                ["operating", "planned", "retired", "canceled_or_postponed"],
                [
                    operating_data,
                    planned_data,
                    retired_data,
                    canceled_or_postponed_data,
                ],
                [
                    OPERATING_GENERATOR_COLUMNS,
                    PLANNED_GENERATOR_COLUMNS,
                    RETIRED_GENERATOR_COLUMNS,
                    CANCELED_OR_POSTPONED_GENERATOR_COLUMNS,
                ],
            )
        }

    def _handle_generator_data(
        self,
        df: pd.DataFrame,
        period: datetime.date,
        updated_at: datetime.datetime,
        columns: list[str],
        generator_status: str,
        verbose: bool = False,
    ) -> pd.DataFrame:
        df.insert(0, "Period", period)
        df.insert(1, "Updated At", updated_at)

        df.columns = df.columns.str.strip()

        cols_to_drop = [col for col in ["Google Map", "Bing Map"] if col in df.columns]

        df = (
            df.rename(
                columns={
                    "Nameplate Capacity (MW)": "Nameplate Capacity",
                    "Net Summer Capacity (MW)": "Net Summer Capacity",
                    "Net Winter Capacity (MW)": "Net Winter Capacity",
                    "Nameplate Energy Capacity (MWh)": "Nameplate Energy Capacity",
                    "DC Net Capacity (MW)": "DC Net Capacity",
                    "Planned Derate of Summer Capacity (MW)": "Planned Derate of Summer Capacity",
                    "Planned Uprate of Summer Capacity (MW)": "Planned Uprate of Summer Capacity",
                },
            )
            .drop(
                columns=cols_to_drop,
            )
            # Dropna values to remove extra footer rows
            .dropna(subset=["Plant ID"])
        )

        # Older files may not have all the columns. These are the columns we want to
        # fill with np.nan if they don't exist.
        columns_to_fill = [
            "Balancing Authority Code",
            "DC Net Capacity",
            "Nameplate Capacity",
            "Nameplate Energy Capacity",
            "Net Winter Capacity",
            "Unit Code",
        ]

        for col in columns_to_fill:
            # If this column is not in the dataframe but should be, add it with np.nan
            if col not in df.columns and col in columns:
                if verbose:
                    logger.warning(
                        f"Column {col} not found in data for {generator_status} generators. Adding and filling with np.nan values.",  # noqa
                    )
                df[col] = np.nan

        for col in GENERATOR_FLOAT_COLUMNS:
            if col in df.columns and df[col].dtype == "object":
                df[col] = (
                    df[col].astype(str).str.strip().replace({"": np.nan}).astype(float)
                )

        for col in GENERATOR_INT_COLUMNS:
            if col in df.columns and (
                df[col].dtype == "object" or df[col].dtype == "float"
            ):
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .str.replace(".0", "")
                    .replace({"": None, "nan": None})
                    .astype("Int64")
                )

        null_generator_id_rows = df.loc[df["Generator ID"].isnull()]

        # There are some rows with null Generator IDs. We drop these rows
        if not null_generator_id_rows.empty:
            logger.warning(
                f"Found rows with null Generator Ids for {generator_status} "
                + f"power plants. {null_generator_id_rows}",
            )
            df = df.dropna(subset=["Generator ID"])

        return df[columns]


def _handle_time(df, frequency="1h"):
    df.insert(0, "Interval End", pd.to_datetime(df["period"], utc=True))
    df.insert(0, "Interval Start", df["Interval End"] - pd.Timedelta(frequency))
    df = df.drop("period", axis=1)
    return df


def _handle_region_data(df):
    df = _handle_time(df, frequency="1h")

    df = df.rename(
        {
            "value": "MW",
            "respondent": "Respondent",
            "respondent-name": "Respondent Name",
            "type": "Type",
        },
        axis=1,
    )

    # ['TI', 'NG', 'DF', 'D']
    df["Type"] = df["Type"].map(
        {
            "D": "Load",
            "TI": "Total Interchange",
            "NG": "Net Generation",
            "DF": "Load Forecast",
        },
    )

    df["MW"] = df["MW"].astype(float)

    df = df[df["Type"].notna()]

    # pivot on Type
    df = df.pivot(
        index=["Interval Start", "Interval End", "Respondent", "Respondent Name"],
        columns="Type",
        values="MW",
    ).reset_index()

    df.columns.name = None

    # fix after pivot
    for col in ["Load", "Net Generation", "Load Forecast", "Total Interchange"]:
        if col in df.columns:
            df[col] = df[col].astype(float)

    return df


def _handle_region_sub_ba_data(df):
    """electricity/rto/region-sub-ba-data"""
    df = _handle_time(df, frequency="1h")

    df = df.rename(
        {
            "value": "MW",
            "subba-name": "Subregion Name",
            "subba": "Subregion",
            "parent": "BA",
            "parent-name": "BA Name",
        },
        axis=1,
    )

    df = df[
        [
            "Interval Start",
            "Interval End",
            "BA",
            "BA Name",
            "Subregion",
            "Subregion Name",
            "MW",
        ]
    ]

    df = df.sort_values(["Interval Start", "Subregion"])

    return df


def _handle_rto_interchange(df):
    """electricity/rto/interchange-data"""
    df = _handle_time(df, frequency="1h")
    df = df.rename(
        {
            "value": "MW",
            "fromba": "From BA",
            "toba": "To BA",
            "fromba-name": "From BA Name",
            "toba-name": "To BA Name",
        },
        axis=1,
    )
    df = df[
        [
            "Interval Start",
            "Interval End",
            "From BA",
            "From BA Name",
            "To BA",
            "To BA Name",
            "MW",
        ]
    ]

    df = df.sort_values(["Interval Start", "From BA"])

    return df


def _handle_fuel_type_data(df):
    """electricity/rto/fuel-type-data"""
    df = _handle_time(df, frequency="1h")

    df = df.rename(
        {
            "value": "MW",
            "respondent": "Respondent",
            "respondent-name": "Respondent Name",
        },
        axis=1,
    )

    df["MW"] = df["MW"].astype(float)

    # The raw data will sometimes have case-sensitive duplicates
    # (e.g. "Pumped Storage","Pumped storage"). We can handle that through the pivot
    # table by summing the duplicates across case-insensitive names.
    df["type-name"] = df["type-name"].str.lower()

    # These columns are grouped together by EIA as confirmed by inspection of the EIA
    # fuel mix graphs. https://www.eia.gov/electricity/gridmonitor/expanded-view/electric_overview/US48/US48/GenerationByEnergySource-4/edit # noqa
    df["type-name"] = df["type-name"].replace(
        {
            "battery": "battery storage",
            "solar battery": "solar with integrated battery storage",
            "unknown energy": "unknown energy storage",
            "unknown": "other",
        },
    )

    # Pivot on fuel type
    df = df.pivot_table(
        index=["Interval Start", "Interval End", "Respondent", "Respondent Name"],
        columns="type-name",
        values="MW",
        aggfunc="sum",
    ).reset_index()

    df.columns.name = None

    df.columns = df.columns.str.title()

    for col in EIA_FUEL_TYPES:
        if col not in df.columns:
            # This has to be np.nan not pd.NA because we are converting to float
            df[col] = np.nan

    fixed_cols = ["Interval Start", "Interval End", "Respondent", "Respondent Name"]
    fuel_mix_cols = [col for col in df.columns if col not in fixed_cols]

    df[fuel_mix_cols] = df[fuel_mix_cols].astype(float)

    # Set final column order with title case
    df = df[fixed_cols + sorted(fuel_mix_cols)]

    # Find any unknown columns and log them
    unknown_columns = set(df.columns) - set(EIA_FUEL_MIX_COLUMNS)

    if unknown_columns:
        logger.warning(f"Unknown columns found in fuel type data: {unknown_columns}")

    df = df.sort_values(["Interval Start", "Respondent"])

    return df


def _handle_henry_hub_natural_gas_spot_prices(df):
    # The other EIA datasets use "period" as the "Interval End" but that is not correct
    # for this dataset because that would put the data one day ahead of how the EIA
    # shows it here: https://www.eia.gov/dnav/ng/hist/rngwhhdD.htm
    # We use the HENRY_HUB_TIMEZONE because the spot prices are based on delivery
    # at Henry Hub in Louisiana. However, this dataset also includes futures prices,
    # where are based on the NYMEX, so US/Central might not be correct for these prices.
    df["Interval Start"] = pd.to_datetime(df["period"]).dt.tz_localize(
        HENRY_HUB_TIMEZONE,
    )
    df["Interval End"] = df["Interval Start"] + pd.Timedelta("1d")

    df = df.rename(
        columns={
            "area-name": "area_name",
            "product-name": "fuel_type",
            "process-name": "price_type",
            "series-description": "series_description",
            "value": "price",
        },
    )

    df = df.replace({"NA": pd.NA})
    df["price"] = df["price"].astype(float)

    df = utils.move_cols_to_front(df, ["Interval Start", "Interval End"])

    return df.sort_values(["Interval Start", "area_name", "series"])


DATASET_CONFIG = {
    "electricity/rto/interchange-data": {
        "index": [
            "period",
            "fromba",
            "toba",
        ],
        "handler": _handle_rto_interchange,
    },
    "electricity/rto/region-sub-ba-data": {
        "index": ["period", "subba", "parent"],
        "handler": _handle_region_sub_ba_data,
    },
    "electricity/rto/region-data": {
        "index": ["period", "respondent", "type"],
        "handler": _handle_region_data,
    },
    "electricity/rto/fuel-type-data": {
        "index": ["period", "respondent", "fueltype"],
        "handler": _handle_fuel_type_data,
    },
    HENRY_HUB_NATURAL_GAS_SPOT_PRICES_PATH: {
        "index": ["period"],
        "handler": _handle_henry_hub_natural_gas_spot_prices,
    },
}
