import concurrent.futures
import datetime
import json
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

import gridstatus
from gridstatus import utils
from gridstatus.gs_logging import log


class EIA:
    BASE_URL = "https://api.eia.gov/v2/"

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

    def list_routes(self, route="/"):
        """List all available routes"""
        url = f"{self.BASE_URL}{route}"
        params = {
            "api_key": self.api_key,
        }
        data = self.session.get(url, params=params)
        response = data.json()["response"]
        return response

    def _fetch_page(self, url, headers):
        data = self.session.get(url, headers=headers)
        response = data.json()["response"]
        df = pd.DataFrame(response["data"])
        return df, response["total"]

    def get_dataset(self, dataset, start, end, n_workers=1, verbose=False):
        """Get data from a dataset

        Only supports "electricity/rto/interchange-data" dataset for now.

        Args:
            dataset (str): Dataset path
            start (str or pd.Timestamp): Start date
            end (str or pd.Timestamp): End date
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

        params = {
            "start": start_str,
            "end": end_str,
            "frequency": "hourly",
            "data": [
                "value",
            ],
            "facets": {},
            "offset": 0,
            "length": 5000,
        }

        headers = {
            "X-Api-Key": self.api_key,
            "X-Params": json.dumps(params),
        }

        log(f"Fetching data from {url}", verbose=verbose)
        log(f"Params: {params}", verbose=verbose)
        log(
            f"Concurrent workers: {n_workers}",
            verbose=verbose,
        )

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

        if dataset in DATASET_HANDLERS:
            df = DATASET_HANDLERS[dataset](df)

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
            log(f"Fetching data from {url}", verbose=verbose)
            df = pd.read_excel(url, sheet_name="Published Hourly Data")

            rename = {
                "NG": "Net Generation",
                "D": "Demand",
                "TI": "Total Interchange",
                "DF": "Demand Forecast",
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

        log(f"Downloading {url}", verbose)
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
                                float(d1_elements[i].text)
                                if d1_elements[i].text != "NA"
                                else np.nan,
                                float(direction_elements[i].text)
                                if direction_elements[i].text != "NA"
                                else np.nan,
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
                    float(price_siblings[0].text)
                    if price_siblings[0].text != "NA"
                    else np.nan,
                    float(direction_siblings[0].text)
                    if direction_siblings[0].text != "NA"
                    else np.nan,
                    float(price_siblings[1].text)
                    if price_siblings[1].text != "NA"
                    else np.nan,
                    float(direction_siblings[1].text)
                    if direction_siblings[1].text != "NA"
                    else np.nan,
                    float(price_siblings[2].text)
                    if price_siblings[2].text != "NA"
                    else np.nan,
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

        log(f"Downloading {url}", verbose)
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

    df["MW"] = df["MW"].astype("Int64")

    # pivot on type
    df = df.pivot_table(
        index=["Interval Start", "Interval End", "Respondent", "Respondent Name"],
        columns="Type",
        values="MW",
    ).reset_index()

    df.columns.name = None

    # fix after pivot
    for col in ["Load", "Net Generation", "Load Forecast", "Total Interchange"]:
        df[col] = df[col].astype("Int64")

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

    df["MW"] = df["MW"].astype("Int64")

    # pivot on type
    df = df.pivot_table(
        index=["Interval Start", "Interval End", "Respondent", "Respondent Name"],
        columns="type-name",
        values="MW",
    ).reset_index()

    fuel_mix_cols = df.columns[4:]

    # nans after pivot because not
    # all respondents have all fuel types
    df[fuel_mix_cols] = df[fuel_mix_cols].astype("Int64").fillna(0)

    df.columns.name = None

    df = df.sort_values(["Interval Start", "Respondent"])

    return df


DATASET_HANDLERS = {
    "electricity/rto/interchange-data": _handle_rto_interchange,
    "electricity/rto/region-data": _handle_region_data,
    "electricity/rto/fuel-type-data": _handle_fuel_type_data,
}

# docs
# https://www.eia.gov/opendata/documentation.php # noqa
