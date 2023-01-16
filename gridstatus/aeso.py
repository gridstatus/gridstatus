import re
import sys

import pandas as pd
import requests
from bs4 import BeautifulSoup

from gridstatus import utils
from gridstatus.base import FuelMix, ISOBase, NotSupported
from gridstatus.decorators import support_date_range

ACTUAL_FORECAST_URL = "http://ets.aeso.ca/ets_web/ip/Market/Reports/ActualForecastWMRQHReportServlet"  # noqa 501

CURRENT_SUPPLY_DEMAND_REPORT_URL = (
    "http://ets.aeso.ca/ets_web/ip/Market/Reports/CSDReportServlet"
)


class AESO(ISOBase):
    """Alberta Electric System Operator (AESO)"""

    name = "Alberta Electric System Operator"
    iso_id = "aeso"
    default_timezone = "US/Mountain"

    status_homepage = ""
    interconnection_homepage = ""

    markets = []

    trading_hub_locations = []

    def get_status(self, date="latest", verbose=False) -> str:
        raise NotSupported("Status not supported")

    def get_fuel_mix(self, date, start=None, end=None, verbose=False):
        if date != "latest":
            raise NotSupported("Only latest fuel mix data is supported")

        dfs = self._load_current_supply_demand_dfs()

        # transpose Generation table
        df = dfs["GENERATION"].T
        # apply first row as new column names
        df = df.rename(columns=df.iloc[0].to_dict())[1:]
        # rename columns
        df = df.rename(
            columns={
                "GAS": "Gas",
                "HYDRO": "Hydro",
                "ENERGY STORAGE": "Energy Storage",
                "SOLAR": "Solar",
                "WIND": "Wind",
                "OTHER": "Other",
                "DUAL FUEL": "Dual Fuel",
                "COAL": "Coal",
            },
        )
        # extract Total Net Generation (TNG)
        mix = {k: int(v["GENERATION TNG"]) for k, v in df.to_dict().items()}

        return FuelMix(iso=self.name, time=dfs["Time"].iloc[0, 0], mix=mix)

    def _load_current_supply_demand_dfs(self):
        html = requests.get(CURRENT_SUPPLY_DEMAND_REPORT_URL)
        leaf_tables = self._extract_leaf_tables(html)
        table_dfs = []
        for table in leaf_tables:
            table_dfs += pd.read_html(str(table))
        dfs = {}
        for df in table_dfs:
            rv = self._parse_df(df)
            dfs.update(rv)
        return dfs

    def _get_actual_forecast_df(self, date=None, end=None):
        all_dfs = []
        params = {}
        if date is not None:
            params["beginDate"] = date.strftime("%m%d%Y")
        if end is not None:
            params["endDate"] = end.strftime("%m%d%Y")
        html = requests.get(ACTUAL_FORECAST_URL, params=params)
        soup = BeautifulSoup(html.content, "html.parser")
        tables = soup.select("table")
        for table in tables:
            text = table.get_text().strip()
            if len(text) == 0:
                continue
            dfs = pd.read_html(str(table))
            for df in dfs:
                if df.empty:
                    continue
                all_dfs.append(df)
        df = all_dfs[0]
        return df

    def _extract_leaf_tables(self, html):
        leaf_tables = []
        soup = BeautifulSoup(html.text, "html.parser")
        tables = soup.select("table")
        parents = set()
        for table in tables:
            table_parent = table.find_parent("table")
            if table_parent:
                parents.add(table_parent)
        for table in tables:
            if table not in parents:
                leaf_tables.append(table)
        return leaf_tables

    def get_interconnection_queue(self):
        raise NotSupported("Interconnection queue not supported")

    def get_load(self, date, end=None, verbose=False):
        if date != "latest":
            raise NotSupported("Only latest load data is supported")

        dfs = self._load_current_supply_demand_dfs()
        time = dfs["Time"].iloc[0, 0]
        summary_df = dfs["SUMMARY"]
        load_val = summary_df.iloc[0]["Alberta Internal Load (AIL)"]
        return {"time": time, "load": load_val}

    @support_date_range(frequency="31D")
    def get_load_forecast(self, date, end=None, verbose=False):
        if utils.is_today(date, tz=self.default_timezone):
            date, end = None, None
        df = self._get_actual_forecast_df(date, end)
        df["Time"] = self._parse_date_hour_ending(df)

        # approximate last updated timestamp by first
        # value for Forecast Pool Price
        fpp = df[["Time", "Forecast Pool Price"]]
        fpp = fpp[fpp["Forecast Pool Price"] != "-"]
        forecast_time = fpp["Time"].max()

        df["Forecast Time"] = forecast_time

        df = df.rename(columns={"Forecast AIL": "Load Forecast"})
        df = df[["Forecast Time", "Time", "Load Forecast"]]

        return df

    def _parse_date_hour_ending(self, df):
        return pd.to_datetime(
            df["Date (HE)"].str.split(" ").str[0]
            + " "
            + (df["Date (HE)"].str.split(" ").str[1].astype(int) - 1)
            .astype(str)
            .str.zfill(2)
            + ":00",
        ).dt.tz_localize(self.default_timezone, ambiguous="infer")

    @support_date_range(frequency="1D")
    def get_storage(self, date, verbose=False):
        raise NotSupported("Storage not supported")

    @support_date_range(frequency="31D")
    def get_gas_prices(
        self,
        date,
        end=None,
        fuel_region_id="ALL",
        sleep=4,
        verbose=False,
    ):
        pass

    @support_date_range(frequency="31D")
    def get_ghg_allowance(
        self,
        date,
        end=None,
        sleep=4,
        verbose=False,
    ):
        pass

    @support_date_range(frequency="1D")
    def get_as_prices(self, date, end=None, sleep=4, verbose=False):
        pass

    @support_date_range(frequency="31D")
    def get_as_procurement(
        self,
        date,
        end=None,
        market="DAM",
        sleep=4,
        verbose=False,
    ):
        pass

    def _parse_df(self, df):
        rv = {}
        if df.empty or df.iloc[0, 0] == "Legend":
            return rv
        elif "Last Update" in df.iloc[0, 0]:
            text = re.sub(r".*Last Update : ", "", df.iloc[0, 0])
            timestamp = pd.Timestamp(text, tz=self.default_timezone)
            rv = {"Time": pd.DataFrame(data=[{"Time": timestamp}])}
        elif "SUMMARY" in df.columns:
            df = df.rename(columns={"SUMMARY": "FIELD", "SUMMARY.1": "VALUE"})
            df = df[["FIELD", "VALUE"]]
            df = df.set_index("FIELD").T
            rv = {"SUMMARY": df}
        elif (
            "BIOMASS AND OTHER" in df.columns
            or "COAL" in df.columns
            or "DUAL FUEL" in df.columns
            or "ENERGY STORAGE" in df.columns
            or "GAS" in df.columns
            or "GENERATION" in df.columns
            or "HYDRO" in df.columns
            or "INTERCHANGE" in df.columns
            or "SOLAR" in df.columns
            or "SUMMARY" in df.columns
            or "WIND" in df.columns
        ):
            rv = self._parse_nested_df(df)
        else:
            print(f"WARNING - unknown table format = {df.to_string()}", file=sys.stderr)
        return rv

    def _parse_nested_df(self, df):
        subheader_row_indices = []
        for i, row in df.iterrows():
            if all(col == row[0] for col in df.iloc[i, 1:]):
                subheader_row_indices.append(i)

        s_dfs = []
        for idx in range(len(subheader_row_indices)):
            subheader_row_index = subheader_row_indices[idx]
            if idx < len(subheader_row_indices) - 1:
                next_subheader = subheader_row_indices[idx + 1]
            else:
                next_subheader = len(df)
            s_df = pd.DataFrame(
                columns=df.columns,
                data=df[subheader_row_index:next_subheader],
            )
            s_df = s_df.reset_index(drop=True)
            s_dfs.append(s_df)

        if len(s_dfs) == 0:
            s_dfs = [df]

        rv = {}
        [rv.update(self._parse_table_df(s_df)) for s_df in s_dfs]

        return rv

    def _parse_table_df(self, df):
        rv = {}

        if df.empty:
            return rv

        df_name = df.columns.tolist()[0]

        if self._all_row_values_equal(df.iloc[0]):
            subheader = df.iloc[0][0]
            df_name = f"{df_name} {subheader}"
            df = self._distribute_first_row(df)

        if self._columns_values_same_prefix(df.columns):
            df = self._apply_header(df)

        if df.iloc[-1][0] == "TOTAL":
            df = df.drop(index=df.index[-1])

        rv = {df_name: df}
        return rv

    def _all_row_values_equal(self, row):
        return all(col == row[0] for col in row[1:])

    def _columns_values_same_prefix(self, cols):
        return all(col.startswith(cols[0]) for col in cols[1:])

    def _distribute_first_row(self, df):
        prefix = df.iloc[0, 0]
        for col in df.columns:
            df.iloc[1][col] = f"{prefix} {df.iloc[1][col]}"
        df = df[1:]
        df = df.reset_index(drop=True)
        return df

    def _apply_header(self, df):
        rename_columns = df.iloc[0].to_dict()
        prefix = df.columns.tolist()[0]
        rename_columns = {k: f"{prefix} {v}" for k, v in rename_columns.items()}
        df = df.rename(columns=rename_columns).drop(index=0).reset_index(drop=True)
        return df

if __name__ == "__main__":
    import gridstatus

    control = gridstatus.Ercot().get_fuel_mix("latest")
    print(f"control = {control}", file=sys.stderr)

    control = gridstatus.NYISO().get_fuel_mix("latest")
    print(f"control = {control}", file=sys.stderr)

    iso = gridstatus.AESO()
    fuel_mix = iso.get_fuel_mix("latest")
    print(f"fuel_mix = {fuel_mix}", file=sys.stderr)
