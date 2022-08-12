import io
import pdb
from zipfile import ZipFile

import pandas as pd
import requests

import isodata
from isodata import utils
from isodata.base import FuelMix, ISOBase, Markets


class NYISO(ISOBase):
    name = "New York ISO"
    iso_id = "nyiso"
    default_timezone = "US/Eastern"

    # Markets
    REAL_TIME_5_MIN = Markets.REAL_TIME_5_MIN
    DAY_AHEAD_5_MIN = Markets.DAY_AHEAD_5_MIN

    # def get_latest_status(self):
    #     # https://www.nyiso.com/en/system-conditions
    #      http://mis.nyiso.com/public/P-35list.htm
    #     pass

    def get_latest_fuel_mix(self):
        # note: this is simlar datastructure to pjm
        url = "https://www.nyiso.com/o/oasis-rest/oasis/currentfuel/line-current"
        data = self._get_json(url)
        mix_df = pd.DataFrame(data["data"])
        time_str = mix_df["timeStamp"].max()
        time = pd.Timestamp(time_str)
        mix_df = mix_df[mix_df["timeStamp"] == time_str].set_index("fuelCategory")[
            "genMWh"
        ]
        mix_dict = mix_df.to_dict()
        return FuelMix(time=time, mix=mix_dict, iso=self.name)

    def get_fuel_mix_today(self):
        "Get fuel mix for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_fuel_mix)

    def get_fuel_mix_yesterday(self):
        "Get fuel mix for yesterdat in 5 minute intervals"
        return self._yesterday_from_historical(self.get_historical_fuel_mix)

    def get_historical_fuel_mix(self, date):
        mix_df = _download_nyiso_archive(date, "rtfuelmix")
        mix_df = mix_df.pivot_table(
            index="Time Stamp",
            columns="Fuel Category",
            values="Gen MW",
            aggfunc="first",
        ).reset_index()

        mix_df["Time Stamp"] = pd.to_datetime(mix_df["Time Stamp"]).dt.tz_localize(
            self.default_timezone,
        )

        mix_df = mix_df.rename(columns={"Time Stamp": "Time"})

        return mix_df

    def get_latest_demand(self):
        return self._latest_from_today(self.get_demand_today)

    def get_demand_today(self):
        "Get demand for today in 5 minute intervals"
        d = self._today_from_historical(self.get_historical_demand)
        return d

    def get_demand_yesterday(self):
        "Get demand for yesterday in 5 minute intervals"
        return self._yesterday_from_historical(self.get_historical_demand)

    def get_historical_demand(self, date):
        """Returns demand at a previous date in 5 minute intervals"""
        data = _download_nyiso_archive(date, "pal")

        # drop NA loads
        data = data.dropna(subset=["Load"])

        # TODO demand by zone
        demand = data.groupby("Time Stamp")["Load"].sum().reset_index()

        demand = demand.rename(columns={"Time Stamp": "Time", "Load": "Demand"})

        demand["Time"] = pd.to_datetime(demand["Time"]).dt.tz_localize(
            self.default_timezone,
        )

        return demand

    def get_latest_supply(self):
        """Returns most recent data point for supply in MW

        Updates every 5 minutes
        """
        return self._latest_supply_from_fuel_mix()

    def get_supply_today(self):
        "Get supply for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_supply)

    def get_supply_yesterday(self):
        "Get supply for yesterday in 5 minute intervals"
        return self._yesterday_from_historical(self.get_historical_supply)

    def get_historical_supply(self, date):
        """Returns supply at a previous date in 5 minute intervals"""
        return self._supply_from_fuel_mix(date)

    def get_latest_lmp(self, market: str, nodes: list):
        return self._latest_lmp_from_today(market, nodes, node_column="Zone")

    def get_lmp_today(self, market: str, nodes: list):
        "Get lmp for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_lmp, market, nodes)

    def get_lmp_yesterday(self, market: str, nodes: list):
        "Get lmp for yesterday in 5 minute intervals"
        return self._yesterday_from_historical(self.get_historical_lmp, market, nodes)

    def get_historical_lmp(self, date, market: str, nodes: list):
        """
        Supported Markets: REAL_TIME_5_MIN, DAY_AHEAD_5_MIN
        """
        # todo support generator and zone
        if market == self.REAL_TIME_5_MIN:
            marketname = "realtime"
            filename = marketname + "_zone"
        if market == self.DAY_AHEAD_5_MIN:
            marketname = "damlbmp"
            filename = marketname + "_zone"

        df = _download_nyiso_archive(date, market_name=marketname, filename=filename)

        # todo handle node
        columns = {
            "Time Stamp": "Time",
            "Name": "Zone",
            "LBMP ($/MWHr)": "LMP",
            "Marginal Cost Losses ($/MWHr)": "Loss",
            "Marginal Cost Congestion ($/MWHr)": "Congestion",
        }

        df = df.rename(columns=columns)

        df["Energy"] = df["LMP"] - (df["Loss"] - df["Congestion"])
        df["Market"] = market

        df = df[["Time", "Market", "Zone", "LMP", "Energy", "Congestion", "Loss"]]

        df["Time"] = pd.to_datetime(df["Time"]).dt.tz_localize(self.default_timezone)

        data = utils.filter_lmp_nodes(df, nodes, node_column="Zone")

        return df


# def get_day_ahead_prices(self,)

# https://www.nyiso.com/energy-market-operational-data


def _download_nyiso_archive(date, market_name, filename=None):

    if filename is None:
        filename = market_name

    date = isodata.utils._handle_date(date)
    month = date.strftime("%Y%m01")
    day = date.strftime("%Y%m%d")

    csv_filename = f"{day}{filename}.csv"
    csv_url = f"http://mis.nyiso.com/public/csv/{market_name}/{csv_filename}"
    zip_url = f"http://mis.nyiso.com/public/csv/{market_name}/{month}{filename}_csv.zip"

    # the last 7 days of file are hosted directly as csv
    try:
        df = pd.read_csv(csv_url)
    except:
        r = requests.get(zip_url)
        z = ZipFile(io.BytesIO(r.content))
        df = pd.read_csv(z.open(csv_filename))

    return df


"""
pricing data

https://www.nyiso.com/en/energy-market-operational-data
"""
