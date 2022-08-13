import io
import math
import re
from tkinter import E
from urllib import request

import pandas as pd
import requests

import isodata
from isodata import utils
from isodata.base import FuelMix, ISOBase, Markets


class ISONE(ISOBase):
    name = "ISO New England"
    iso_id = "isone"
    default_timezone = "US/Eastern"

    REAL_TIME_5_MIN = Markets.REAL_TIME_5_MIN
    REAL_TIME_HOURLY = Markets.REAL_TIME_HOURLY
    DAY_AHEAD_HOURLY = Markets.DAY_AHEAD_HOURLY

    hubs = {"H.INTERNAL_HUB": 4000}
    zones = {
        ".Z.MAINE": 4001,
        ".Z.NEWHAMPSHIRE": 4002,
        ".Z.VERMONT": 4003,
        ".Z.CONNECTICUT": 4004,
        ".Z.RHODEISLAND": 4005,
        ".Z.SEMASS": 4006,
        ".Z.WCMASS": 4007,
        ".Z.NEMASSBOST": 4008,
    }
    interfaces = {
        ".I.SALBRYNB345": 4010,
        ".I.ROSETON 345": 4011,
        ".I.HQ_P1_P2345": 4012,
        ".I.HQHIGATE120": 4013,
        ".I.SHOREHAM138": 4014,
        ".I.NRTHPORT138": 4017,
    }

    def get_latest_fuel_mix(self):
        r = requests.post(
            "https://www.iso-ne.com/ws/wsclient",
            data={"_nstmp_requestType": "fuelmix"},
        ).json()
        mix_df = pd.DataFrame(r[0]["data"]["GenFuelMixes"]["GenFuelMix"])
        time = pd.Timestamp(
            mix_df["BeginDate"].max(),
            tz=self.default_timezone,
        )

        # todo has marginal flag
        mix_dict = mix_df.set_index("FuelCategory")["GenMw"].to_dict()

        return FuelMix(time, mix_dict, self.name)

    def get_fuel_mix_today(self):
        "Get fuel mix for today"
        # todo should this use the latest endpoint?
        return self._today_from_historical(self.get_historical_fuel_mix)

    def get_fuel_mix_yesterday(self):
        "Get fuel mix for yesterday"
        return self._yesterday_from_historical(self.get_historical_fuel_mix)

    def get_historical_fuel_mix(self, date):
        """Return fuel mix at a previous date

        Provided at frequent, but irregular intervals by ISONE
        """
        date = isodata.utils._handle_date(date)
        url = "https://www.iso-ne.com/transform/csv/genfuelmix?start=" + date.strftime(
            "%Y%m%d",
        )

        df = _make_request(url, skiprows=[0, 1, 2, 3, 5])

        df["Date"] = pd.to_datetime(df["Date"] + " " + df["Time"]).dt.tz_localize(
            self.default_timezone,
        )

        mix_df = df.pivot_table(
            index="Date",
            columns="Fuel Category",
            values="Gen Mw",
            aggfunc="first",
        ).reset_index()

        mix_df = mix_df.rename(columns={"Date": "Time"})

        return mix_df

    def get_latest_demand(self):
        return self._latest_from_today(self.get_demand_today)

    def get_demand_today(self):
        return self._today_from_historical(self.get_historical_demand)

    def get_demand_yesterday(self):
        return self._yesterday_from_historical(self.get_historical_demand)

    def get_historical_demand(self, date):
        """Return demand at a previous date in 5 minute intervals"""
        # todo document the earliest supported date
        # _nstmp_formDate: 1659489137907
        date = isodata.utils._handle_date(date)

        date_str = date.strftime("%m/%d/%Y")
        data = {
            "_nstmp_startDate": date_str,
            "_nstmp_endDate": date_str,
            "_nstmp_twodays": False,
            "_nstmp_twodaysCheckbox": False,
            "_nstmp_requestType": "systemload",
            "_nstmp_forecast": True,
            "_nstmp_actual": True,
            "_nstmp_cleared": True,
            "_nstmp_priorDay": True,
            "_nstmp_inclPumpLoad": True,
            "_nstmp_inclBtmPv": True,
        }

        r = requests.post(
            "https://www.iso-ne.com/ws/wsclient",
            data=data,
        ).json()

        data = pd.DataFrame(r[0]["data"]["actual"])

        data["BeginDate"] = pd.to_datetime(data["BeginDate"]).dt.tz_convert(
            self.default_timezone,
        )

        df = data[["BeginDate", "Mw"]].rename(
            columns={"BeginDate": "Time", "Mw": "Demand"},
        )

        return df

    def get_latest_supply(self):
        """Returns most recent data point for supply in MW"""
        return self._latest_supply_from_fuel_mix()

    def get_supply_today(self):
        "Get supply for today in MW"
        return self._today_from_historical(self.get_historical_supply)

    def get_supply_yesterday(self):
        "Get supply for yesterday in MW"
        return self._yesterday_from_historical(self.get_historical_supply)

    def get_historical_supply(self, date):
        """Returns supply at a previous date in MW"""
        return self._supply_from_fuel_mix(date)

    def get_latest_lmp(self, market: str, nodes: list):
        """
        Find Node ID mapping: https://www.iso-ne.com/markets-operations/settlements/pricing-node-tables/
        """
        # todo optimize to read latest csv
        if market == self.REAL_TIME_5_MIN:
            url = "https://www.iso-ne.com/transform/csv/fiveminlmp/current?type=prelim"
            data = _make_request(url, skiprows=[0, 1, 2, 4])
        elif market == self.REAL_TIME_HOURLY:
            url = "https://www.iso-ne.com/transform/csv/hourlylmp/current?type=prelim&market=rt"
            data = _make_request(url, skiprows=[0, 1, 2, 4])

            # todo does this handle single digital hours?
            data["Local Time"] = (
                data["Local Date"]
                + " "
                + data["Local Time"].astype(str).str.zfill(2)
                + ":00"
            )

        else:
            raise RuntimeError("LMP Market is not supported")

        data = _process_lmp(data, market, self.default_timezone, nodes)
        return data

    def get_lmp_today(self, market: str, nodes: list):
        "Get lmp for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_lmp, market, nodes)

    def get_lmp_yesterday(self, market: str, nodes: list):
        "Get lmp for yesterday in 5 minute intervals"
        return self._yesterday_from_historical(self.get_historical_lmp, market, nodes)

    def get_historical_lmp(self, date, market: str, nodes: list):
        """Find Node ID mapping: https://www.iso-ne.com/markets-operations/settlements/pricing-node-tables/"""
        date = isodata.utils._handle_date(date)
        date_str = date.strftime("%Y%m%d")

        now = pd.Timestamp.now(tz=self.default_timezone)

        if market == self.REAL_TIME_5_MIN:
            # todo handle intervals for current day
            intervals = ["00-04", "04-08", "08-12", "12-16", "16-20", "20-24"]

            # optimze for current day
            if now.date() == date.date():
                hour = now.hour
                # select completed 4 hour intervals based on current hour
                intervals = intervals[: math.ceil((hour + 1) / 4) - 1]

            dfs = []
            for interval in intervals:
                print("Loading interval {}".format(interval))
                u = f"https://www.iso-ne.com/static-transform/csv/histRpts/5min-rt-prelim/lmp_5min_{date_str}_{interval}.csv"
                dfs.append(
                    pd.read_csv(
                        u,
                        skiprows=[0, 1, 2, 3, 5],
                        skipfooter=1,
                        engine="python",
                    ),
                )

            data = pd.concat(dfs)

            data["Local Time"] = (
                date.strftime(
                    "%Y-%m-%d",
                )
                + " "
                + data["Local Time"]
            )

            # add current interval
            if now.date() == date.date():
                url = "https://www.iso-ne.com/transform/csv/fiveminlmp/currentrollinginterval"
                print("Loading current interval")
                data_current = _make_request(url, skiprows=[0, 1, 2, 4])

                data_current = data_current[
                    data_current["Local Time"] > data["Local Time"].max()
                ]

                data = pd.concat([data, data_current])

        elif market == self.REAL_TIME_HOURLY:
            if date.date() < now.date():
                url = f"https://www.iso-ne.com/static-transform/csv/histRpts/rt-lmp/lmp_rt_prelim_{date_str}.csv"
                data = _make_request(url, skiprows=[0, 1, 2, 3, 5])
                # todo document hour starting vs ending
                data["Local Time"] = (
                    data["Date"]
                    + " "
                    + (data["Hour Ending"] - 1).astype(str).str.zfill(2)
                    + ":00"
                )
            else:
                raise RuntimeError("Today not support for hourly lmp")

        elif market == self.DAY_AHEAD_HOURLY:
            url = f"https://www.iso-ne.com/static-transform/csv/histRpts/da-lmp/WW_DALMP_ISO_{date_str}.csv"
            data = _make_request(url, skiprows=[0, 1, 2, 3, 5])
            # todo document hour starting vs ending
            data["Local Time"] = (
                data["Date"]
                + " "
                + (data["Hour Ending"] - 1).astype(str).str.zfill(2)
                + ":00"
            )
        else:
            raise RuntimeError("LMP Market is not supported")

        data = _process_lmp(data, market, self.default_timezone, nodes)

        return data

        # daily historical fuel mix
        # https://www.iso-ne.com/static-assets/documents/2022/01/2022_daygenbyfuel.xlsx
        # a bunch more here: https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/daily-gen-fuel-type


def _make_request(url, skiprows):
    with requests.Session() as s:
        # in testing, never takes more than 2 attempts
        attempt = 0
        while attempt < 3:
            # make first get request to get cookies set
            r1 = s.get(
                "https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/gen-fuel-mix",
            )

            r2 = s.get(url)

            if r2.status_code == 200:
                break

            print("Attempt {} failed. Retrying...".format(attempt + 1))
            attempt += 1

        df = pd.read_csv(
            io.StringIO(r2.content.decode("utf8")),
            skiprows=skiprows,
            skipfooter=1,
            engine="python",
        )
        return df


def _process_lmp(data, market, timezone, nodes):
    # todo handle location types
    rename = {
        "Location ID": "Node",
        "Location": "Node",
        "Local Time": "Time",
        "Locational Marginal Price": "LMP",
        "LMP": "LMP",
        "Energy Component": "Energy",
        "Congestion Component": "Congestion",
        "Loss Component": "Loss",
        "Marginal Loss Component": "Loss",
    }

    data.rename(columns=rename, inplace=True)

    data["Market"] = market

    data["Time"] = pd.to_datetime(data["Time"]).dt.tz_localize(timezone)

    data = data[
        [
            "Time",
            "Market",
            "Node",
            "LMP",
            "Energy",
            "Congestion",
            "Loss",
        ]
    ]

    data = utils.filter_lmp_nodes(data, nodes)
    return data
