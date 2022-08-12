import io
import re
from urllib import request

import pandas as pd
import requests

import isodata
from isodata.base import FuelMix, ISOBase


class ISONE(ISOBase):
    name = "ISO New England"
    iso_id = "isone"
    default_timezone = "US/Eastern"

    def get_latest_fuel_mix(self):
        r = requests.post(
            "https://www.iso-ne.com/ws/wsclient",
            data={"_nstmp_requestType": "fuelmix"},
        ).json()
        mix_df = pd.DataFrame(r[0]["data"]["GenFuelMixes"]["GenFuelMix"])
        time = pd.Timestamp(mix_df["BeginDate"].max(), tz=self.default_timezone)

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
                skiprows=[0, 1, 2, 3, 5],
                skipfooter=1,
                engine="python",
            )

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

        r = requests.post("https://www.iso-ne.com/ws/wsclient", data=data).json()

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


# daily historical fuel mix
# https://www.iso-ne.com/static-assets/documents/2022/01/2022_daygenbyfuel.xlsx
# a bunch more here: https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/daily-gen-fuel-type
