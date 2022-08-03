import re
from urllib import request
from .base import ISOBase, FuelMix
import pandas as pd
import requests
import isodata


class ISONE(ISOBase):
    name = "ISO New England"
    iso_id = "isone"
    default_timezone = "US/Eastern"

    def get_latest_fuel_mix(self):
        r = requests.post("https://www.iso-ne.com/ws/wsclient",
                          data={"_nstmp_requestType": "url", "_nstmp_requestUrl": "/genfuelmix/current"}).json()
        mix_df = pd.DataFrame(r[0]['data']['GenFuelMixes']['GenFuelMix'])
        time = pd.Timestamp(mix_df["BeginDate"].max(),
                            tz=self.default_timezone)

        mix_dict = mix_df.set_index("FuelCategory")["GenMw"].to_dict()
        return FuelMix(time, mix_dict, self.name)

    def get_latest_demand(self):
        data = self.get_demand_today()
        latest = data.iloc[-1]
        return {
            "time": latest["Time"],
            "demand": latest["Demand"]
        }

    def get_demand_today(self):
        return self._today_from_historical(self.get_historical_demand)

    def get_demand_yesterday(self):
        return self._yesterday_from_historical(self.get_historical_demand)

    def get_historical_demand(self, date):
        """Return demand at a previous date in 5 minute intervals"""
        # todo document the earliest supported date
        # _nstmp_formDate: 1659489137907
        date = isodata.utils._handle_date(date)

        date_str = date.strftime('%m/%d/%Y')
        data = {'_nstmp_startDate': date_str,
                '_nstmp_endDate': date_str,
                '_nstmp_twodays': False,
                '_nstmp_twodaysCheckbox': False,
                '_nstmp_requestType': "systemload",
                '_nstmp_forecast': True,
                '_nstmp_actual': True,
                '_nstmp_cleared': True,
                '_nstmp_priorDay': True,
                '_nstmp_inclPumpLoad': True,
                '_nstmp_inclBtmPv': True}

        r = requests.post(
            "https://www.iso-ne.com/ws/wsclient", data=data).json()

        data = pd.DataFrame(r[0]["data"]["actual"])

        data["BeginDate"] = pd.to_datetime(
            data["BeginDate"]).dt.tz_convert(self.default_timezone)

        df = data[["BeginDate", "Mw"]].rename(
            columns={"BeginDate": "Time", "Mw": "Demand"})

        return df

# daily historical fuel mix
# https://www.iso-ne.com/static-assets/documents/2022/01/2022_daygenbyfuel.xlsx
# a bunch more here: https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/daily-gen-fuel-type
