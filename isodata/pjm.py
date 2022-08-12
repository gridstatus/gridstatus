import io

import pandas as pd
import requests

import isodata
from isodata.base import FuelMix, ISOBase


class PJM(ISOBase):
    name = "PJM"
    iso_id = "pjm"
    default_timezone = "US/Eastern"

    def get_latest_fuel_mix(self):
        mix = self.get_fuel_mix_today()
        latest = mix.iloc[-1]
        time = latest.pop("Time")
        mix_dict = latest.to_dict()
        return FuelMix(time=time, mix=mix_dict, iso=self.name)

    def get_fuel_mix_today(self):
        "Get fuel mix for today in hourly intervals"
        return self._today_from_historical(self.get_historical_fuel_mix)

    def get_fuel_mix_yesterday(self):
        "Get fuel mix for yesterdat in hourly intervals"
        return self._yesterday_from_historical(self.get_historical_fuel_mix)

    def get_historical_fuel_mix(self, date):
        date = date = isodata.utils._handle_date(date)
        tomorrow = date + pd.DateOffset(1)

        data = {
            "datetime_beginning_ept": date.strftime("%m/%d/%Y 00:00")
            + "to"
            + tomorrow.strftime("%m/%d/%Y 00:00"),
            "fields": "datetime_beginning_ept,fuel_type,is_renewable,mw",
            "rowCount": 1000,
            "startRow": 1,
        }

        # todo consider converting this to csv like demand
        key = self._get_key()
        r = self._get_json(
            "https://api.pjm.com/api/v1/gen_by_fuel",
            params=data,
            headers={"Ocp-Apim-Subscription-Key": key},
        )
        mix_df = pd.DataFrame(r["items"])

        mix_df = mix_df.pivot_table(
            index="datetime_beginning_ept",
            columns="fuel_type",
            values="mw",
            aggfunc="first",
        ).reset_index()

        mix_df["datetime_beginning_ept"] = pd.to_datetime(
            mix_df["datetime_beginning_ept"],
        ).dt.tz_localize(self.default_timezone)

        mix_df = mix_df.rename(columns={"datetime_beginning_ept": "Time"})

        return mix_df

    def get_latest_supply(self):
        return self._latest_supply_from_fuel_mix()

    def get_supply_today(self):
        "Get supply for today in hourly intervals"
        return self._today_from_historical(self.get_historical_supply)

    def get_supply_yesterday(self):
        "Get supply for yesterdat in hourly intervals"
        return self._yesterday_from_historical(self.get_historical_supply)

    def get_historical_supply(self, date):
        """Returns supply at a previous date at hourly intervals"""
        return self._supply_from_fuel_mix(date)

    def get_latest_demand(self):
        return self._latest_from_today(self.get_demand_today)

    def get_demand_today(self):
        "Get demand for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_demand)

    def get_demand_yesterday(self):
        "Get demand for yesterdat in 5 minute intervals"
        return self._yesterday_from_historical(self.get_historical_demand)

    def get_historical_demand(self, date):
        """Returns demand at a previous date at 5 minute intervals

        Args:
            date (str or datetime.date): date to get demand for. must be in last 30 days
        """
        # todo can support a load area
        date = date = isodata.utils._handle_date(date)
        tomorrow = date + pd.DateOffset(1)

        data = {
            "datetime_beginning_ept": date.strftime("%m/%d/%Y 00:00")
            + "to"
            + tomorrow.strftime("%m/%d/%Y 00:00"),
            "sort": "datetime_beginning_utc",
            "order": "Asc",
            "startRow": 1,
            "isActiveMetadata": "true",
            "fields": "area,datetime_beginning_ept,instantaneous_load",
            "format": "csv",
            "download": "true",
        }
        key = self._get_key()
        r = requests.get(
            "https://api.pjm.com/api/v1/inst_load",
            params=data,
            headers={"Ocp-Apim-Subscription-Key": key},
        )

        data = pd.read_csv(io.StringIO(r.content.decode("utf8")))

        demand = demand = data[data["area"] == "PJM RTO"].drop("area", axis=1)

        demand = demand.rename(
            columns={"datetime_beginning_ept": "Time", "instantaneous_load": "Demand"},
        )

        demand["Time"] = pd.to_datetime(demand["Time"]).dt.tz_localize(
            self.default_timezone,
        )

        demand = demand.sort_values("Time").reset_index(drop=True)
        return demand

    def _get_key(self):
        settings = self._get_json("https://dataminer2.pjm.com/config/settings.json")

        return settings["subscriptionKey"]


"""


PJM web scraping
from bs4 import BeautifulSoup
import re
# pjm_url = 'https://www.pjm.com/markets-and-operations.aspx'
# html_text = requests.get(pjm_url).text
# soup = BeautifulSoup(html_text, 'html.parser')
# text = soup.find(
#     id='rtschartallfuelspjmGenFuel_container').next_sibling.contents[0]

# m = re.search('data:\ \[(.+?)],\ name:', text)
# if m:
#     found = m.group(1)
# else:
#     raise Exception("Could not find fuel mix data")

# parsed = json5.loads("[" + found + "]")

# mix_dict = dict((x["name"], x["y"]) for x in parsed)
"""
