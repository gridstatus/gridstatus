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

        r = self._get_pjm_json("gen_by_fuel", params=data)

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

    def get_historical_supply(self, date):
        """Returns supply at a previous date at hourly intervals"""
        return self._supply_from_fuel_mix(date)

    def get_latest_demand(self):
        return self._latest_from_today(self.get_demand_today)

    def get_demand_today(self):
        "Get demand for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_demand)

    def get_historical_demand(self, date):
        """Returns demand at a previous date at 5 minute intervals

        Args:
            date (str or datetime.date): date to get demand for. must be in last 30 days
        """
        # todo can support a load area
        date = isodata.utils._handle_date(date)
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
            "area": "PJM RTO",
            "format": "json",
            "download": "true",
        }
        r = self._get_pjm_json("inst_load", params=data)

        data = pd.DataFrame(r)

        demand = demand = data.drop("area", axis=1)

        demand = demand.rename(
            columns={
                "datetime_beginning_ept": "Time",
                "instantaneous_load": "Demand",
            },
        )

        demand["Time"] = pd.to_datetime(demand["Time"]).dt.tz_localize(
            self.default_timezone,
        )

        demand = demand.sort_values("Time").reset_index(drop=True)
        return demand

    def get_forecast_today(self):
        """Get forecast for today in hourly intervals.

        Updates every Every half hour on the quarter E.g. 1:15 and 1:45

        """
        # todo: should we use the UTC field instead of EPT?
        data = {
            "startRow": 1,
            "rowCount": 1000,
            "fields": "evaluated_at_datetime_ept,forecast_area,forecast_datetime_beginning_ept,forecast_load_mw",
            "forecast_area": "RTO_COMBINED",
        }
        r = self._get_pjm_json("load_frcstd_7_day", params=data)
        data = pd.DataFrame(r["items"]).rename(
            columns={
                "evaluated_at_datetime_ept": "Forecast Time",
                "forecast_datetime_beginning_ept": "Time",
                "forecast_load_mw": "Load Forecast",
            },
        )

        data.drop("forecast_area", axis=1, inplace=True)

        data["Forecast Time"] = pd.to_datetime(data["Forecast Time"]).dt.tz_localize(
            self.default_timezone,
        )
        data["Time"] = pd.to_datetime(data["Time"]).dt.tz_localize(
            self.default_timezone,
        )

        return data

    # todo https://dataminer2.pjm.com/feed/load_frcstd_hist/definition
    # def get_historical_forecast(self, date):
    # pass

    def _get_pjm_json(self, endpoint, params):
        r = self._get_json(
            "https://api.pjm.com/api/v1/" + endpoint,
            params=params,
            headers={"Ocp-Apim-Subscription-Key": self._get_key()},
        )

        return r

    def _get_key(self):
        settings = self._get_json(
            "https://dataminer2.pjm.com/config/settings.json",
        )

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
