import imp
from .base import ISOBase, FuelMix
import pandas as pd
import isodata


class PJM(ISOBase):
    name = "PJM"
    iso_id = "pjm"
    default_timezone = 'US/Eastern'

    # can get historical data from this api
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
            "datetime_beginning_ept": date.strftime('%m/%d/%Y 00:00') + "to" + tomorrow.strftime('%m/%d/%Y 00:00'),
            "fields": "datetime_beginning_ept,fuel_type,is_renewable,mw",
            "rowCount": 1000,
            "startRow": 1
        }

        settings = self._get_json(
            "https://dataminer2.pjm.com/config/settings.json")
        r = self._get_json("https://api.pjm.com/api/v1/gen_by_fuel", params=data,
                           headers={"Ocp-Apim-Subscription-Key": settings["subscriptionKey"]})
        mix_df = pd.DataFrame(r["items"])

        mix_df = mix_df.pivot_table(index="datetime_beginning_ept",
                                    columns="fuel_type", values="mw", aggfunc="first").reset_index()

        mix_df["datetime_beginning_ept"] = pd.to_datetime(
            mix_df["datetime_beginning_ept"]).dt.tz_localize(self.default_timezone)

        mix_df = mix_df.rename(columns={"datetime_beginning_ept": "Time"})

        return mix_df

    def get_latest_supply(self):
        mix = self.get_latest_fuel_mix()
        return {
            "time": mix.time,
            "supply": mix.total_production
        }

    def get_supply_today(self):
        "Get supply for today in hourly intervals"
        return self._today_from_historical(self.get_historical_supply)

    def get_supply_yesterday(self):
        "Get supply for yesterdat in hourly intervals"
        return self._yesterday_from_historical(self.get_historical_supply)

    def get_historical_supply(self, date):
        """Returns supply at a previous date at hourly intervals"""
        return self._supply_from_fuel_mix(date)


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
