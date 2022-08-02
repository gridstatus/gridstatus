import imp
from .base import ISOBase, FuelMix
import pandas as pd


class PJM(ISOBase):
    name = "PJM"
    iso_id = "pjm"

    # can get historical data from this api
    def get_latest_fuel_mix(self):
        settings = self._get_json(
            "https://dataminer2.pjm.com/config/settings.json")
        r = self._get_json("https://api.pjm.com/api/v1/gen_by_fuel",
                           headers={"Ocp-Apim-Subscription-Key": settings["subscriptionKey"]})
        mix_df = pd.DataFrame(r["items"])

        time_str = mix_df["datetime_beginning_ept"].max()

        time = pd.Timestamp(time_str, tz='US/Eastern')

        mix_df = mix_df[mix_df["datetime_beginning_ept"]
                        == time_str].set_index("fuel_type")["mw"]

        mix_dict = mix_df.to_dict()

        return FuelMix(time=time, mix=mix_dict, iso=self.name)


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
