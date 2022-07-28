import json5
import pandas as pd
from tabulate import tabulate
import requests
# TODO: this is needed to make SPP request work. restrict only to SPP
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'


class ISOBase:

    def get_json(self, *args, **kwargs):
        r = requests.get(*args, **kwargs)
        r = r.json()
        return r

    def get_fuel_mix(self):
        raise NotImplementedError()


class FuelMix:
    def __init__(self, time, mix, unit="MW") -> None:
        self.time = time
        self.unit = unit

        mix_df = pd.Series(mix, name=self.unit).sort_values(
            ascending=False).to_frame()
        mix_df["Percent"] = mix_df[self.unit] / mix_df[self.unit].sum() * 100
        mix_df.index.name = "Fuel"
        self._mix_df = mix_df

    def __repr__(self) -> str:
        # TODO sort by magnitude
        s = "Total Production: %d %s \n" % (self.total_production, self.unit)
        s += "Time: %s \n" % self.time

        mix = self.mix
        mix["Percent"] = mix["Percent"].round(1)
        s += tabulate(mix, headers='keys', tablefmt='psql')

        return s

    @property
    def total_production(self):
        return self.mix[self.unit].sum()

    @property
    def mix(self):
        return self._mix_df.copy()


class MISO(ISOBase):
    BASE = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx"

    def __init__(self) -> None:
        super().__init__()

    def get_fuel_mix(self):
        url = self.BASE + "?messageType=getfuelmix&returnType=json"
        r = self.get_json(url)

        time = r["RefId"]  # todo parse time

        mix = {}
        for fuel in r["Fuel"]["Type"]:
            amount = int(fuel["ACT"])
            if amount == -1:
                amount = 0
            mix[fuel["CATEGORY"]] = amount

        print(r["TotalMW"])  # todo - this total does add up to each part
        fm = FuelMix(time=time, mix=mix)
        return fm


class CAISO(ISOBase):
    BASE = "https://www.caiso.com/outlook/SP"

    def get_current_status(self) -> str:
        """Get Current Status of the Grid

        Known possible values: Normal
        """
        stats_url = self.BASE + "/stats.txt"
        r = self.get_json(stats_url)
        # todo is it possible for this to return more than one element?
        return r["gridstatus"][0]

    def get_fuel_mix(self):
        url = self.BASE + "/fuelsource.csv"
        df = pd.read_csv(url)

        mix = df.iloc[-1].to_dict()
        time = mix.pop("Time")  # handle time and time zone

        return FuelMix(time=time, mix=mix)


class PJM(ISOBase):

    def get_fuel_mix(self):
        r = self.get_json("https://api.pjm.com/api/v1/gen_by_fuel",
                          headers={"Ocp-Apim-Subscription-Key": 'b2621f9a5e6f48fdb184983d55f239ba'})
        mix_df = pd.DataFrame(r["items"])

        time = mix_df["datetime_beginning_ept"].max()

        mix_df = mix_df[mix_df["datetime_beginning_ept"]
                        == time].set_index("fuel_type")["mw"]

        mix_dict = mix_df.to_dict()

        return FuelMix(time=time, mix=mix_dict)


class Ercot(ISOBase):

    def get_fuel_mix(self):
        url = "https://www.ercot.com/api/1/services/read/dashboards/combine-wind-solar.json"
        r = self.get_json(url)

        # rows with nulls are forecasts
        df = pd.DataFrame(r['currentDay']["data"])
        df = df.dropna(subset=["actualSolar"])

        day = r['currentDay']["date"]
        hour = df["hourEnding"].max()  # latest hour in dataset
        time = day + "%d:00:00" % (hour)

        currentHour = df.iloc[-1]

        mix_dict = {
            "wind": currentHour["actualWind"],
            "solar": currentHour["actualSolar"]
        }

        return FuelMix(time=time, mix=mix_dict)


class SPP(ISOBase):

    def get_fuel_mix(self):
        url = "https://marketplace.spp.org/chart-api/gen-mix/asChart"
        r = self.get_json(url)["response"]

        data = {
            "Timestamp":  r["labels"]
        }
        data.update((d["label"], d["data"]) for d in r["datasets"])

        historical_mix = pd.DataFrame(data)

        current_mix = historical_mix.iloc[0].to_dict()

        time = current_mix.pop("Timestamp")

        return FuelMix(time=time, mix=current_mix)


class NYISO(ISOBase):

    def get_fuel_mix(self):
        # note: this is simlar datastructure to pjm
        url = "https://www.nyiso.com/o/oasis-rest/oasis/currentfuel/line-current?1659038374105"
        data = self.get_json(url)
        mix_df = pd.DataFrame(data["data"])
        time = mix_df["timeStamp"].max()
        mix_df = mix_df[mix_df["timeStamp"]
                        == time].set_index("fuelCategory")["genMWh"]
        mix_dict = mix_df.to_dict()
        return FuelMix(time, mix_dict)


class ISONE(ISOBase):

    def get_fuel_mix(self):
        r = requests.post("https://www.iso-ne.com/ws/wsclient",
                          data={"_nstmp_requestType": "url", "_nstmp_requestUrl": "/genfuelmix/current"}).json()

        mix_df = pd.DataFrame(r[0]['data']['GenFuelMixes']['GenFuelMix'])
        time = mix_df["BeginDate"].max()  # eastern time

        mix_dict = mix_df.set_index("FuelCategory")["GenMw"].to_dict()
        return FuelMix(time, mix_dict)


if __name__ == "__main__":

    # isos = [MISO(), CAISO(), PJM(), Ercot(), SPP(), NYISO]
    # for iso in isos:
    #     mix = iso.get_fuel_mix()
    #     assert isinstance(mix, FuelMix)
    #     assert isinstance(mix.mix, pd.Series)
    #     print(mix)

    iso = ISONE()
    d = iso.get_fuel_mix()


"""
Todos

- fuel mix
    - how standardize should the mix be? 
    - mark renewables
    - historical data
    - is the unit mh or mhw?
- units for return values
- documentation
    - include where the data is from
    - api reference
"""


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
