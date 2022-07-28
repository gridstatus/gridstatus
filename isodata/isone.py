import re
from urllib import request
from .base import ISOBase, FuelMix
import pandas as pd
import requests


class ISONE(ISOBase):

    def get_fuel_mix(self):
        r = requests.post("https://www.iso-ne.com/ws/wsclient",
                          data={"_nstmp_requestType": "url", "_nstmp_requestUrl": "/genfuelmix/current"}).json()

        mix_df = pd.DataFrame(r[0]['data']['GenFuelMixes']['GenFuelMix'])
        time = mix_df["BeginDate"].max()  # eastern time

        mix_dict = mix_df.set_index("FuelCategory")["GenMw"].to_dict()
        return FuelMix(time, mix_dict)
