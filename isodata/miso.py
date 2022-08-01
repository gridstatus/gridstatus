from pandas import Timestamp
import pandas as pd
from .base import ISOBase, FuelMix


class MISO(ISOBase):
    BASE = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx"

    def __init__(self) -> None:
        super().__init__()

    def get_fuel_mix(self):
        url = self.BASE + "?messageType=getfuelmix&returnType=json"
        r = self.get_json(url)

        date, time_str, am_pm = r["Fuel"]["Type"][0]["INTERVALEST"].split(" ")
        year, month, day, = map(int, date.split("-"))
        hour, minute, second = map(int, time_str.split(":"))
        if am_pm == "PM":
            hour += 12

        time = pd.Timestamp(
            year=year, month=month, day=day, hour=hour, minute=minute,  tz="America/Chicago")

        mix = {}
        for fuel in r["Fuel"]["Type"]:
            amount = int(fuel["ACT"])
            if amount == -1:
                amount = 0
            mix[fuel["CATEGORY"]] = amount

        # print(r["TotalMW"])  # todo - this total does add up to each part

        fm = FuelMix(time=time, mix=mix)
        return fm
