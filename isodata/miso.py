from .base import ISOBase, FuelMix


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
