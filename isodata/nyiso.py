from .base import ISOBase, FuelMix
import pandas as pd


class NYISO(ISOBase):

    def get_current_status(self):
        # https://www.nyiso.com/en/system-conditions
        pass

    def get_fuel_mix(self):
        # note: this is simlar datastructure to pjm
        url = "https://www.nyiso.com/o/oasis-rest/oasis/currentfuel/line-current"
        data = self.get_json(url)
        mix_df = pd.DataFrame(data["data"])
        time = pd.Timestamp(mix_df["timeStamp"].max())
        mix_df = mix_df[mix_df["timeStamp"]
                        == time].set_index("fuelCategory")["genMWh"]
        mix_dict = mix_df.to_dict()
        return FuelMix(time, mix_dict)

    def get_historical_fuel_mix(self):
        # above url gives daily fuel mix
        # this url gives yesterday https://www.nyiso.com/o/oasis-rest/oasis/currentfuel/line-yest?1659394157047
        pass


"""
pricing data

https://www.nyiso.com/en/energy-market-operational-data
"""
