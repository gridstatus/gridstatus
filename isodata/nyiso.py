from .base import ISOBase, FuelMix
import pandas as pd


class NYISO(ISOBase):
    name = "New York ISO"
    iso_id = "nyiso"

    # def get_latest_status(self):
    #     # https://www.nyiso.com/en/system-conditions
    #     pass

    def get_latest_fuel_mix(self):
        # note: this is simlar datastructure to pjm
        url = "https://www.nyiso.com/o/oasis-rest/oasis/currentfuel/line-current"
        data = self._get_json(url)
        mix_df = pd.DataFrame(data["data"])
        time_str = mix_df["timeStamp"].max()
        time = pd.Timestamp(time_str)
        mix_df = mix_df[mix_df["timeStamp"]
                        == time_str].set_index("fuelCategory")["genMWh"]
        mix_dict = mix_df.to_dict()
        return FuelMix(time=time, mix=mix_dict, iso=self.name)

    # def get_historical_fuel_mix(self):
    #     # above url gives daily fuel mix
    #     # this url gives yesterday https://www.nyiso.com/o/oasis-rest/oasis/currentfuel/line-yest?1659394157047
    #     pass


"""
pricing data

https://www.nyiso.com/en/energy-market-operational-data
"""
