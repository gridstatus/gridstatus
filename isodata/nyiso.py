from .base import ISOBase, FuelMix
import pandas as pd


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
