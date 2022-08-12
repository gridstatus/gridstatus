import pandas as pd

from isodata.base import FuelMix, ISOBase


class SPP(ISOBase):
    name = "Southwest Power Pool"
    iso_id = "spp"

    def get_latest_fuel_mix(self):
        url = "https://marketplace.spp.org/chart-api/gen-mix/asChart"
        r = self._get_json(url)["response"]

        data = {"Timestamp": r["labels"]}
        data.update((d["label"], d["data"]) for d in r["datasets"])

        historical_mix = pd.DataFrame(data)

        current_mix = historical_mix.iloc[0].to_dict()

        time = pd.Timestamp(current_mix.pop("Timestamp"))

        return FuelMix(time=time, mix=current_mix, iso=self.name)

    def get_latest_supply(self):
        """Returns most recent data point for supply in MW"""
        return self._latest_supply_from_fuel_mix()

    def get_latest_demand(self):
        return self._latest_from_today(self.get_demand_today)

    def get_demand_today(self):
        """Returns demand for last 24hrs in 5 minute intervals"""
        url = "https://marketplace.spp.org/chart-api/load-forecast/asChart"
        r = self._get_json(url)["response"]

        load = r["datasets"][2]

        # sanity check to make sure direct index of 2 is correct
        assert load["label"] == "Actual Load"

        df = pd.DataFrame({"Time": r["labels"], "Demand": load["data"]}).dropna(
            subset=["Demand"],
        )

        df["Time"] = pd.to_datetime(df["Time"])

        return df

        # todo where does date got in argument order
        # def get_historical_lmp(self, date, market: str, nodes: list):
        # 5 minute interal data
        # https://marketplace.spp.org/file-browser-api/download/rtbm-lmp-by-location?path=/2022/08/By_Interval/08/RTBM-LMP-SL-202208082125.csv

        # hub and interface prices
        # https://marketplace.spp.org/pages/hub-and-interface-prices

        # historical generation mix
        # https://marketplace.spp.org/pages/generation-mix-rolling-365
        # https://marketplace.spp.org/chart-api/gen-mix-365/asFile
        # 15mb file with five minute resolution


# historical generation mix
# https://marketplace.spp.org/pages/generation-mix-rolling-365
# https://marketplace.spp.org/chart-api/gen-mix-365/asFile
# 15mb file with five minute resolution
