import pandas as pd
from pandas import Timestamp

from isodata.base import FuelMix, ISOBase


class MISO(ISOBase):
    BASE = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx"

    name = "Midcontinent ISO"
    iso_id = "miso"
    # says EST in time stamp but EDT is currently in affect. EST == CDT, so using central time for now
    default_timezone = "US/Central"

    REAL_TIME_5_MIN = "REAL_TIME_5_MIN"
    DAY_AHEAD_HOURLY = "DAY_AHEAD_HOURLY"

    hubs = [
        "ILLINOIS.HUB",
        "INDIANA.HUB",
        "LOUISIANA.HUB",
        "MICHIGAN.HUB",
        "MINN.HUB",
        "MS.HUB",
        "TEXAS.HUB",
        "ARKANSAS.HUB",
    ]

    def __init__(self) -> None:
        super().__init__()

    def get_latest_fuel_mix(self):
        url = self.BASE + "?messageType=getfuelmix&returnType=json"
        r = self._get_json(url)

        time = pd.to_datetime(r["Fuel"]["Type"][0]["INTERVALEST"]).tz_localize(
            self.default_timezone,
        )

        mix = {}
        for fuel in r["Fuel"]["Type"]:
            amount = int(fuel["ACT"])
            if amount == -1:
                amount = 0
            mix[fuel["CATEGORY"]] = amount

        # print(r["TotalMW"])  # todo - this total does add up to each part

        fm = FuelMix(time=time, mix=mix, iso=self.name)
        return fm

    def get_latest_demand(self):
        # this is same result as using get_demand_today
        url = "https://misotodaysoutlook.azurewebsites.net/api/Outlook"
        r = self._get_json(url)

        return {
            "time": pd.to_datetime(r[1]["d"]).tz_localize(self.default_timezone),
            "demand": float(r[1]["v"].replace(",", "")),
        }

    def get_latest_supply(self):
        """Returns most recent data point for supply in MW"""
        return self._latest_supply_from_fuel_mix()

    def get_demand_today(self):
        url = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=gettotalload&returnType=json"
        r = self._get_json(url)

        date = pd.to_datetime(r["LoadInfo"]["RefId"].split(" ")[0])

        df = pd.DataFrame([x["Load"] for x in r["LoadInfo"]["FiveMinTotalLoad"]])

        df["Time"] = df["Time"].apply(
            lambda x, date=date: date
            + pd.Timedelta(
                hours=int(
                    x.split(":")[0],
                ),
                minutes=int(x.split(":")[1]),
            ),
        )
        df["Time"] = df["Time"].dt.tz_localize(self.default_timezone)
        df = df.rename(columns={"Value": "Demand"})

        df["Demand"] = pd.to_numeric(df["Demand"])

        return df

    def get_latest_lmp(self, market: str, nodes: list):
        """
        Supported Markets:

        REAL_TIME_5_MIN (FiveMinLMP)
        DAY_AHEAD_HOURLY (DayAheadExPostLMP)
        """
        url = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=getLMPConsolidatedTable&returnType=json"
        r = self._get_json(url)

        time = r["LMPData"]["RefId"]
        time_str = time[:11] + " " + time[-9:]
        time = pd.to_datetime(time_str).tz_localize(self.default_timezone)
        if market == self.REAL_TIME_5_MIN:
            data = pd.DataFrame(r["LMPData"]["FiveMinLMP"]["PricingNode"])
        elif market == self.DAY_AHEAD_HOURLY:
            data = pd.DataFrame(
                r["LMPData"]["DayAheadExPostLMP"]["PricingNode"],
            )
            time = time.ceil("H")

        rename = {
            "name": "Node",
            "LMP": "LMP",
            "MLC": "Loss",
            "MCC": "Congestion",
        }

        data.rename(columns=rename, inplace=True)

        data[["LMP", "Loss", "Congestion"]] = data[["LMP", "Loss", "Congestion"]].apply(
            pd.to_numeric,
            errors="coerce",
        )

        data["Energy"] = data["LMP"] - data["Loss"] - data["Congestion"]
        data["Time"] = time
        data["Market"] = market

        data = data[
            [
                "Time",
                "Market",
                "Node",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
        ]

        return data


"""
Notes

- Real-time 5-minute LMP data for current day, previous day available
https://api.misoenergy.org/MISORTWDBIReporter/Reporter.asmx?messageType=rollingmarketday&returnType=json

- market reports https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/#nt=
historical fuel mix: https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/#nt=%2FMarketReportType%3ASummary%2FMarketReportName%3AHistorical%20Generation%20Fuel%20Mix%20(xlsx)&t=10&p=0&s=MarketReportPublished&sd=desc

- ancillary services available in consolidate api

"""
