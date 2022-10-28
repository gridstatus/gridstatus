import pandas as pd
from pandas import Timestamp

from gridstatus import utils
from gridstatus.base import FuelMix, ISOBase, Markets, NotSupported


class MISO(ISOBase):
    """Midcontinent Independent System Operator (MISO)"""

    BASE = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx"

    name = "Midcontinent ISO"
    iso_id = "miso"
    # says EST in time stamp but EDT is currently in affect. EST == CDT, so using central time for now
    default_timezone = "US/Central"

    markets = [Markets.REAL_TIME_5_MIN, Markets.DAY_AHEAD_HOURLY]

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

    def get_fuel_mix(self, date, verbose=False):
        if date != "latest":
            raise NotSupported()

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

    def get_supply(self, date, end=None, verbose=False):
        """Get supply for a date in hourly intervals"""
        return self._get_supply(date=date, end=end, verbose=verbose)

    def get_load(self, date, verbose=False):
        if date == "latest":
            return self._latest_from_today(self.get_load, verbose=verbose)

        elif utils.is_today(date):
            r = self._get_load_and_forecast_data()

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
            df = df.rename(columns={"Value": "Load"})
            df["Load"] = pd.to_numeric(df["Load"])

            return df
        else:
            raise NotSupported

    def get_load_forecast(self, date, verbose=False):

        if date != "today":
            raise NotSupported()

        r = self._get_load_and_forecast_data()

        date = pd.to_datetime(r["LoadInfo"]["RefId"].split(" ")[0]).tz_localize(
            tz=self.default_timezone,
        )

        df = pd.DataFrame(
            [x["Forecast"] for x in r["LoadInfo"]["MediumTermLoadForecast"]],
        )

        df["Time"] = date + pd.to_timedelta(df["HourEnding"].astype(int) - 1, "h")

        df["Forecast Time"] = date

        df = df[["Forecast Time", "Time", "LoadForecast"]].rename(
            columns={"LoadForecast": "Load Forecast"},
        )

        return df

    def _get_load_and_forecast_data(self):
        url = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=gettotalload&returnType=json"
        r = self._get_json(url)
        return r

    def get_lmp(self, date, market: str, locations: list = None):
        """
        Supported Markets:

        REAL_TIME_5_MIN (FiveMinLMP)
        DAY_AHEAD_HOURLY (DayAheadExPostLMP)
        """
        if date != "latest":
            raise NotSupported()

        if locations is None:
            locations = "ALL"

        url = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=getLMPConsolidatedTable&returnType=json"
        r = self._get_json(url)

        time = r["LMPData"]["RefId"]
        time_str = time[:11] + " " + time[-9:]
        time = pd.to_datetime(time_str).tz_localize(self.default_timezone)

        market = Markets(market)
        if market == Markets.REAL_TIME_5_MIN:
            data = pd.DataFrame(r["LMPData"]["FiveMinLMP"]["PricingNode"])
        elif market == Markets.DAY_AHEAD_HOURLY:
            data = pd.DataFrame(
                r["LMPData"]["DayAheadExPostLMP"]["PricingNode"],
            )
            time = time.ceil("H")

        rename = {
            "name": "Location",
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
        data["Market"] = market.value
        data["Location Type"] = "Pricing Node"
        data.loc[
            data["Location"].str.endswith(
                ".HUB",
            ),
            "Location Type",
        ] = "Hub"
        data = data[
            [
                "Time",
                "Market",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
        ]

        data = utils.filter_lmp_locations(data, locations)

        return data


"""
Notes

- Real-time 5-minute LMP data for current day, previous day available
https://api.misoenergy.org/MISORTWDBIReporter/Reporter.asmx?messageType=rollingmarketday&returnType=json

- market reports https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/#nt=
historical fuel mix: https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/#nt=%2FMarketReportType%3ASummary%2FMarketReportName%3AHistorical%20Generation%20Fuel%20Mix%20(xlsx)&t=10&p=0&s=MarketReportPublished&sd=desc

- ancillary services available in consolidate api

"""
