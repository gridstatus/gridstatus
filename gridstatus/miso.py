import json

import pandas as pd
import requests

from gridstatus import utils
from gridstatus.base import ISOBase, Markets, NotSupported
from gridstatus.lmp_config import lmp_config
from gridstatus.logging import log


class MISO(ISOBase):
    """Midcontinent Independent System Operator (MISO)"""

    BASE = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx"

    interconnection_homepage = (
        "https://www.misoenergy.org/planning/generator-interconnection/GI_Queue/"
    )

    name = "Midcontinent ISO"
    iso_id = "miso"
    # miso spans multiple timezones, so picking central
    # all parsing is done in EST since that is what api returns
    default_timezone = "US/Eastern"

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
        """Get the fuel mix for a given day for a provided MISO.

        Arguments:
            date (datetime.date, str): "latest", "today", or an object
                that can be parsed as a datetime for the day to return data.

            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: DataFrame with columns "Time", "Load", "Fuel Mix"
        """
        if date != "latest":
            raise NotSupported()

        url = self.BASE + "?messageType=getfuelmix&returnType=json"
        r = self._get_json(url, verbose=verbose)

        time = (
            pd.to_datetime(r["Fuel"]["Type"][0]["INTERVALEST"])
            .tz_localize(
                "EST",
            )
            .tz_convert(
                self.default_timezone,
            )
        )

        mix = {}
        for fuel in r["Fuel"]["Type"]:
            amount = int(fuel["ACT"])
            if amount == -1:
                amount = 0
            mix[fuel["CATEGORY"]] = amount

        df = pd.DataFrame(mix, index=[time])
        df.index.name = "Interval Start"
        df = df.reset_index()
        df = add_interval_end(df, 5)
        return df

    def get_load(self, date, verbose=False):
        if date == "latest":
            return self._latest_from_today(self.get_load, verbose=verbose)

        elif utils.is_today(date, tz=self.default_timezone):
            r = self._get_load_and_forecast_data(verbose=verbose)

            date = pd.to_datetime(r["LoadInfo"]["RefId"].split(" ")[0])

            df = pd.DataFrame([x["Load"] for x in r["LoadInfo"]["FiveMinTotalLoad"]])

            df["Interval Start"] = df["Time"].apply(
                lambda x, date=date: date
                + pd.Timedelta(
                    hours=int(
                        x.split(":")[0],
                    ),
                    minutes=int(x.split(":")[1]),
                ),
            )
            df["Interval Start"] = (
                df["Interval Start"]
                .dt.tz_localize("EST")
                .dt.tz_convert(
                    self.default_timezone,
                )
            )
            df = df.rename(columns={"Value": "Load"})
            df["Load"] = pd.to_numeric(df["Load"])
            df = add_interval_end(df, 5)
            return df

        else:
            raise NotSupported

    def get_load_forecast(self, date, verbose=False):
        if not utils.is_today(date, self.default_timezone):
            raise NotSupported()

        r = self._get_load_and_forecast_data(verbose=verbose)

        date = (
            pd.to_datetime(r["LoadInfo"]["RefId"].split(" ")[0])
            .tz_localize(
                tz="EST",
            )
            .tz_convert(
                self.default_timezone,
            )
        )

        df = pd.DataFrame(
            [x["Forecast"] for x in r["LoadInfo"]["MediumTermLoadForecast"]],
        )

        df["Interval Start"] = date + pd.to_timedelta(
            df["HourEnding"].astype(int) - 1,
            "h",
        )

        df["Forecast Time"] = date

        df = add_interval_end(df, 60)

        df = df[
            ["Time", "Interval Start", "Interval End", "Forecast Time", "LoadForecast"]
        ].rename(
            columns={"LoadForecast": "Load Forecast"},
        )

        return df

    def _get_load_and_forecast_data(self, verbose=False):
        url = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=gettotalload&returnType=json"  # noqa
        r = self._get_json(url, verbose=verbose)
        return r

    @lmp_config(
        supports={
            Markets.REAL_TIME_5_MIN: ["latest"],
            Markets.DAY_AHEAD_HOURLY: ["latest"],
        },
    )
    def get_lmp(self, date, market: str, locations: list = None, verbose=False):
        """
        Supported Markets:
            - ``REAL_TIME_5_MIN`` - (FiveMinLMP)
            - ``DAY_AHEAD_HOURLY`` - (DayAheadExPostLMP)
        """
        if locations is None:
            locations = "ALL"

        url = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=getLMPConsolidatedTable&returnType=json"  # noqa
        r = self._get_json(url, verbose=verbose)

        time = r["LMPData"]["RefId"]
        time_str = time[:11] + " " + time[-9:-4]
        time_zone = time[-3:]
        interval_start = (
            pd.to_datetime(time_str)
            .tz_localize(
                time_zone,
            )
            .tz_convert(self.default_timezone)
        )

        if market == Markets.REAL_TIME_5_MIN:
            data = pd.DataFrame(r["LMPData"]["FiveMinLMP"]["PricingNode"])
            interval_duration = 5
        elif market == Markets.DAY_AHEAD_HOURLY:
            data = pd.DataFrame(
                r["LMPData"]["DayAheadExPostLMP"]["PricingNode"],
            )
            interval_start = interval_start.floor("H")
            interval_duration = 60

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
        data["Interval Start"] = interval_start
        data = add_interval_end(data, interval_duration)
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
                "Interval Start",
                "Interval End",
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

    def get_interconnection_queue(self, verbose=False):
        """Get the interconnection queue

        Returns:
            pandas.DataFrame: Interconnection queue
        """
        url = "https://www.misoenergy.org/api/giqueue/getprojects"

        msg = f"Downloading interconnection queue from {url}"
        log(msg, verbose)

        json_str = requests.get(url).text
        data = json.loads(json_str)
        # todo there are also study documents available:  https://www.misoenergy.org/planning/generator-interconnection/GI_Queue/gi-interactive-queue/
        # there is also a map that plots the locations of these projects:
        queue = pd.DataFrame(data)

        queue = queue.rename(
            columns={
                "postGIAStatus": "Post Generator Interconnection Agreement Status",
                "doneDate": "Interconnection Approval Date",
            },
        )

        queue["Capacity (MW)"] = queue[
            [
                "summerNetMW",
                "winterNetMW",
            ]
        ].max(axis=1)

        rename = {
            "projectNumber": "Queue ID",
            "county": "County",
            "state": "State",
            "transmissionOwner": "Transmission Owner",
            "poiName": "Interconnection Location",
            "queueDate": "Queue Date",
            "withdrawnDate": "Withdrawn Date",
            "applicationStatus": "Status",
            "Capacity (MW)": "Capacity (MW)",
            "summerNetMW": "Summer Capacity (MW)",
            "winterNetMW": "Winter Capacity (MW)",
            "negInService": "Proposed Completion Date",
            "fuelType": "Generation Type",
        }

        extra_columns = [
            "facilityType",
            "Post Generator Interconnection Agreement Status",
            "Interconnection Approval Date",
            "inService",
            "giaToExec",
            "studyCycle",
            "studyGroup",
            "studyPhase",
            "svcType",
            "dp1ErisMw",
            "dp1NrisMw",
            "dp2ErisMw",
            "dp2NrisMw",
            "sisPhase1",
        ]

        missing = [
            # todo the actual complettion date
            # can be calculated by looking at status and other date columns
            "Actual Completion Date",
            "Withdrawal Comment",
            "Project Name",
            "Interconnecting Entity",
        ]

        queue = utils.format_interconnection_df(
            queue=queue,
            rename=rename,
            extra=extra_columns,
            missing=missing,
        )

        return queue


def add_interval_end(df, duration_min):
    """Add an interval end column to a dataframe

    Args:
        df (pandas.DataFrame): Dataframe with a time column
        duration_min (int): Interval duration in minutes

    Returns:
        pandas.DataFrame: Dataframe with an interval end column
    """
    df["Interval End"] = df["Interval Start"] + pd.Timedelta(minutes=duration_min)
    df["Time"] = df["Interval Start"]
    df = utils.move_cols_to_front(
        df,
        ["Time", "Interval Start", "Interval End"],
    )
    return df


"""
Notes

- Real-time 5-minute LMP data for current day, previous day available
https://api.misoenergy.org/MISORTWDBIReporter/Reporter.asmx?messageType=rollingmarketday&returnType=json

- market reports https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/#nt=
historical fuel mix: https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/#nt=%2FMarketReportType%3ASummary%2FMarketReportName%3AHistorical%20Generation%20Fuel%20Mix%20(xlsx)&t=10&p=0&s=MarketReportPublished&sd=desc

- ancillary services available in consolidate api

"""  # noqa
