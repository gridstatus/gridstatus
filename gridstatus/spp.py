import pandas as pd
import requests
from bs4 import BeautifulSoup

from gridstatus import utils
from gridstatus.base import (
    FuelMix,
    GridStatus,
    InterconnectionQueueStatus,
    ISOBase,
    NotSupported,
)


class SPP(ISOBase):
    """Southwest Power Pool (SPP)"""

    name = "Southwest Power Pool"
    iso_id = "spp"

    default_timezone = "US/Central"

    status_homepage = "https://www.spp.org/markets-operations/current-grid-conditions/"
    interconnection_homepage = (
        "https://www.spp.org/engineering/generator-interconnection/"
    )

    def get_status(self, date=None, verbose=False):
        if date != "latest":
            raise NotSupported()

        url = "https://www.spp.org/markets-operations/current-grid-conditions/"
        html_text = requests.get(url).text
        soup = BeautifulSoup(html_text, "html.parser")
        conditions_element = soup.find("h1")
        last_update_time = conditions_element.findNextSibling("p").text[14:-1]
        status_text = (
            conditions_element.findNextSibling(
                "p",
            )
            .findNextSibling("p")
            .text
        )

        date_str = last_update_time[: last_update_time.index(", at ")]
        if "a.m." in last_update_time:
            time_str = last_update_time[
                last_update_time.index(", at ") + 5 : last_update_time.index(" a.m.")
            ]
            hour, minute = map(int, time_str.split(":"))
        elif "p.m." in last_update_time:
            time_str = last_update_time[
                last_update_time.index(", at ") + 5 : last_update_time.index(" p.m.")
            ]
            hour, minute = map(int, time_str.split(":"))
            if hour < 12:
                hour += 12
        else:
            raise "Cannot parse time of status"

        date_obj = (
            pd.to_datetime(date_str)
            .replace(
                hour=hour,
                minute=minute,
            )
            .tz_localize(self.default_timezone)
        )

        if (
            status_text
            == "SPP is currently in Normal Operations with no effective advisories or alerts."
        ):
            status = "Normal"
            notes = [status_text]
        else:
            status = status_text
            notes = None

        return GridStatus(
            time=date_obj,
            status=status,
            notes=notes,
            reserves=None,
            iso=self,
        )

    def get_fuel_mix(self, date, verbose=False):

        if date != "latest":
            raise NotSupported

        url = "https://marketplace.spp.org/chart-api/gen-mix/asChart"
        r = self._get_json(url)["response"]

        data = {"Timestamp": r["labels"]}
        data.update((d["label"], d["data"]) for d in r["datasets"])

        historical_mix = pd.DataFrame(data)

        current_mix = historical_mix.iloc[0].to_dict()

        time = pd.Timestamp(current_mix.pop("Timestamp"))

        return FuelMix(time=time, mix=current_mix, iso=self.name)

    def get_load(self, date, verbose=False):
        """Returns load for last 24hrs in 5 minute intervals"""

        if date == "latest":
            return self._latest_from_today(self.get_load)

        elif utils.is_today(date):
            df = self._get_load_and_forecast(verbose=verbose)

            df = df.dropna(subset=["Actual Load"])

            df = df.rename(columns={"Actual Load": "Load"})

            df = df[["Time", "Load"]]

            return df

        else:
            raise NotSupported()

    def get_load_forecast(self, date, forecast_type="MID_TERM", verbose=False):
        """

        type (str): MID_TERM is hourly for next 7 days or SHORT_TERM is every five minutes for a few hours
        """
        df = self._get_load_and_forecast(verbose=verbose)

        # gives forecast from before current day
        # only include forecasts start at current day
        last_actual = df.dropna(subset=["Actual Load"])["Time"].max()
        current_day = last_actual.replace(hour=0, minute=0)

        current_day_forecast = df[df["Time"] > current_day].copy()

        # assume forecast is made at last actual
        current_day_forecast["Forecast Time"] = last_actual

        if forecast_type == "MID_TERM":
            forecast_col = "Mid-Term Forecast"
        elif forecast_type == "SHORT_TERM":
            forecast_col = "Short-Term Forecast"
        else:
            raise RuntimeError("Invalid forecast type")

        # there will be empty rows regardless of forecast type since they dont align
        current_day_forecast = current_day_forecast.dropna(
            subset=[forecast_col],
        )

        current_day_forecast = current_day_forecast[
            ["Forecast Time", "Time", forecast_col]
        ].rename({forecast_col: "Load Forecast"}, axis=1)

        return current_day_forecast

    def _get_load_and_forecast(self, verbose=False):
        url = "https://marketplace.spp.org/chart-api/load-forecast/asChart"

        if verbose:
            print("Getting load and forecast from {}".format(url))

        r = self._get_json(url)["response"]

        data = {"Time": r["labels"]}
        for d in r["datasets"][:3]:
            if d["label"] == "Actual Load":
                data["Actual Load"] = d["data"]
            elif d["label"] == "Mid-Term Load Forecast":
                data["Mid-Term Forecast"] = d["data"]
            elif d["label"] == "Short-Term Load Forecast":
                data["Short-Term Forecast"] = d["data"]

        df = pd.DataFrame(data)

        df["Time"] = pd.to_datetime(
            df["Time"],
        ).dt.tz_convert(self.default_timezone)

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

    def get_interconnection_queue(self, verbose=False):
        """Get interconnection queue

        Returns:
            pd.DataFrame: Interconnection queue


        """
        url = "https://opsportal.spp.org/Studies/GenerateActiveCSV"
        if verbose:
            print("Getting interconnection queue from {}".format(url))

        queue = pd.read_csv(url, skiprows=1)

        queue["Status (Original)"] = queue["Status"]

        queue["Status"] = queue["Status"].map(
            {
                "IA FULLY EXECUTED/COMMERCIAL OPERATION": InterconnectionQueueStatus.COMPLETED.value,
                "IA FULLY EXECUTED/ON SCHEDULE": InterconnectionQueueStatus.COMPLETED.value,
                "IA FULLY EXECUTED/ON SUSPENSION": InterconnectionQueueStatus.COMPLETED.value,
                "IA PENDING": InterconnectionQueueStatus.ACTIVE.value,
                "DISIS STAGE": InterconnectionQueueStatus.ACTIVE.value,
                "None": InterconnectionQueueStatus.ACTIVE.value,
            },
        )

        queue["Generation Type"] = queue[["Generation Type", "Fuel Type"]].apply(
            lambda x: " - ".join(x.dropna()),
            axis=1,
        )

        queue["Proposed Completion Date"] = queue["Commercial Operation Date"]

        rename = {
            "Generation Interconnection Number": "Queue ID",
            " Nearest Town or County": "County",
            "State": "State",
            "TO at POI": "Transmission Owner",
            "Capacity": "Capacity (MW)",
            "MAX Summer MW": "Summer Capacity (MW)",
            "MAX Winter MW": "Winter Capacity (MW)",
            "Generation Type": "Generation Type",
            "Request Received": "Queue Date",
            "Substation or Line": "Interconnection Location",
        }

        # todo: there are a few columns being parsed as "unamed" that aren't being included but should
        extra_columns = [
            "In-Service Date",
            "Commercial Operation Date",
            "Cessation Date",
            "Current Cluster",
            "Cluster Group",
            "Replacement Generator Commercial Op Date",
            "Service Type",
        ]

        missing = [
            "Project Name",
            "Interconnecting Entity",
            "Withdrawn Date",
            "Withdrawal Comment",
            "Actual Completion Date",
        ]

        queue = utils.format_interconnection_df(
            queue=queue,
            rename=rename,
            extra=extra_columns,
            missing=missing,
        )

        return queue


# historical generation mix
# https://marketplace.spp.org/pages/generation-mix-rolling-365
# https://marketplace.spp.org/chart-api/gen-mix-365/asFile
# 15mb file with five minute resolution
