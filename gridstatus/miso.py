import json
import urllib
import warnings
from typing import BinaryIO

import pandas as pd
import requests

from gridstatus import utils
from gridstatus.base import ISOBase, Markets, NoDataFoundException, NotSupported
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import log
from gridstatus.lmp_config import lmp_config


class MISO(ISOBase):
    """Midcontinent Independent System Operator (MISO)"""

    BASE = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx"

    interconnection_homepage = (
        "https://www.misoenergy.org/planning/generator-interconnection/GI_Queue/"
    )

    name = "Midcontinent ISO"
    iso_id = "miso"

    # Parsing of raw data is done in EST since that is what api returns and what
    # MISO operates in
    # Source: https://www.rtoinsider.com/25291-ferc-oks-miso-use-of-eastern-standard-time-in-day-ahead-market/ # noqa
    default_timezone = "EST"

    markets = [
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_5_MIN_WEEKLY,
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_HOURLY_FINAL,
        Markets.REAL_TIME_HOURLY_PRELIM,
    ]

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

        time = pd.to_datetime(r["Fuel"]["Type"][0]["INTERVALEST"]).tz_localize(
            self.default_timezone,
        )

        mix = {}
        for fuel in r["Fuel"]["Type"]:
            amount = float(fuel["ACT"])
            mix[fuel["CATEGORY"]] = amount

        df = pd.DataFrame(mix, index=[time])
        df.index.name = "Interval Start"
        df = df.reset_index()
        df = add_interval_end(df, 5)
        return df

    def get_load(self, date, verbose=False):
        if date == "latest":
            return self.get_load(date="today", verbose=verbose)

        elif utils.is_today(date, tz=self.default_timezone):
            r = self._get_load_data(verbose=verbose)

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
            df["Interval Start"] = df["Interval Start"].dt.tz_localize(
                self.default_timezone,
            )
            df = df.rename(columns={"Value": "Load"})
            df["Load"] = pd.to_numeric(df["Load"])
            df = add_interval_end(df, 5)
            return df

        else:
            raise NotSupported

    @support_date_range(frequency="DAY_START")
    def get_load_forecast(self, date, end=None, verbose=False):
        """
        https://docs.misoenergy.org/marketreports/YYYYMMDD_df_al.xls
        """
        if date == "latest":
            return self.get_load_forecast(date="today", verbose=verbose)

        url = f"https://docs.misoenergy.org/marketreports/{date.strftime('%Y%m%d')}_df_al.xls"  # noqa

        log(msg=f"Downloading load forecast data from {url}", verbose=verbose)
        df = pd.read_excel(url, sheet_name="Sheet1", skiprows=4, skipfooter=1)

        df = df.dropna(subset=["HourEnding"])
        df = df.loc[df["HourEnding"] != "HourEnding"]
        df.loc[:, "HourEnding"] = df["HourEnding"].astype(int)

        df["Interval End"] = (
            pd.to_datetime(df["Market Day"])
            + pd.to_timedelta(df["HourEnding"], unit="h")
        ).dt.tz_localize(self.default_timezone)

        df["Interval Start"] = df["Interval End"] - pd.Timedelta(hours=1)

        # Assume publish time is 12 am. MLTF is made every 15 minutes, but maybe
        # released only once a day
        # https://pubs.naruc.org/pub/64EABF52-1866-DAAC-99FB-ACEE7EEC8DAD
        df["Publish Time"] = date.normalize()

        df.columns = df.columns.map(lambda x: x.replace("(MWh)", "").strip())

        df = utils.move_cols_to_front(
            df,
            ["Interval Start", "Interval End", "Publish Time"],
        ).drop(columns=["Market Day", "HourEnding"])

        # Include only forecasts for the current day into the future
        df = df.loc[
            df["Interval Start"] >= date,
            [col for col in df if "ActualLoad" not in col],
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    def _get_load_data(self, verbose=False):
        url = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=gettotalload&returnType=json"  # noqa
        r = self._get_json(url, verbose=verbose)
        return r

    # Older datasets do not have every region. In that case, we insert the column
    # as null
    solar_and_wind_forecast_region_cols = [
        "North",
        "Central",
        "South",
        "MISO",
    ]

    solar_and_wind_forecast_cols = [
        "Interval Start",
        "Interval End",
        "Publish Time",
        *solar_and_wind_forecast_region_cols,
    ]

    @support_date_range(frequency="DAY_START")
    def get_solar_forecast(self, date, verbose=False):
        if date == "latest":
            return self.get_solar_forecast(date="today", verbose=verbose)

        return self._get_solar_and_wind_forecast_data(
            date,
            fuel="solar",
            verbose=verbose,
        )

    @support_date_range(frequency="DAY_START")
    def get_wind_forecast(self, date, verbose=False):
        if date == "latest":
            return self.get_wind_forecast(date="today", verbose=verbose)

        return self._get_solar_and_wind_forecast_data(
            date,
            fuel="wind",
            verbose=verbose,
        )

    def _get_mom_forecast_report(self, date, verbose=False):
        # Example url: https://docs.misoenergy.org/marketreports/20240327_mom.xlsx
        url = f"https://docs.misoenergy.org/marketreports/{date.strftime('%Y%m%d')}_mom.xlsx"  # noqa

        log(f"Downloading mom forecast data from {url}", verbose)

        try:
            # Ignore the UserWarning from openpyxl about styles
            warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
            excel_file = pd.ExcelFile(url, engine="openpyxl")
        except urllib.error.HTTPError as e:
            if e.status == 404:
                raise NoDataFoundException(
                    f"No solar or wind forecast found for {date}",
                )

        return excel_file

    def _get_solar_and_wind_forecast_data(self, date, fuel, verbose=False):
        excel_file = self._get_mom_forecast_report(date, verbose)
        publish_time = pd.to_datetime(excel_file.book.properties.modified, utc=True)

        # The data schema changes on 2022-06-13
        skiprows = (
            4 if date > pd.Timestamp("2022-06-12", tz=self.default_timezone) else 3
        )

        df = (
            pd.read_excel(
                excel_file,
                sheet_name=f"{fuel.upper()} HOURLY",
                skiprows=skiprows,
                skipfooter=1,
            )
            .dropna(how="all")
            .assign(**{"Publish Time": publish_time})
        )

        # Handle older datasets
        df = df.rename(columns={"Day HE": "DAY HE"})

        # Convert column that looks like this **03/27/2024 1 **03/27/2024 24 or to
        # a valid datetime. Assume Hour Ending is in local time

        df["hour"] = df["DAY HE"].str.extract(r"(\d+)$").astype(int)
        df["date"] = pd.to_datetime(
            df["DAY HE"].str.replace("**", "").str.split(" ").str[0],
            format="%m/%d/%Y",
        )

        df["Interval Start"] = (
            pd.to_datetime(df["date"])
            + pd.to_timedelta(
                df["hour"] - 1,
                "h",
            )
            # This forecast does not handle DST changes
        ).dt.tz_localize(self.default_timezone)

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)

        for col in self.solar_and_wind_forecast_region_cols:
            if col not in df.columns:
                df[col] = pd.NA

        return df[self.solar_and_wind_forecast_cols]

    @support_date_range(frequency="W-MON")
    def get_lmp_weekly(self, date, end=None, verbose=False):
        """Retrieves weekly lmp data that includes price corrections to the real time
        data.

        Data from: https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/#nt=%2FMarketReportType%3AHistorical%20LMP%2FMarketReportName%3AWeekly%20Real-Time%205-Min%20LMP%20(zip)&t=10&p=0&s=MarketReportPublished&sd=desc
        """
        if date == "latest" or utils.is_today(date, tz=self.default_timezone):
            raise NotSupported("Only historical data is available for weekly LMPs")

        if not date.weekday() == 0:
            log("Weekly LMP data is only available for Mondays", verbose)
            log("Changing date to the previous Monday", verbose)
            date -= pd.DateOffset(days=date.weekday())

        # The data file contains data starting two weeks before the date and
        # ending one week before the date. To get data that covers the date, we
        # need to add two weeks to the date
        date += pd.DateOffset(weeks=2)

        download_url = f"https://docs.misoenergy.org/marketreports/{date.strftime('%Y%m%d')}_5MIN_LMP.zip"

        try:
            df = pd.read_csv(
                download_url,
                compression="zip",
                skiprows=4,
                skipfooter=1,
                # The c parser does not support skipfooter
                engine="python",
            )
        except urllib.error.HTTPError:
            raise NoDataFoundException(f"No LMP data found for {date}")

        return self._handle_lmp_weekly(df, verbose)

    def _handle_lmp_weekly(self, df, verbose=False):
        df["Interval Start"] = pd.to_datetime(df["MKTHOUR_EST"]).dt.tz_localize(
            self.default_timezone,
        )
        df = add_interval_end(df, 5).drop(columns=["Time"])

        df = df.rename(
            columns={
                "CON_LMP": "Congestion",
                "LOSS_LMP": "Loss",
                "PNODENAME": "Location",
            },
        )
        node_to_type = self._get_node_to_type_mapping(verbose)

        df = df.merge(
            node_to_type,
            left_on="Location",
            right_on="Node",
            how="left",
        )

        df["Energy"] = df["LMP"] - df["Loss"] - df["Congestion"]
        df["Market"] = Markets.REAL_TIME_5_MIN_WEEKLY.value

        df = utils.move_cols_to_front(
            df,
            [
                "Interval Start",
                "Interval End",
                "Market",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ],
        ).drop(columns=["MKTHOUR_EST", "Node"])

        return df.sort_values("Interval Start").reset_index(drop=True)

    @lmp_config(
        supports={
            Markets.REAL_TIME_5_MIN: ["latest", "today"],
            Markets.DAY_AHEAD_HOURLY: ["today", "historical"],
            Markets.REAL_TIME_HOURLY_FINAL: ["historical"],
            Markets.REAL_TIME_HOURLY_PRELIM: ["historical"],
        },
    )
    @support_date_range(frequency="DAY_START")
    def get_lmp(
        self,
        date,
        end=None,
        market: str = Markets.REAL_TIME_5_MIN,
        locations: list = "ALL",
        verbose=False,
    ):
        """
        Supported Markets:
            - ``REAL_TIME_5_MIN`` - (FiveMinLMP)
            - ``DAY_AHEAD_HOURLY`` - (DayAheadExPostLMP)
            - ``REAL_TIME_HOURLY_FINAL`` - (RealTimeFinalLMP)
            - ``REAL_TIME_HOURLY_PRELIM`` - (RealTimePrelimLMP) Only 4 days of data
                available, with the most recent being yesterday.
        """
        if market == Markets.REAL_TIME_5_MIN:
            latest_url = "https://api.misoenergy.org/MISORTWDBIReporter/Reporter.asmx?messageType=currentinterval&returnType=csv"  # noqa
            today_url = "https://api.misoenergy.org/MISORTWDBIReporter/Reporter.asmx?messageType=rollingmarketday&returnType=csv"  # noqa

            if date == "latest":
                url = latest_url
            elif utils.is_today(date, tz=self.default_timezone):
                url = today_url

            log(f"Downloading LMP data from {url}", verbose)
            data = pd.read_csv(url)

            data["Interval Start"] = pd.to_datetime(data["INTERVAL"]).dt.tz_localize(
                self.default_timezone,
            )

            node_to_type = self._get_node_to_type_mapping()

            data = data.merge(
                node_to_type,
                left_on="CPNODE",
                right_on="Node",
                how="left",
            )

            interval_duration = 5

        elif market in [
            Markets.REAL_TIME_HOURLY_FINAL,
            Markets.REAL_TIME_HOURLY_PRELIM,
            Markets.DAY_AHEAD_HOURLY,
        ]:
            date_str = date.strftime("%Y%m%d")

            if market == Markets.DAY_AHEAD_HOURLY:
                url = f"https://docs.misoenergy.org/marketreports/{date_str}_da_expost_lmp.csv"  # noqa
            elif market == Markets.REAL_TIME_HOURLY_FINAL:
                url = f"https://docs.misoenergy.org/marketreports/{date_str}_rt_lmp_final.csv"  # noqa
            elif market == Markets.REAL_TIME_HOURLY_PRELIM:
                url = f"https://docs.misoenergy.org/marketreports/{date_str}_rt_lmp_prelim.csv"

            log(f"Downloading LMP data from {url}", verbose)
            raw_data = pd.read_csv(url, skiprows=4)
            data = self._handle_hourly_lmp(date, raw_data)
            interval_duration = 60

        data = data.sort_values(["Interval Start", "Node"])
        data = add_interval_end(data, interval_duration)

        data = data.rename(
            columns={
                "Node": "Location",
                "Type": "Location Type",
                "LMP": "LMP",
                "MLC": "Loss",
                "MCC": "Congestion",
            },
        )

        data[["LMP", "Loss", "Congestion"]] = data[["LMP", "Loss", "Congestion"]].apply(
            pd.to_numeric,
            errors="coerce",
        )

        data["Energy"] = data["LMP"] - data["Loss"] - data["Congestion"]
        data["Market"] = market.value

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

    def _handle_hourly_lmp(self, date, raw_data):
        data_melted = raw_data.melt(
            id_vars=["Node", "Type", "Value"],
            value_vars=[col for col in raw_data.columns if col.startswith("HE")],
            var_name="HE",
            value_name="value",
        )

        data = data_melted.pivot_table(
            index=["Node", "Type", "HE"],
            columns="Value",
            values="value",
            aggfunc="first",
        ).reset_index()

        data["Interval Start"] = (
            data["HE"]
            .apply(
                lambda x: date.replace(tzinfo=None, hour=int(x.split(" ")[1]) - 1),
            )
            .dt.tz_localize(self.default_timezone)
        )

        return data

    def _get_node_to_type_mapping(self, verbose=False):
        # use dam to get location types
        today = utils._handle_date("today", self.default_timezone)
        url = f"https://docs.misoenergy.org/marketreports/{today.strftime('%Y%m%d')}_da_expost_lmp.csv"  # noqa
        log(f"Downloading LMP data from {url}", verbose)
        today_dam_data = pd.read_csv(url, skiprows=4)
        node_to_type = (
            today_dam_data[["Node", "Type"]]
            .drop_duplicates()
            .rename(columns={"Type": "Location Type"})
        )

        return node_to_type

    def get_raw_interconnection_queue(self, verbose=False) -> BinaryIO:
        url = "https://www.misoenergy.org/api/giqueue/getprojects"

        msg = f"Downloading interconnection queue from {url}"
        log(msg, verbose)

        response = requests.get(url)
        return utils.get_response_blob(response)

    def get_interconnection_queue(self, verbose=False):
        """Get the interconnection queue

        Returns:
            pandas.DataFrame: Interconnection queue
        """
        raw_data = self.get_raw_interconnection_queue(verbose)
        data = json.loads(raw_data.read().decode("utf-8"))
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
