import copy
import io
import time
import warnings
from contextlib import redirect_stderr
from zipfile import ZipFile

import numpy as np
import pandas as pd
import requests
import tabula
from tabulate import tabulate
from termcolor import colored

from gridstatus import caiso_utils, utils
from gridstatus.base import (
    GridStatus,
    ISOBase,
    Markets,
    NoDataFoundException,
    NotSupported,
)
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import log
from gridstatus.lmp_config import lmp_config

_BASE = "https://www.caiso.com/outlook/SP"
_HISTORY_BASE = "https://www.caiso.com/outlook/SP/History"

DAY_AHEAD_MARKET_MARKET_RUN_ID = "DAM"
REAL_TIME_DISPATCH_MARKET_RUN_ID = "RTD"


def determine_lmp_frequency(args):

    """if querying all must use 1d frequency"""
    locations = args.get("locations", "")
    market = args.get("market", "")
    # due to limitations of OASIS api
    if isinstance(locations, str) and locations.lower() in ["all", "all_ap_nodes"]:
        if market == Markets.REAL_TIME_5_MIN:
            return "1h"
        elif market == Markets.REAL_TIME_15_MIN:
            return "1h"
        elif market == Markets.DAY_AHEAD_HOURLY:
            return "1D"
        else:
            raise NotSupported(f"Market {market} not supported")
    else:
        return "31D"


def determine_oasis_frequency(args):
    dataset_config = copy.deepcopy(oasis_dataset_config[args["dataset"]])
    # get meta if it exists. and then max_query_frequency if it exists
    meta = dataset_config.get("meta", {})
    max_query_frequency = meta.get("max_query_frequency", None)
    if max_query_frequency is not None:
        return max_query_frequency

    return "31D"


class CAISO(ISOBase):
    """California Independent System Operator (CAISO)"""

    name = "California ISO"
    iso_id = "caiso"
    default_timezone = "US/Pacific"

    status_homepage = "https://www.caiso.com/TodaysOutlook/Pages/default.aspx"
    interconnection_homepage = "https://rimspub.caiso.com/rimsui/logon.do"

    # Markets PRC_INTVL_LMP, PRC_RTPD_LMP, PRC_LMP
    markets = [
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_15_MIN,
        Markets.DAY_AHEAD_HOURLY,
    ]

    trading_hub_locations = [
        "TH_NP15_GEN-APND",
        "TH_SP15_GEN-APND",
        "TH_ZP26_GEN-APND",
    ]

    def _current_day(self):
        # get current date from stats api
        return self.get_status(date="latest").time.date()

    def get_stats(self, verbose=False):
        stats_url = _BASE + "/stats.txt"
        r = self._get_json(stats_url, verbose=verbose)
        return r

    def get_status(self, date="latest", verbose=False) -> str:
        """Get Current Status of the Grid. Only date="latest" is supported

        Known possible values: Normal, Restricted Maintenance Operations, Flex Alert
        """

        if date == "latest":
            # todo is it possible for this to return more than one element?
            r = self.get_stats(verbose=verbose)

            time = pd.to_datetime(r["slotDate"]).tz_localize("US/Pacific")
            # can only store one value for status so we concat them together
            status = ", ".join(r["gridstatus"])
            reserves = r["Current_reserve"]

            return GridStatus(time=time, status=status, reserves=reserves, iso=self)
        else:
            raise NotSupported()

    @support_date_range(frequency="DAY_START")
    def get_fuel_mix(self, date, start=None, end=None, verbose=False):
        """Get fuel mix in 5 minute intervals for a provided day

        Arguments:
            date (datetime.date, str): "latest", "today", or an object
                that can be parsed as a datetime for the day to return data.

            start (datetime.date, str): start of date range to return.
                alias for `date` parameter.
                Only specify one of `date` or `start`.

            end (datetime.date, str): "today" or an object that can be parsed
                as a datetime for the day to return data.
                Only used if requesting a range of dates.

            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with columns - 'Time' and columns \
                for each fuel type.
        """
        if date == "latest":
            mix = self.get_fuel_mix("today", verbose=verbose)
            return mix.tail(1).reset_index(drop=True)

        return self._get_historical_fuel_mix(date, verbose=verbose)

    def _get_historical_fuel_mix(self, date, verbose=False):
        df = _get_historical("fuelsource", date, column="Solar", verbose=verbose)

        # rename some inconsistent columns names to standardize across dates
        df = df.rename(
            columns={
                "Small hydro": "Small Hydro",
                "Natural gas": "Natural Gas",
                "Large hydro": "Large Hydro",
            },
        )

        # when day light savings time switches, there are na rows
        # maybe better way to do this in case there are other cases
        # where there are all na rows
        # ignore Time, Interval Start, Interval End columns
        subset = set(df.columns) - set(["Time", "Interval Start", "Interval End"])
        df = df.dropna(axis=0, how="all", subset=subset)

        return df

    @support_date_range(frequency="DAY_START")
    def get_load(self, date, end=None, verbose=False):
        """Return load at a previous date in 5 minute intervals"""

        if date == "latest":
            return self.get_load("today", verbose=verbose)

        return self._get_historical_load(date, verbose=verbose)

    def _get_historical_load(self, date, verbose=False):
        df = _get_historical("demand", date, column="Current demand", verbose=verbose)

        df = df[["Time", "Interval Start", "Interval End", "Current demand"]]
        df = df.rename(columns={"Current demand": "Load"})
        df = df.dropna(subset=["Load"])
        return df

    @support_date_range(frequency="31D")
    def get_load_forecast(self, date, end=None, sleep=4, verbose=False):
        """Returns load forecast for a previous date in 1 hour intervals

        Arguments:
            date (datetime.date, pd.Timestamp, str): day to return.
                If string, format should be YYYYMMDD e.g 20200623

            sleep (int): number of seconds to sleep before returning to avoid
                hitting rate limit in regular usage. Defaults to 5 seconds.

        """
        current_time = pd.Timestamp.now(tz=self.default_timezone)

        df = self.get_oasis_dataset(
            dataset="demand_forecast",
            start=date,
            end=end,
            raw_data=False,
            verbose=verbose,
            sleep=sleep,
        )

        df = df.rename(
            columns={"MW": "Load Forecast"},
        )

        # returns many areas, we only want one overall iso
        df = df[df["TAC_AREA_NAME"] == "CA ISO-TAC"]

        df = df.sort_values("Time")

        df = self._add_forecast_publish_time(
            df,
            current_time,
            # DAM Hourly Demand Forecast is published at 9:10 AM according to OASIS.
            # ATLAS Reference > Publications > OASIS Publications Schedule
            publish_time_offset_from_day_start=pd.Timedelta(hours=9, minutes=10),
        )

        df = df[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Load Forecast",
            ]
        ]

        return df

    def get_solar_and_wind_forecast_dam(self, date, end=None, verbose=False):
        """Return wind and solar forecast in hourly intervals

        Data at: http://oasis.caiso.com/mrioasis/logon.do  at System Demand >
        Wind and Solar Forecast
        """
        if date == "latest":
            return self.get_solar_and_wind_forecast_dam("today", verbose=verbose)

        current_time = pd.Timestamp.now(tz=self.default_timezone)

        data = self.get_oasis_dataset(
            dataset="wind_and_solar_forecast",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )

        return self._process_solar_and_wind_forecast_dam(
            data,
            current_time,
            # Day-ahead hourly wind and solar forecast is published at 7:00 AM according
            # to OASIS.
            publish_time_offset_from_day_start=pd.Timedelta(hours=7),
        )

    def _process_solar_and_wind_forecast_dam(
        self,
        data,
        current_time,
        publish_time_offset_from_day_start,
    ):
        df = data[
            [
                "Interval Start",
                "Interval End",
                "TRADING_HUB",
                "RENEWABLE_TYPE",
                "MW",
            ]
        ]

        # Totals across all trading hubs for each renewable type at each interval
        totals = (
            df.groupby(
                ["RENEWABLE_TYPE", "Interval Start", "Interval End"],
            )["MW"]
            .sum()
            .reset_index()
        )

        totals["TRADING_HUB"] = "CAISO"

        df = pd.concat([df, totals])

        df = df.pivot_table(
            columns=["RENEWABLE_TYPE"],
            index=["Interval Start", "Interval End", "TRADING_HUB"],
            values="MW",
        ).reset_index()

        df = self._add_forecast_publish_time(
            df,
            current_time,
            # Day-ahead hourly wind and solar forecast is published at 7:00 AM according
            # to OASIS.
            publish_time_offset_from_day_start=publish_time_offset_from_day_start,
        )

        df = utils.move_cols_to_front(
            df.rename(
                columns={
                    "TRADING_HUB": "Location",
                    "Solar": "Solar MW",
                    "Wind": "Wind MW",
                },
            ),
            ["Interval Start", "Interval End", "Publish Time", "Location"],
        )

        df.columns.name = None

        return df.sort_values(
            ["Interval Start", "Publish Time", "Location"],
        ).reset_index(drop=True)

    def _add_forecast_publish_time(
        self,
        data,
        current_time,
        publish_time_offset_from_day_start,
    ):
        """
        Labels forecasts with a publish time using the logic:

        - If tomorrow or further in the future, the publish time is
            * Today's publish time if current time is after the publish time
            * Yesterday's publish time if current time is before the publish time
        - If today or earlier, the publish time is the previous day's publish time

        We assume the forecast was published the day before the forecasted day unless
        the forecast is in the future to avoid having publish times in the future.
        """
        hour_offset = publish_time_offset_from_day_start.components.hours
        minute_offset = publish_time_offset_from_day_start.components.minutes

        # Use replace to avoid DST issues
        todays_publish_time = current_time.normalize().replace(
            hour=hour_offset,
            minute=minute_offset,
        )

        # If the current time is after the publish time, then future forecasts were
        # published today. Otherwise, they were published yesterday.
        if current_time > todays_publish_time:
            future_forecasts_publish_time = todays_publish_time
        else:
            future_forecasts_publish_time = todays_publish_time - pd.Timedelta(days=1)

        # Forecasts tomorrow and later get the future forecasts publish time
        # Forecasts today and earlier get a publish time of the previous day at the
        # publish time offset
        data["Publish Time"] = np.where(
            data["Interval Start"].dt.date > future_forecasts_publish_time.date(),
            future_forecasts_publish_time,
            data["Interval Start"].apply(
                lambda x: x.floor("D").replace(hour=hour_offset, minute=minute_offset),
            )
            # DAM is for the next day
            - pd.Timedelta(days=1),
        )

        return data

    def get_pnodes(self, verbose=False):
        start = utils._handle_date("today")

        df = self.get_oasis_dataset(
            dataset="pnode_map",
            start=start,
            end=start + pd.Timedelta(days=1),
            verbose=verbose,
        )

        df = df.rename(
            columns={
                "APNODE_ID": "Aggregate PNode ID",
                "PNODE_ID": "PNode ID",
            },
        )
        return df

    @lmp_config(
        supports={
            Markets.DAY_AHEAD_HOURLY: ["latest", "today", "historical"],
            Markets.REAL_TIME_15_MIN: ["latest", "today", "historical"],
            Markets.REAL_TIME_5_MIN: ["latest", "today", "historical"],
        },
    )
    @support_date_range(frequency=determine_lmp_frequency)
    def get_lmp(
        self,
        date,
        market: str,
        locations: list = None,
        sleep: int = 5,
        end=None,
        verbose=False,
    ):
        """Get LMP pricing starting at supplied date for a list of locations.

        Arguments:
            date (datetime.date, str): date to return data

            market: market to return from. supports:

            locations (list): list of locations to get data from.
                If no locations are provided, defaults to NP15,
                SP15, and ZP26, which are the trading hub locations.
                USE "ALL_AP_NODES" for all Aggregate Pricing Node.
                Use "ALL" to get all nodes. For a list of locations,
                call ``CAISO.get_pnodes()``

            sleep (int): number of seconds to sleep before returning to
                avoid hitting rate limit in regular usage. Defaults to 5 seconds.

        Returns:
            pandas.DataFrame: A DataFrame of pricing data
        """
        if date == "latest":
            return self._latest_lmp_from_today(market=market, locations=locations)

        if locations is None:
            locations = self.trading_hub_locations

        assert isinstance(locations, list) or locations.lower() in [
            "all",
            "all_ap_nodes",
        ], "locations must be a list, 'ALL_AP_NODES', or 'ALL'"

        if market == Markets.DAY_AHEAD_HOURLY:
            dataset = "lmp_day_ahead_hourly"
            PRICE_COL = "MW"
        elif market == Markets.REAL_TIME_15_MIN:
            dataset = "lmp_real_time_15_min"
            PRICE_COL = "PRC"
        elif market == Markets.REAL_TIME_5_MIN:
            dataset = "lmp_real_time_5_min"
            PRICE_COL = "VALUE"
        else:
            raise RuntimeError("LMP Market is not supported")

        if isinstance(locations, list):
            nodes_str = ",".join(locations)
            params = {
                "node": nodes_str,
            }
        elif locations.lower() == "all":
            params = {
                "grp_type": "ALL",
            }
        elif locations.lower() == "all_ap_nodes":
            params = {
                "grp_type": "ALL_APNODES",
            }

        if (
            end is None
            and market in [Markets.REAL_TIME_15_MIN, Markets.REAL_TIME_5_MIN]
            and not isinstance(locations, list)
            and locations.lower() in ["all", "all_ap_nodes"]
        ):
            warnings.warn(
                "Only 1 hour of data will be returned for real time markets if end is "
                "not specified and all nodes are requested",
                # noqa
            )

        df = self.get_oasis_dataset(
            dataset=dataset,
            start=date,
            end=end,
            params=params,
            sleep=sleep,
            raw_data=False,
            verbose=verbose,
        )

        if df.empty:
            raise NoDataFoundException(
                f"No data found for start date: {date} and end date: {end}",
            )

        df = df.pivot_table(
            index=["Time", "Interval Start", "Interval End", "NODE"],
            columns="LMP_TYPE",
            values=PRICE_COL,
            aggfunc="first",
        )

        df = df.reset_index().rename(
            columns={
                "NODE": "Location",
                "LMP": "LMP",
                "MCE": "Energy",
                "MCC": "Congestion",
                "MCL": "Loss",
            },
        )

        df["Market"] = market.value
        df["Location Type"] = "Node"

        # if -APND in location then "APND" Location Type

        df.loc[
            df["Location"].str.endswith("-APND"),
            "Location Type",
        ] = "AP Node"

        df.loc[
            df["Location"].isin(self.trading_hub_locations),
            "Location Type",
        ] = "Trading Hub"

        # if starts with "DLAP_" then "DLAP" Location Type
        df.loc[
            df["Location"].str.startswith("DLAP_"),
            "Location Type",
        ] = "DLAP"

        df = df[
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

        # data = utils.filter_lmp_locations(df, locations=location_filter)
        data = df

        # clean up pivot name in header
        data.columns.name = None

        return data

    @support_date_range(frequency="DAY_START")
    def get_storage(self, date, verbose=False):
        """Return storage charging or discharging for today in 5 minute intervals

        Negative means charging, positive means discharging

        Arguments:
            date (datetime.date, str): date to return data
        """
        if date == "latest":
            return self._latest_from_today(self.get_storage)

        df = _get_historical("storage", date, column="Total batteries", verbose=verbose)

        rename = {
            "Total batteries": "Supply",
            "Stand-alone batteries": "Stand-alone Batteries",
            "Hybrid batteries": "Hybrid Batteries",
        }

        # need to cast back to int since
        # _get_historical sometimes returns as float
        # because during DST switch there are nans
        # in the data that get dropped
        df[list(rename.keys())] = df[rename.keys()].astype(int)

        df = df.rename(
            columns=rename,
        )
        df = df[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Supply",
                "Stand-alone Batteries",
                "Hybrid Batteries",
            ]
        ]

        return df

    @support_date_range(frequency="31D")
    def get_gas_prices(
        self,
        date,
        end=None,
        fuel_region_id="ALL",
        sleep=4,
        verbose=False,
    ):
        """Return gas prices at a previous date

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            fuel_region_id(str, or list): single fuel region id or list of fuel
                region ids to return data for. Defaults to ALL, which returns
                all fuel regions.

        Returns:
            pandas.DataFrame: A DataFrame of gas prices
        """

        if isinstance(fuel_region_id, list):
            fuel_region_id = ",".join(fuel_region_id)

        df = self.get_oasis_dataset(
            dataset="fuel_prices",
            start=date,
            end=end,
            params={
                "fuel_region_id": fuel_region_id,
            },
            raw_data=False,
            sleep=sleep,
        )

        df = df.rename(
            columns={
                "FUEL_REGION_ID": "Fuel Region Id",
                "PRC": "Price",
            },
        )
        df = (
            df.sort_values("Time")
            .sort_values(
                ["Fuel Region Id", "Time"],
            )
            .reset_index(drop=True)
        )

        df = df[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Fuel Region Id",
                "Price",
            ]
        ]
        return df

    def get_fuel_regions(self, verbose=False):
        """Retrieves the (mostly static) list of fuel regions with associated data.
        This file can be joined to the gas prices on Fuel Region Id"""
        url = "https://www.caiso.com/documents/fuelregion_electricregiondefinitions.xlsx"  # noqa

        log(f"Fetching {url}", verbose=verbose)

        # Only want the "GPI_Fuel_Region" sheet
        return pd.read_excel(url, sheet_name="GPI_Fuel_Region").rename(
            columns={
                "Fuel Region": "Fuel Region Id",
                "Cap & Trade Credit": "Cap and Trade Credit",
            },
        )

    @support_date_range(frequency="31D")
    def get_ghg_allowance(
        self,
        date,
        end=None,
        sleep=4,
        verbose=False,
    ):
        """Return ghg allowance at a previous date

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.
        """

        df = self.get_oasis_dataset(
            dataset="ghg_allowance",
            start=date,
            end=end,
            raw_data=False,
            sleep=sleep,
        )

        df = df.rename(
            columns={
                "GHG_PRC_IDX": "GHG Allowance Price",
            },
        )

        df = df[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "GHG Allowance Price",
            ]
        ]

        return df

    def get_raw_interconnection_queue(self, verbose):
        url = "http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx"

        msg = f"Downloading interconnection queue from {url}"
        log(msg, verbose)
        response = requests.get(url)
        return utils.get_response_blob(response)

    def get_interconnection_queue(self, verbose=False):
        raw_data = self.get_raw_interconnection_queue(verbose)

        sheets = pd.read_excel(raw_data, skiprows=3, sheet_name=None)

        # remove legend at the bottom
        queued_projects = sheets["Grid GenerationQueue"][:-8]
        completed_projects = sheets["Completed Generation Projects"][:-2]
        withdrawn_projects = sheets["Withdrawn Generation Projects"][:-2].rename(
            columns={"Project Name - Confidential": "Project Name"},
        )

        queue = pd.concat(
            [queued_projects, completed_projects, withdrawn_projects],
        )

        queue = queue.rename(
            columns={
                "Interconnection Request\nReceive Date": (
                    "Interconnection Request Receive Date"
                ),
                "Actual\nOn-line Date": "Actual On-line Date",
                "Current\nOn-line Date": "Current On-line Date",
                "Interconnection Agreement \nStatus": (
                    "Interconnection Agreement Status"
                ),
                "Study\nProcess": "Study Process",
                "Proposed\nOn-line Date\n(as filed with IR)": (
                    "Proposed On-line Date (as filed with IR)"
                ),
                "System Impact Study or \nPhase I Cluster Study": (
                    "System Impact Study or Phase I Cluster Study"
                ),
                "Facilities Study (FAS) or \nPhase II Cluster Study": (
                    "Facilities Study (FAS) or Phase II Cluster Study"
                ),
                "Optional Study\n(OS)": "Optional Study (OS)",
            },
        )

        type_columns = ["Type-1", "Type-2", "Type-3"]
        queue["Generation Type"] = queue[type_columns].apply(
            lambda x: " + ".join(x.dropna()),
            axis=1,
        )

        rename = {
            "Queue Position": "Queue ID",
            "Project Name": "Project Name",
            "Generation Type": "Generation Type",
            "Queue Date": "Queue Date",
            "County": "County",
            "State": "State",
            "Application Status": "Status",
            "Current On-line Date": "Proposed Completion Date",
            "Actual On-line Date": "Actual Completion Date",
            "Reason for Withdrawal": "Withdrawal Comment",
            "Withdrawn Date": "Withdrawn Date",
            "Utility": "Transmission Owner",
            "Station or Transmission Line": "Interconnection Location",
            "Net MWs to Grid": "Capacity (MW)",
        }

        extra_columns = [
            "Type-1",
            "Type-2",
            "Type-3",
            "Fuel-1",
            "Fuel-2",
            "Fuel-3",
            "MW-1",
            "MW-2",
            "MW-3",
            "Interconnection Request Receive Date",
            "Interconnection Agreement Status",
            "Study Process",
            "Proposed On-line Date (as filed with IR)",
            "System Impact Study or Phase I Cluster Study",
            "Facilities Study (FAS) or Phase II Cluster Study",
            "Optional Study (OS)",
            "Full Capacity, Partial or Energy Only (FC/P/EO)",
            "Off-Peak Deliverability and Economic Only",
            "Feasibility Study or Supplemental Review",
        ]

        missing = [
            "Interconnecting Entity",
            "Summer Capacity (MW)",
            "Winter Capacity (MW)",
        ]

        queue = utils.format_interconnection_df(
            queue=queue,
            rename=rename,
            extra=extra_columns,
            missing=missing,
        )

        return queue

    @support_date_range(frequency="DAY_START")
    def get_curtailment(self, date, verbose=False):
        """Return curtailment data for a given date

        Notes:
            * requires java to be installed in order to run
            * Data available from June 30, 2016 to present


        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            verbose: print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of curtailment data
        """
        # round to beginning of day
        date = date.normalize()

        # todo handle not always just 4th pge
        date_str = date.strftime("%b-%d-%Y").lower()

        pdf = None
        base_url = "http://www.caiso.com/documents/wind-solar-real-time-dispatch-curtailment-report-"  # noqa

        # Base url and date string format change for dates prior to May 31, 2024
        if date < pd.Timestamp("2024-05-31", tz=date.tzinfo):
            base_url = "https://www.caiso.com/documents/wind_solarreal-timedispatchcurtailmentreport"  # noqa

            date_str = date.strftime("%b%d_%Y").lower()

        # # handle specfic case where dec 02, 2021 has wrong year in file name
        if date_str == "dec02_2021":
            date_str = "02dec_2020"

        url = f"{base_url}{date_str}.pdf"

        msg = f"Fetching URL: {url}"
        log(msg, verbose)

        r = requests.get(url)
        if r.status_code == 404:
            raise ValueError(
                f"Could not find curtailment PDF for {date}",
            )
        pdf = io.BytesIO(r.content)

        if pdf is None:
            raise ValueError(
                "Could not find curtailment PDF for {}".format(date),
            )

        with io.StringIO() as buf, redirect_stderr(buf):
            try:
                tables = tabula.read_pdf(pdf, pages="all")
            except Exception:
                print(buf.getvalue())
                raise RuntimeError("Problem Reading PDF")

        index_curtailment_table = list(
            map(lambda df: "FUEL TYPE" in df.columns, tables),
        ).index(True)
        tables = tables[index_curtailment_table:]
        if len(tables) == 0:
            raise ValueError("No tables found")
        elif len(tables) == 1:
            df = tables[0]
        else:
            # this is case where there was a continuation of the
            # curtailment table
            # on a second page. there is no header,
            # make parsed header of extra table the first row

            def _handle_extra_table(extra_table):
                extra_table = pd.concat(
                    [
                        extra_table.columns.to_frame().T.replace("Unnamed: 0", None),
                        extra_table,
                    ],
                )
                extra_table.columns = tables[0].columns

                return extra_table

            extra_tables = [tables[0]] + [_handle_extra_table(t) for t in tables[1:]]

            df = pd.concat(extra_tables).reset_index()

        rename = {
            "DATE": "Date",
            "HOU\rR": "Hour",
            "HOUR": "Hour",
            "CURT TYPE": "Curtailment Type",
            "REASON": "Curtailment Reason",
            "FUEL TYPE": "Fuel Type",
            "CURTAILED MWH": "Curtailment (MWh)",
            "CURTAILED\rMWH": "Curtailment (MWh)",
            "CURTAILED MW": "Curtailment (MW)",
            "CURTAILED\rMW": "Curtailment (MW)",
        }

        df = df.rename(columns=rename)

        # convert from hour ending to hour beginning
        df["Hour"] = df["Hour"].astype(int) - 1

        df["Time"] = df["Hour"].apply(
            lambda x, date=date: date + pd.Timedelta(hours=x),
        )

        df["Interval Start"] = df["Time"]
        df["Interval End"] = df["Time"] + pd.Timedelta(hours=1)

        df = df.drop(columns=["Date", "Hour"])

        df["Fuel Type"] = df["Fuel Type"].map(
            {
                "SOLR": "Solar",
                "WIND": "Wind",
            },
        )

        df = df[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Curtailment Type",
                "Curtailment Reason",
                "Fuel Type",
                "Curtailment (MWh)",
                "Curtailment (MW)",
            ]
        ]

        return df

    @support_date_range(frequency="DAY_START")
    def get_as_prices(self, date, end=None, market="DAM", sleep=4, verbose=False):
        """Return AS prices for a given date for each region

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            market (str): DAM or HASP. Defaults to DAM.

            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of AS prices
        """

        params = {
            "market_run_id": market,
        }

        df = self.get_oasis_dataset(
            dataset="as_clearing_prices",
            start=date,
            end=end,
            params=params,
            sleep=sleep,
            verbose=verbose,
            raw_data=False,
        )

        df = df.rename(
            columns={
                "ANC_REGION": "Region",
                "MARKET_RUN_ID": "Market",
            },
        )

        as_type_map = {
            "NR": "Non-Spinning Reserves",
            "RD": "Regulation Down",
            "RMD": "Regulation Mileage Down",
            "RMU": "Regulation Mileage Up",
            "RU": "Regulation Up",
            "SR": "Spinning Reserves",
        }
        df["ANC_TYPE"] = df["ANC_TYPE"].map(as_type_map)

        df = df.pivot_table(
            index=[
                "Time",
                "Interval Start",
                "Interval End",
                "Region",
                "Market",
            ],
            columns="ANC_TYPE",
            values="MW",
        ).reset_index()

        df = df.fillna(0)

        df.columns.name = None

        return df

    @support_date_range(frequency="DAY_START")
    def get_curtailed_non_operational_generator_report(
        self,
        date,
        end=None,
        verbose=False,
    ):
        """Return curtailed non-operational generator report for a given date.
            Earliest available date is June 17, 2021.


        Arguments:
            date (datetime.date, str): date to return data
            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of curtailed non-operational generator report

            column glossary:
            http://www.caiso.com/market/Pages/OutageManagement/Curtailed
            -OperationalGeneratorReportGlossary.aspx

            if requesting multiple days, may want to run
            following to remove outages that get reported across multiple days:
            ```df.drop_duplicates(
                subset=["OUTAGE MRID", "CURTAILMENT START DATE TIME"], keep="last")```


        """

        # date must on or be after june 17, 2021
        if date.date() < pd.Timestamp("2021-06-17").date():
            raise ValueError(
                "Date must be on or after June 17, 2021",
            )

        # After May 31, 2024, the date format is %b-%d-%Y.lower() (jun-01-2024)
        date_str = date.strftime("%b-%d-%Y").lower()

        # May 31, 2024 uses a unique format (2024-05-31)
        if date.date() == pd.Timestamp("2024-05-31").date():
            date_str = date.strftime("%Y-%m-%d").lower()
        # Before May 31, 2024, the date format is %Y%m%d (20240530)
        elif date < pd.Timestamp("2024-05-31", tz=date.tzinfo):
            date_str = date.strftime("%Y%m%d")

        url = f"https://www.caiso.com/documents/curtailed-non-operational-generator-prior-trade-date-report-{date_str}.xlsx"  # noqa

        # Jun 1, 2024 has an extra "and" in the url
        if date.date() == pd.Timestamp("2024-06-01").date():
            url = f"https://www.caiso.com/documents/curtailed-and-non-operational-generator-prior-trade-date-report-{date_str}.xlsx"  # noqa

        log(f"Fetching {url}", verbose=verbose)
        # fetch this way to avoid having to
        # make request twice
        content = requests.get(url).content
        content_io = io.BytesIO(content)

        # find index of OUTAGE MRID
        test_parse = pd.read_excel(
            content_io,
            usecols="B:M",
            sheet_name="PREV_DAY_OUTAGES",
        )
        first_col = test_parse[test_parse.columns[0]]
        outage_mrid_index = first_col[first_col == "OUTAGE MRID"].index[0] + 1

        # load again, but skip rows up to outage mrid
        df = pd.read_excel(
            content_io,
            usecols="B:M",
            skiprows=outage_mrid_index,
            sheet_name="PREV_DAY_OUTAGES",
        )

        # drop columns where the name is nan
        # artifact of the excel file
        df = df.dropna(axis=1, how="all")

        # published day after
        publish_time = date.normalize() + pd.DateOffset(days=1)
        df.insert(0, "Publish Time", publish_time)

        df["CURTAILMENT START DATE TIME"] = pd.to_datetime(
            df["CURTAILMENT START DATE TIME"],
        ).dt.tz_localize(self.default_timezone, ambiguous=True)
        df["CURTAILMENT END DATE TIME"] = pd.to_datetime(
            df["CURTAILMENT END DATE TIME"],
        ).dt.tz_localize(self.default_timezone, ambiguous=True)

        # only some dates have this
        if "OUTAGE STATUS" in df.columns:
            df = df.drop(columns=["OUTAGE STATUS"])

        df = df.rename(
            columns={
                "OUTAGE MRID": "Outage MRID",
                "RESOURCE NAME": "Resource Name",
                "RESOURCE ID": "Resource ID",
                "OUTAGE TYPE": "Outage Type",
                "NATURE OF WORK": "Nature of Work",
                "CURTAILMENT START DATE TIME": "Curtailment Start Time",
                "CURTAILMENT END DATE TIME": "Curtailment End Time",
                "CURTAILMENT MW": "Curtailment MW",
                "RESOURCE PMAX MW": "Resource PMAX MW",
                "NET QUALIFYING CAPACITY MW": "Net Qualifying Capacity MW",
            },
        )

        # if there are duplicates, set trce
        if df.duplicated(subset=["Outage MRID", "Curtailment Start Time"]).any():
            # drop where start and end are the same and end time isnt null
            # this appears to fix
            df = df[
                ~(
                    (df["Curtailment Start Time"] == df["Curtailment End Time"])
                    & (df["Curtailment End Time"].notnull())
                )
            ]

            assert not df.duplicated(
                subset=["Outage MRID", "Curtailment Start Time"],
            ).any(), "There are still duplicates"

        return df

    @support_date_range(frequency="DAY_START")
    def get_tie_flows_real_time(self, date, end=None, verbose=False):
        """Return real time tie flow data.

        From OASIS: Energy > Energy Imbalance Market > EIM Transfer by Tie

        Arguments:
            date (datetime.date, str): date to return data
            end (datetime.date, str): last date of range to return data.

        Returns:
            pandas.DataFrame
        """
        if date == "latest":
            date = pd.Timestamp.utcnow().round("5min")
            end = date + pd.Timedelta(minutes=5)

        df = self.get_oasis_dataset(
            dataset="tie_flows_real_time",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )

        return self._process_tie_flows_data(df)

    def _process_tie_flows_data(self, df):
        df = df.drop(
            columns=[
                "Time",
                "DATA_ITEM",
                "OPR_DT",
                "OPR_HR",
                "OPR_INTERVAL",
                "OASIS_REC_STAT",
                "UPD_DATE",
                "UPD_BY",
                "GROUP",
                # Same as FROM_BAA
                "BAA_GRP_ID",
            ],
        ).rename(columns={"MARKET_TYPE": "MARKET", "VALUE": "MW"})

        # Multiply imports by -1 to match convention of imports being negative
        df["MW"] = np.where(df["DIRECTION"] == "I", df["MW"] * -1, df["MW"])

        # Sum MW by Interval Start, TIE_NAME, FROM_BAA, TO_BAA so we can remove
        # the direction column
        df = (
            df.groupby(
                [
                    "Interval Start",
                    "Interval End",
                    "TIE_NAME",
                    "FROM_BAA",
                    "TO_BAA",
                    "MARKET",
                ],
            )["MW"]
            .sum()
            .reset_index()
        )

        df.columns = df.columns.map(
            lambda x: x.title()
            .replace("_", " ")
            .replace("Baa", "BAA")
            .replace("Mw", "MW"),
        )

        # Create an identifier column (separated by hyphens because some of the tie
        # names have underscores in them) to use for indexing
        df["Interface ID"] = df["Tie Name"] + "-" + df["From BAA"] + "-" + df["To BAA"]

        df = utils.move_cols_to_front(
            df,
            [
                "Interval Start",
                "Interval End",
                "Interface ID",
                "Tie Name",
                "From BAA",
                "To BAA",
                "Market",
                "MW",
            ],
        )

        return df.sort_values(
            ["Interval Start", "Interface ID"],
        )

    @support_date_range(frequency=determine_oasis_frequency)
    def get_oasis_dataset(
        self,
        dataset,
        date,
        end=None,
        params=None,
        raw_data=True,
        sleep=5,
        verbose=False,
    ):
        """Return data from OASIS for a given dataset

        Arguments:
            dataset (str): dataset to return data for. See CAISO.list_oasis_datasets
                for supported datasets
            date (datetime.date, str): date to return data
            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.
            params (dict): dictionary of parameters to pass to dataset.
                See CAISO.list_oasis_datasets for supported parameters
            raw_data (bool, optional): return raw data from OASIS. Defaults to True.
            sleep (int, optional): number of seconds to sleep between
                requests. Defaults to 5.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of OASIS data
        """

        # deepcopy to avoid modifying original
        dataset_config = copy.deepcopy(oasis_dataset_config[dataset])

        if params is None:
            params = {}

        for p in params:
            if p not in dataset_config["params"]:
                raise ValueError(
                    f"Parameter {p} not supported for dataset {dataset}",
                )

            # if it's a list, make sure param value is in list
            if (
                isinstance(dataset_config["params"][p], list)
                and params[p] not in dataset_config["params"][p]
            ):
                raise ValueError(
                    f"Parameter {p} not supported for dataset {dataset}",
                )

            dataset_config["params"][p] = params[p]

        # if any dataset_config values are list,
        # take first as default
        for k, v in dataset_config["params"].items():
            if isinstance(v, list):
                dataset_config["params"][k] = v[0]

        # combine kv from queyr and params
        config_flat = {
            **dataset_config["query"],
            **dataset_config["params"],
        }

        # filter out null values
        config_flat = {k: v for k, v in config_flat.items() if v is not None}

        df = _get_oasis(
            config=config_flat,
            start=date,
            end=end,
            raw_data=raw_data,
            verbose=verbose,
            sleep=sleep,
        )

        if df is None:
            if end:
                print(f"No data for {date} to {end}")
            else:
                print(f"No data for {date}")
            return pd.DataFrame()

        return df

    @support_date_range(frequency="31D")
    def get_as_procurement(
        self,
        date,
        end=None,
        market="DAM",
        sleep=4,
        verbose=False,
    ):
        """Get ancillary services procurement data from CAISO.

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            market: DAM or RTM. Defaults to DAM.

        Returns:
            pandas.DataFrame: A DataFrame of ancillary services data
        """
        assert market in ["DAM", "RTM"], "market must be DAM or RTM"

        df = self.get_oasis_dataset(
            dataset="as_results",
            start=date,
            end=end,
            params={
                "market_run_id": market,
            },
            sleep=sleep,
            verbose=verbose,
            raw_data=False,
        )

        df = df.rename(
            columns={
                "ANC_REGION": "Region",
                "MARKET_RUN_ID": "Market",
            },
        )

        as_type_map = {
            "NR": "Non-Spinning Reserves",
            "RD": "Regulation Down",
            "RMD": "Regulation Mileage Down",
            "RMU": "Regulation Mileage Up",
            "RU": "Regulation Up",
            "SR": "Spinning Reserves",
        }
        df["ANC_TYPE"] = df["ANC_TYPE"].map(as_type_map)

        result_type_map = {
            "AS_BUY_MW": "Procured (MW)",
            "AS_SELF_MW": "Self-Provided (MW)",
            "AS_MW": "Total (MW)",
            "AS_COST": "Total Cost",
        }
        df["RESULT_TYPE"] = df["RESULT_TYPE"].map(result_type_map)

        df["column"] = df["ANC_TYPE"] + " " + df["RESULT_TYPE"]

        df = df.pivot_table(
            index=[
                "Time",
                "Interval Start",
                "Interval End",
                "Region",
                "Market",
            ],
            columns="column",
            values="MW",
        ).reset_index()

        df.columns.name = None

        return df

    def list_oasis_datasets(self, dataset=None):
        # pandas dataframe of oasis dataset name
        # param name
        # param default value
        # and param values

        for dataset_name, config in oasis_dataset_config.items():
            if dataset is not None and dataset_name not in dataset:
                continue
            print(colored(f"Dataset: {dataset_name}", "cyan"))
            if len(config["params"]) == 0:
                print("    No parameters")
            else:
                table_data = []
                for k, v in config["params"].items():
                    default = v[0] if isinstance(v, list) else v
                    possible_values = (
                        ", ".join(str(val) for val in v)
                        if isinstance(v, list)
                        else "N/A"
                    )
                    table_data.append([k, default, possible_values])

                print(
                    tabulate(
                        table_data,
                        headers=[
                            "Parameter",
                            "Default",
                            "Possible Values",
                        ],
                        tablefmt="grid",
                    ),
                )

            print("\n")


def _make_timestamp(time_str, today, timezone="US/Pacific"):
    hour, minute = map(int, time_str.split(":"))
    ts = pd.Timestamp(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=hour,
        minute=minute,
    )
    ts = ts.tz_localize(timezone, ambiguous=True)
    return ts


def _get_historical(
    file: str,
    date: str | pd.Timestamp,
    column: str,
    verbose: bool = False,
) -> pd.DataFrame:

    """Get the historical data file from CAISO given a data series name, formats, and returns a pandas dataframe.

    Args:
        file (str): The name of the data we are wanting, which is equivalent to the file to get from CAISO
        date (str | pd.Timestamp): The date of the data to get from CAISO
        column (str): The column to check for the latest value time
        verbose (bool, optional): Whether to print out the URL being fetched, defaults to False

    Returns:
        pd.DataFrame: A pandas dataframe of the data
    """

    if utils.is_today(date, CAISO.default_timezone):
        url: str = f"{_BASE}/{file}.csv"
        latest = True
    else:
        date_str: str = date.strftime("%Y%m%d")
        url: str = f"{_HISTORY_BASE}/{date_str}/{file}.csv"
        latest = False
    msg: str = f"Fetching URL: {url}"
    log(msg, verbose)
    df = pd.read_csv(url)

    # sometimes there are extra rows at the end, so this lets us ignore them
    df = df.dropna(subset=["Time"])

    # drop every column after Time where values
    # are all null. this happens during spring DST
    # change and caiso keeps the non-existent hour
    # but has nulls for all other columns
    df = df.dropna(subset=df.columns[1:], how="all")

    # for the latest data, we want to check if the data is actually from the previous day and update the date accordingly
    if latest:
        latest_file_time = caiso_utils.check_latest_value_time(df, column)
        current_caiso_time = pd.Timestamp.now(tz=CAISO.default_timezone)

        if latest_file_time > current_caiso_time:
            date = date - pd.Timedelta(days=1)

    df["Time"] = df["Time"].apply(
        _make_timestamp,
        today=date,
        timezone=CAISO.default_timezone,
    )

    # sometimes returns midnight, which is technically the next day
    # to be careful, let's check if that is the case before dropping
    if df.iloc[-1]["Time"].hour == 0:
        df = df.iloc[:-1]

    # insert interval start/end columns
    df.insert(1, "Interval Start", df["Time"])

    # be careful if this is ever not 5 minutes
    df.insert(2, "Interval End", df["Time"] + pd.Timedelta(minutes=5))

    return df


def _get_oasis(config, start, end=None, raw_data=False, verbose=False, sleep=5):
    start, end = _caiso_handle_start_end(start, end)
    config = copy.deepcopy(config)
    config["startdatetime"] = start
    config["enddatetime"] = end

    base_url = f"http://oasis.caiso.com/oasisapi/{config.pop('path')}?"

    url = base_url + "&".join(
        [f"{k}={v}" for k, v in config.items()],
    )

    msg = f"Fetching URL: {url}"
    log(msg, verbose)

    retry_num = 0
    while retry_num < 3:
        r = requests.get(url)

        if r.status_code == 200:
            break

        retry_num += 1
        print(f"Failed to get data from CAISO. Error: {r.status_code}")
        print(f"Retrying {retry_num}...")
        time.sleep(sleep)

    # this is when no data is available
    if (
        "Content-Disposition" not in r.headers
        or ".xml.zip;" in r.headers["Content-Disposition"]
        or b".xml" in r.content
    ):
        # avoid rate limiting
        time.sleep(sleep)
        return None

    z = ZipFile(io.BytesIO(r.content))

    # parse and concat all files
    dfs = []
    for f in z.namelist():
        df = pd.read_csv(z.open(f))
        dfs.append(df)

    df = pd.concat(dfs)

    # if col ends in _GMT, then try to parse as UTC
    for col in df.columns:
        if col.endswith("_GMT"):
            df[col] = pd.to_datetime(
                df[col],
                utc=True,
            )

    # handle different column names
    # across different datasets
    start_cols = [
        "INTERVALSTARTTIME_GMT",
        "INTERVAL_START_GMT",
        "STARTTIME_GMT",
        "START_DATE_GMT",
    ]
    end_cols = [
        "INTERVALENDTIME_GMT",
        "INTERVAL_END_GMT",
        "ENDTIME_GMT",
        "END_DATE_GMT",
    ]
    start_col = None
    end_col = None
    for col in start_cols:
        if col in df.columns:
            start_col = col
            df = df.sort_values(by=start_col)
            break
    for col in end_cols:
        if col in df.columns:
            end_col = col
            break

    if not raw_data and start_col in df.columns:
        df[start_col] = df[start_col].dt.tz_convert(
            CAISO.default_timezone,
        )

        df[end_col] = df[end_col].dt.tz_convert(
            CAISO.default_timezone,
        )

        df.rename(
            columns={
                start_col: "Interval Start",
                end_col: "Interval End",
            },
            inplace=True,
        )

        df.insert(0, "Time", df["Interval Start"])

    # avoid rate limiting
    time.sleep(sleep)

    return df


def _caiso_handle_start_end(date, end):
    start = date.tz_convert("UTC")

    if end:
        end = end
        end = end.tz_convert("UTC")
    else:
        end = start + pd.DateOffset(1)

    start = start.strftime("%Y%m%dT%H:%M-0000")
    end = end.strftime("%Y%m%dT%H:%M-0000")

    return start, end


# Transmission Interface Usage Report
oasis_dataset_config = {
    "transmission_interface_usage": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "TRNS_USAGE",
            "version": 1,
        },
        "params": {
            "market_run_id": ["DAM", "HASP", "RRPD"],
            # you can also specify a specific interface
            "ti_id": "ALL",
            "ti_direction": ["ALL", "E", "I"],
        },
    },
    "schedule_by_tie": {
        "query": {
            "path": "GroupZip",
            "resultformat": 6,
            "version": 12,
        },
        "params": {
            "groupid": [
                "RTD_ENE_SCH_BY_TIE_GRP",
                "DAM_ENE_SCH_BY_TIE_GRP",
                "RUC_ENE_SCH_BY_TIE_GRP",
                "RTPD_ENE_SCH_BY_TIE_GRP",
            ],
        },
        "meta": {
            "max_query_frequency": "1d",
        },
    },
    "as_requirements": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "AS_REQ",
            "version": 1,
        },
        "params": {
            "market_run_id": ["DAM", "HASP", "RTM", "2DA"],
            "anc_type": ["ALL", "NR", "RD", "RU", "SR", "RMD", "RMU"],
            "anc_region": [
                "ALL",
                "AS_CAISO",
                "AS_CAISO_EXP",
                "AS_NP26",
                "AS_NP26_EXP",
                "AS_SP26",
                "AS_SP26_EXP",
            ],
        },
    },
    "as_clearing_prices": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_AS",
            "version": 12,
        },
        "params": {
            "market_run_id": ["DAM", "HASP"],
            "anc_type": ["ALL", "NR", "RD", "RMD", "RMU", "RU", "SR"],
            "anc_region": [
                "ALL",
                "AS_CAISO",
                "AS_SP26_EXP",
                "AS_SP26",
                "AS_CAISO_EXP",
                "AS_NP26_EXP",
                "AS_NP26",
            ],
        },
    },
    "fuel_prices": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_FUEL",
            "version": 1,
        },
        "params": {
            "fuel_region_id": "ALL",
        },
    },
    "ghg_allowance": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_GHG_ALLOWANCE",
            "version": 1,
        },
        "params": {},
    },
    "wind_and_solar_forecast": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "SLD_REN_FCST",
            "version": 1,
        },
        "params": {"market_run_id": "DAM"},
    },
    "pnode_map": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "ATL_PNODE_MAP",
            "version": 1,
        },
        "params": {
            "pnode_id": "ALL",
        },
    },
    "lmp_day_ahead_hourly": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_LMP",
            "version": 12,
        },
        "params": {
            "market_run_id": "DAM",
            "node": None,
            "grp_type": [None, "ALL", "ALL_APNODES"],
        },
    },
    "lmp_real_time_5_min": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_INTVL_LMP",
            "version": 3,
        },
        "params": {
            "market_run_id": "RTM",
            "node": None,
            "grp_type": [None, "ALL", "ALL_APNODES"],
        },
    },
    "lmp_real_time_15_min": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_RTPD_LMP",
            "version": 3,
        },
        "params": {
            "market_run_id": "RTPD",
            "node": None,
            "grp_type": [None, "ALL", "ALL_APNODES"],
        },
    },
    "demand_forecast": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "SLD_FCST",
            "version": 1,
        },
        "params": {
            # TODO: support additional markets
            "market_run_id": "DAM",
        },
    },
    "as_results": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "AS_RESULTS",
            "version": 1,
        },
        "params": {
            "market_run_id": ["DAM", "HASP", "RTM"],
            "anc_type": ["ALL", "NR", "RD", "RU", "SR", "RMD", "RMU"],
            "anc_region": [
                "ALL",
                "AS_CAISO",
                "AS_CAISO_EXP",
                "AS_NP26",
                "AS_NP26_EXP",
                "AS_SP26",
                "AS_SP26_EXP",
            ],
        },
    },
    "excess_btm_production": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "ENE_EBTMP_PERF_DATA",
            "version": 11,
        },
        "params": {},
        "meta": {
            "publish_delay": "3 months",
        },
    },
    "public_bids": {
        "query": {
            "path": "GroupZip",
            "resultformat": 6,
            "version": 3,
        },
        "params": {
            "groupid": ["PUB_DAM_GRP", "PUB_RTM_GRP"],
        },
        "meta": {
            "publish_delay": "90 days",
            "max_query_frequency": "1d",
        },
    },
    "tie_flows_real_time": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "ENE_EIM_TRANSFER_TIE",
            "version": 4,
        },
        "params": {
            "baa_grp_id": "ALL",
            "market_run_id": REAL_TIME_DISPATCH_MARKET_RUN_ID,
        },
    },
    "tie_schedule_day_ahead_hourly": {
        "query": {
            "path": "GroupZip",
            "resultformat": 6,
            "version": 12,
        },
        "params": {
            "groupid": ["DAM_ENE_SCH_BY_TIE_GRP"],
            "market_run_id": DAY_AHEAD_MARKET_MARKET_RUN_ID,
        },
    },
}
