import io
import time
from contextlib import redirect_stderr
from zipfile import ZipFile

import pandas as pd
import requests
import tabula

import gridstatus
from gridstatus import utils
from gridstatus.base import FuelMix, GridStatus, ISOBase, Markets, NotSupported
from gridstatus.decorators import support_date_range

_BASE = "https://www.caiso.com/outlook/SP"
_HISTORY_BASE = "https://www.caiso.com/outlook/SP/History"


class CAISO(ISOBase):
    """California Independent System Operator (CAISO)"""

    name = "California ISO"
    iso_id = "caiso"
    default_timezone = "US/Pacific"

    status_homepage = "https://www.caiso.com/TodaysOutlook/Pages/default.aspx"
    interconnection_homepage = "https://rimspub.caiso.com/rimsui/logon.do"

    # Markets PRC_RTPD_LMP, PRC_HASP_LMP, PRC_LMP
    markets = [
        Markets.REAL_TIME_15_MIN,
        Markets.REAL_TIME_HOURLY,
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

    @support_date_range(frequency="1D")
    def get_fuel_mix(self, date, end=None, verbose=False):
        """Get fuel mix in 5 minute intervals for a provided day

        Arguments:
            date (datetime or str): "latest", "today", or an object that can be parsed as a datetime for the day to return data.

            start (datetime or str): start of date range to return. alias for `date` parameter. Only specify one of `date` or `start`.

            end (datetime or str): "today" or an object that can be parsed as a datetime for the day to return data. Only used if requesting a range of dates.

            verbose (bool): print verbose output. Defaults to False.

        Returns:
            pd.Dataframe: dataframe with columns: Time and columns for each fuel type
        """
        if date == "latest":
            mix = self.get_fuel_mix("today", verbose=verbose)
            latest = mix.iloc[-1]
            time = latest.pop("Time")
            mix_dict = latest.to_dict()
            return FuelMix(time=time, mix=mix_dict, iso=self.name)

        return self._get_historical_fuel_mix(date, verbose=verbose)

    def _get_historical_fuel_mix(self, date, verbose=False):

        url = _HISTORY_BASE + "/%s/fuelsource.csv"
        df = _get_historical(url, date, verbose=verbose)

        # rename some inconsistent columns names to standardize across dates
        df = df.rename(
            columns={
                "Small hydro": "Small Hydro",
                "Natural gas": "Natural Gas",
                "Large hydro": "Large Hydro",
            },
        )

        # when day light savings time switches, there are na rows
        df = df.dropna()

        return df

    @support_date_range(frequency="1D")
    def get_load(self, date, end=None, verbose=False):
        """Return load at a previous date in 5 minute intervals"""

        if date == "latest":
            # todo call today
            load_url = _BASE + "/demand.csv"
            df = pd.read_csv(load_url)

            # get last non null row
            data = df[~df["Current demand"].isnull()].iloc[-1]

            return {
                "time": _make_timestamp(data["Time"], self._current_day()),
                "load": data["Current demand"],
            }

        return self._get_historical_load(date, verbose=verbose)

    def _get_historical_load(self, date, verbose=False):
        url = _HISTORY_BASE + "/%s/demand.csv"
        df = _get_historical(url, date, verbose=verbose)
        df = df[["Time", "Current demand"]]
        df = df.rename(columns={"Current demand": "Load"})
        df = df.dropna(subset=["Load"])
        return df

    @support_date_range(frequency="31D")
    def get_load_forecast(self, date, end=None, sleep=4, verbose=False):
        """Returns load forecast for a previous date in 1 hour intervals

        Arguments:
            date(datetime, pd.Timestamp, or str): day to return. if string, format should be YYYYMMDD e.g 20200623
            sleep(int): number of seconds to sleep before returning to avoid hitting rate limit in regular usage. Defaults to 5 seconds.

        """

        start, end = _caiso_handle_start_end(date, end)

        url = (
            "http://oasis.caiso.com/oasisapi/SingleZip?"
            + "resultformat=6&queryname=SLD_FCST&version=1&market_run_id=DAM"
            + f"&startdatetime={start}&enddatetime={end}"
        )

        df = _get_oasis(
            url,
            usecols=["INTERVALSTARTTIME_GMT", "MW", "TAC_AREA_NAME"],
            verbose=verbose,
            sleep=sleep,
        ).rename(
            columns={"INTERVALSTARTTIME_GMT": "Time", "MW": "Load Forecast"},
        )

        # returns many areas, we only want one overall iso
        df = df[df["TAC_AREA_NAME"] == "CA ISO-TAC"]

        df = df.sort_values("Time")

        df["Forecast Time"] = df["Time"].iloc[0]

        df = df[["Forecast Time", "Time", "Load Forecast"]]
        return df

    def get_pnodes(self):
        url = "http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ATL_PNODE_MAP&version=1&startdatetime=20220801T07:00-0000&enddatetime=20220802T07:00-0000&pnode_id=ALL"
        df = pd.read_csv(
            url,
            compression="zip",
            usecols=["APNODE_ID", "PNODE_ID"],
        ).rename(
            columns={
                "APNODE_ID": "Aggregate PNode ID",
                "PNODE_ID": "PNode ID",
            },
        )
        return df

    @support_date_range(frequency="31D")
    def get_lmp(
        self,
        date,
        market: str,
        locations: list = None,
        sleep: int = 5,
        end=None,
        verbose=False,
    ):
        """Get day ahead LMP pricing starting at supplied date for a list of locations.

        Arguments:
            date: date to return data

            market: market to return from. supports:

            locations(list): list of locations to get data from. If no locations are provided, defaults to NP15, SP15, and ZP26, which are the trading hub locations.
            For a list of locations, call CAISO.get_pnodes()

            sleep(int): number of seconds to sleep before returning to avoid hitting rate limit in regular usage. Defaults to 5 seconds.

        Returns
            dataframe of pricing data
        """
        if date == "latest":
            return self._latest_lmp_from_today(market=market, locations=locations)

        if locations is None:
            locations = self.trading_hub_locations

        # todo make sure defaults to local timezone
        start, end = _caiso_handle_start_end(date, end)

        market = Markets(market)
        if market == Markets.DAY_AHEAD_HOURLY:
            query_name = "PRC_LMP"
            market_run_id = "DAM"
            version = 12
            PRICE_COL = "MW"
        elif market == Markets.REAL_TIME_15_MIN:
            query_name = "PRC_RTPD_LMP"
            market_run_id = "RTPD"
            version = 3
            PRICE_COL = "PRC"
        elif market == Markets.REAL_TIME_HOURLY:
            query_name = "PRC_HASP_LMP"
            market_run_id = "HASP"
            version = 3
            PRICE_COL = "MW"
        else:
            raise RuntimeError("LMP Market is not supported")

        nodes_str = ",".join(locations)
        url = f"http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname={query_name}&version={version}&startdatetime={start}&enddatetime={end}&market_run_id={market_run_id}&node={nodes_str}"

        df = _get_oasis(
            url,
            usecols=[
                "INTERVALSTARTTIME_GMT",
                "NODE",
                "LMP_TYPE",
                PRICE_COL,
            ],
            verbose=verbose,
            sleep=sleep,
        )

        df = df.pivot_table(
            index=["INTERVALSTARTTIME_GMT", "NODE"],
            columns="LMP_TYPE",
            values=PRICE_COL,
            aggfunc="first",
        )

        df = df.reset_index().rename(
            columns={
                "INTERVALSTARTTIME_GMT": "Time",
                "NODE": "Location",
                "LMP": "LMP",
                "MCE": "Energy",
                "MCC": "Congestion",
                "MCL": "Loss",
            },
        )

        df["Market"] = market.value
        df["Location Type"] = None

        df.loc[
            df["Location"].isin(self.trading_hub_locations),
            "Location Type",
        ] = "Trading Hub"

        df = df[
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

        data = utils.filter_lmp_locations(df, locations)

        # clean up pivot name in header
        data.columns.name = None

        return df

    @support_date_range(frequency="1D")
    def get_storage(self, date, verbose=False):
        """Return storage charging or discharging for today in 5 minute intervals

        Negative means charging, positive means discharging

        Arguments:
            date: date to return data
        """
        if date == "latest":
            return self._latest_from_today(self.get_storage)

        url = _HISTORY_BASE + "/%s/storage.csv"
        df = _get_historical(url, date, verbose=verbose)
        df = df.rename(columns={"Batteries": "Supply"})
        df["Type"] = "Batteries"
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
            date: date to return data

            end: last date of range to return data. if None, returns only date. Defaults to None.

            fuel_region_id(str, or list): single fuel region id or list of fuel region ids to return data for. Defaults to ALL, which returns all fuel regions.

        Returns:
            dataframe of gas prices
        """

        start, end = _caiso_handle_start_end(date, end)

        if isinstance(fuel_region_id, list):
            fuel_region_id = ",".join(fuel_region_id)

        url = f"http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_FUEL&version=1&FUEL_REGION_ID={fuel_region_id}&startdatetime={start}&enddatetime={end}"

        df = _get_oasis(
            url,
            usecols=[
                "INTERVALSTARTTIME_GMT",
                "FUEL_REGION_ID",
                "PRC",
            ],
            verbose=verbose,
            sleep=sleep,
        ).rename(
            columns={
                "INTERVALSTARTTIME_GMT": "Time",
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
        return df

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
            date: date to return data
            end: last date of range to return data. if None, returns only date. Defaults to None.
        """

        start, end = _caiso_handle_start_end(date, end)

        url = f"http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_GHG_ALLOWANCE&version=1&startdatetime={start}&enddatetime={end}"

        df = _get_oasis(
            url,
            usecols=[
                "INTERVALSTARTTIME_GMT",
                "GHG_PRC_IDX",
            ],
            verbose=verbose,
            sleep=sleep,
        ).rename(
            columns={
                "INTERVALSTARTTIME_GMT": "Time",
                "GHG_PRC_IDX": "GHG Allowance Price",
            },
        )

        return df

    def get_interconnection_queue(self, verbose=False):
        url = "http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx"

        if verbose:
            print("Downloading interconnection queue from {}".format(url))

        sheets = pd.read_excel(url, skiprows=3, sheet_name=None)

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
                "Interconnection Request\nReceive Date": "Interconnection Request Receive Date",
                "Actual\nOn-line Date": "Actual On-line Date",
                "Current\nOn-line Date": "Current On-line Date",
                "Interconnection Agreement \nStatus": "Interconnection Agreement Status",
                "Study\nProcess": "Study Process",
                "Proposed\nOn-line Date\n(as filed with IR)": "Proposed On-line Date (as filed with IR)",
                "System Impact Study or \nPhase I Cluster Study": "System Impact Study or Phase I Cluster Study",
                "Facilities Study (FAS) or \nPhase II Cluster Study": "Facilities Study (FAS) or Phase II Cluster Study",
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

    @support_date_range(frequency="1D")
    def get_curtailment(self, date, verbose=False):
        """Return curtailment data for a given date

        Notes:
            * requires java to be installed in order to run
            * Data available from June 30, 2016 to present


        Arguments:
            date: date to return data
            end: last date of range to return data. if None, returns only date. Defaults to None.
            verbose: print out url being fetched. Defaults to False.

        Returns:
            dataframe of curtailment data
        """

        # http://www.caiso.com/Documents/Wind_SolarReal-TimeDispatchCurtailmentReport02dec_2020.pdf

        date_strs = [
            date.strftime("%b%d_%Y"),
            date.strftime(
                "%d%b_%Y",
            ).lower(),
            date.strftime("-%b%d_%Y"),
        ]

        # handle specfic case where dec 02, 2021 has wrong year in file name
        if date_strs[0] == "Dec02_2021":
            date_strs = ["02dec_2020"]
        if date_strs[0] == "Dec02_2020":
            # this correct, so make sure we don't try the other file since 2021 is published wrong
            date_strs = ["Dec02_2020"]

        # todo handle not always just 4th pge

        pdf = None
        for date_str in date_strs:
            f = f"http://www.caiso.com/Documents/Wind_SolarReal-TimeDispatchCurtailmentReport{date_str}.pdf"
            if verbose:
                print("Fetching URL: ", f)
            r = requests.get(f)
            if b"404 - Page Not Found" in r.content:
                continue
            pdf = io.BytesIO(r.content)
            break

        if pdf is None:
            raise ValueError(
                "Could not find curtailment PDF for {}".format(date),
            )

        with io.StringIO() as buf, redirect_stderr(buf):
            try:
                tables = tabula.read_pdf(pdf, pages="all")
            except:
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
            # this is case where there was a continuation of the curtailment table
            # on a second page. there is no header, make parsed header of extra table the first row

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
                "Curtailment Type",
                "Curtailment Reason",
                "Fuel Type",
                "Curtailment (MWh)",
                "Curtailment (MW)",
            ]
        ]

        return df

    @support_date_range(frequency="1D")
    def get_as_prices(self, date, end=None, sleep=4, verbose=False):
        """Return AS prices for a given date for each region

        Arguments:
            date: date to return data
            end: last date of range to return data. if None, returns only date. Defaults to None.
            verbose: print out url being fetched. Defaults to False.

        Returns:
            dataframe of AS prices
        """

        start, end = _caiso_handle_start_end(date, end)

        url = f"http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_AS&version=12&startdatetime={start}&enddatetime={end}&market_run_id=DAM&anc_type=ALL&anc_region=ALL"

        df = _get_oasis(url=url, verbose=verbose, sleep=sleep).rename(
            columns={
                "INTERVALSTARTTIME_GMT": "Time",
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
            index=["Time", "Region", "Market"],
            columns="ANC_TYPE",
            values="MW",
        ).reset_index()

        df = df.fillna(0)

        df.columns.name = None

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
            date: date to return data
            end: last date of range to return data. if None, returns only date. Defaults to None.
            market: DAM or RTM. Defaults to DAM.

        Returns:
            dataframe of ancillary services data
        """
        assert market in ["DAM", "RTM"], "market must be DAM or RTM"

        start, end = _caiso_handle_start_end(date, end)

        url = f"http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=AS_RESULTS&version=1&startdatetime={start}&enddatetime={end}&market_run_id={market}&anc_type=ALL&anc_region=ALL"

        df = _get_oasis(url=url, verbose=verbose, sleep=sleep).rename(
            columns={
                "INTERVALSTARTTIME_GMT": "Time",
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
            index=["Time", "Region", "Market"],
            columns="column",
            values="MW",
        ).reset_index()

        df.columns.name = None

        return df


def _make_timestamp(time_str, today, timezone="US/Pacific"):
    hour, minute = map(int, time_str.split(":"))
    return pd.Timestamp(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=hour,
        minute=minute,
        tz=timezone,
    )


def _get_historical(url, date, verbose=False):

    date_str = date.strftime("%Y%m%d")
    date_obj = date
    url = url % date_str

    if verbose:
        print("Fetching URL: ", url)

    df = pd.read_csv(url)

    # sometimes there are extra rows at the end, so this lets us ignore them
    df = df.dropna(subset=["Time"])

    df["Time"] = df["Time"].apply(
        _make_timestamp,
        today=date_obj,
        timezone="US/Pacific",
    )

    # sometimes returns midnight, which is technically the next day
    # to be careful, let's check if that is the case before dropping
    if df.iloc[-1]["Time"].hour == 0:
        df = df.iloc[:-1]

    return df


def _get_oasis(url, usecols=None, verbose=False, sleep=4):
    if verbose:
        print(url)

    retry_num = 0
    while retry_num < 3:
        r = requests.get(url)

        if r.status_code == 200:
            break

        retry_num += 1
        print(f"Failed to get data from CAISO. Error: {r.status_code}")
        print(f"Retrying {retry_num}...")
        time.sleep(5)

    z = ZipFile(io.BytesIO(r.content))

    df = pd.read_csv(
        z.open(z.namelist()[0]),
        usecols=usecols,
    )

    if "INTERVALSTARTTIME_GMT" in df.columns:
        df["INTERVALSTARTTIME_GMT"] = pd.to_datetime(
            df["INTERVALSTARTTIME_GMT"],
            utc=True,
        ).dt.tz_convert(CAISO.default_timezone)

    # avoid rate limiting
    time.sleep(5)

    return df


#  get Ancillary Services


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


if __name__ == "__main__":
    import gridstatus

    print("asd")
    iso = gridstatus.CAISO()

    df = iso.get_curtailment(
        start="2016-06-30",
        end="today",
        save_to="caiso_curtailment_2/",
        verbose=True,
    )

    # check if any files are missing
    import glob
    import os

    files = glob.glob("caiso_curtailment/*.csv")
    dates = (
        pd.Series(
            [pd.to_datetime(f[-12:-4]) for f in files],
        )
        .sort_values()
        .to_frame()
        .set_index(0, drop=False)
    )
    diffs = dates.diff()[0].dt.days
    miss = diffs[diffs > 1]
