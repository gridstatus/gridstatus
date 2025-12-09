import urllib.error
from enum import StrEnum
from typing import BinaryIO, Dict, Literal, NamedTuple

import pandas as pd
import requests

import gridstatus
from gridstatus import utils
from gridstatus.base import (
    GridStatus,
    InterconnectionQueueStatus,
    ISOBase,
    Markets,
)
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import logger
from gridstatus.lmp_config import lmp_config


# NYISO offers LMP data at two locational granularities
# load zone and point of generator interconnection
class NYISOLocationType(StrEnum):
    ZONE = "zone"
    GENERATOR = "generator"


REFERENCE_BUS_LOCATION = "NYISO_LBMP_REFERENCE"

LOAD_DATASET = "pal"
FUEL_MIX_DATASET = "rtfuelmix"
LOAD_FORECAST_DATASET = "isolf"
DAM_LMP_DATASET = "damlbmp"
REAL_TIME_LMP_DATASET = "realtime"
REAL_TIME_HOURLY_LMP_DATASET = "rtlbmp"
REAL_TIME_EVENTS_DATASET = "RealTimeEvents"
BTM_SOLAR_ACTUAL_DATASET = "btmactualforecast"
BTM_SOLAR_FORECAST_DATASET = "btmdaforecast"
INTERFACE_LIMITS_AND_FLOWS_DATASET = "ExternalLimitsFlows"
LAKE_ERIE_CIRCULATION_REAL_TIME_DATASET = "eriecirculationrt"
LAKE_ERIE_CIRCULATION_DAY_AHEAD_DATASET = "eriecirculationda"
AS_PRICES_DAY_AHEAD_HOURLY_DATASET = "damasp"
AS_PRICES_REAL_TIME_5_MIN_DATASET = "rtasp"
LIMITING_CONSTRAINTS_REAL_TIME_DATASET = "LimitingConstraints"
LIMITING_CONSTRAINTS_DAY_AHEAD_DATASET = "DAMLimitingConstraints"

"""
Pricing data:
https://www.nyiso.com/en/energy-market-operational-data
"""


class DatasetInterval(NamedTuple):
    time_type: Literal["start", "end", "instantaneous"]
    interval_duration_minutes: int | None


DATASET_INTERVAL_MAP: Dict[str, DatasetInterval] = {
    LOAD_DATASET: DatasetInterval("instantaneous", None),
    FUEL_MIX_DATASET: DatasetInterval("instantaneous", None),
    LOAD_FORECAST_DATASET: DatasetInterval("start", 60),
    DAM_LMP_DATASET: DatasetInterval("start", 60),
    REAL_TIME_LMP_DATASET: DatasetInterval("end", 5),
    REAL_TIME_HOURLY_LMP_DATASET: DatasetInterval("start", 60),
    REAL_TIME_EVENTS_DATASET: DatasetInterval("instantaneous", None),
    BTM_SOLAR_ACTUAL_DATASET: DatasetInterval("start", 60),
    BTM_SOLAR_FORECAST_DATASET: DatasetInterval("start", 60),
    INTERFACE_LIMITS_AND_FLOWS_DATASET: DatasetInterval("start", 5),
    LAKE_ERIE_CIRCULATION_REAL_TIME_DATASET: DatasetInterval("instantaneous", None),
    LAKE_ERIE_CIRCULATION_DAY_AHEAD_DATASET: DatasetInterval("instantaneous", None),
    AS_PRICES_DAY_AHEAD_HOURLY_DATASET: DatasetInterval("start", 60),
    AS_PRICES_REAL_TIME_5_MIN_DATASET: DatasetInterval("start", 5),
    LIMITING_CONSTRAINTS_REAL_TIME_DATASET: DatasetInterval("start", 5),
    LIMITING_CONSTRAINTS_DAY_AHEAD_DATASET: DatasetInterval("start", 60),
}


class NYISO(ISOBase):
    """New York Independent System Operator (NYISO)"""

    name = "New York ISO"
    iso_id = "nyiso"
    default_timezone = "US/Eastern"
    markets = [
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_HOURLY,
        Markets.DAY_AHEAD_HOURLY,
    ]
    status_homepage = "https://www.nyiso.com/system-conditions"
    interconnection_homepage = "https://www.nyiso.com/interconnections"

    def _handle_time(
        self,
        df: pd.DataFrame,
        dataset_name: str,
        groupby: str | None = None,
    ) -> pd.DataFrame:
        time_type, interval_duration_minutes = DATASET_INTERVAL_MAP[dataset_name]

        if "Time Stamp" in df.columns:
            time_stamp_col = "Time Stamp"
        elif "Timestamp" in df.columns:
            time_stamp_col = "Timestamp"

        def time_to_datetime(s: pd.Series, dst: str = "infer") -> pd.Series:
            return pd.to_datetime(s).dt.tz_localize(
                self.default_timezone,
                ambiguous=dst,
            )

        if "Time Zone" in df.columns:
            dst = df["Time Zone"] == "EDT"
            df[time_stamp_col] = time_to_datetime(
                df[time_stamp_col],
                dst,
            )

        elif "Name" in df.columns or groupby:
            groupby = groupby or "Name"
            # once we group by name, the time series for each group is no longer ambiguous
            df[time_stamp_col] = df.groupby(groupby, group_keys=False)[
                time_stamp_col
            ].apply(
                time_to_datetime,
                "infer",
            )
        else:
            df[time_stamp_col] = time_to_datetime(
                df[time_stamp_col],
                "infer",
            )

        df = df.rename(columns={time_stamp_col: "Time"})

        if time_type != "instantaneous":
            interval_duration = pd.Timedelta(minutes=interval_duration_minutes)
            if time_type == "start":
                df["Interval Start"] = df["Time"]
                df["Interval End"] = df["Interval Start"] + interval_duration
            elif time_type == "end":
                df["Interval Start"] = df["Time"] - interval_duration
                df["Interval End"] = df["Time"]
                df["Time"] = df["Interval Start"]

            utils.move_cols_to_front(
                df,
                ["Time", "Interval Start", "Interval End"],
            )

        return df

    @support_date_range(frequency="MONTH_START")
    def get_status(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            latest = self._latest_from_today(self.get_status)
            return GridStatus(
                time=latest["time"],
                status=latest["status"],
                reserves=None,
                iso=self,
                notes=latest["notes"],
            )

        status_df = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name=REAL_TIME_EVENTS_DATASET,
            verbose=verbose,
        )

        status_df = status_df.rename(
            columns={"Message": "Status"},
        )

        def _parse_status(row: pd.Series) -> pd.Series:
            STATE_CHANGE = "**State Change. System now operating in "

            row["Notes"] = None
            if row["Status"] == "Start of day system state is NORMAL":
                row["Notes"] = [row["Status"]]
                row["Status"] = "Normal"
            elif STATE_CHANGE in row["Status"]:
                row["Notes"] = [row["Status"]]

                row["Status"] = row["Status"][
                    row["Status"].index(STATE_CHANGE) + len(STATE_CHANGE) : -len(
                        " state.**",
                    )
                ].capitalize()

            return row

        status_df = status_df.apply(_parse_status, axis=1)
        status_df = status_df[["Time", "Status", "Notes"]]
        return status_df

    @support_date_range(frequency="MONTH_START")
    def get_fuel_mix(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        # note: this is simlar datastructure to pjm

        if date == "latest":
            return (
                self.get_fuel_mix(date="today", verbose=verbose)
                .tail(1)
                .reset_index(drop=True)
            )

        mix_df = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name=FUEL_MIX_DATASET,
            verbose=verbose,
        )

        mix_df = mix_df.pivot_table(
            index=["Time"],
            columns="Fuel Category",
            values="Gen MW",
            aggfunc="first",
        ).reset_index()

        mix_df.columns.name = None

        return mix_df

    @support_date_range(frequency="MONTH_START")
    def get_load(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns load at a previous date in 5 minute intervals for
          each zone and total load

        Parameters:
            date (str): Date to get load for. Can be "latest", "today", or
              a date in the format YYYY-MM-DD
            end (str): End date for date range. Optional.
            verbose (bool): Whether to print verbose output. Optional.

        Returns:
            pandas.DataFrame: Load data for NYISO and each zone

        """
        if date == "latest":
            return self.get_load(date="today", verbose=verbose)

        data = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name=LOAD_DATASET,
            verbose=verbose,
        )

        # pivot table
        df = data.pivot_table(
            index=["Time"],
            columns="Name",
            values="Load",
            aggfunc="first",
        )

        df.insert(0, "Load", df.sum(axis=1))

        df.reset_index(inplace=True)
        # drop NA loads
        # data = data.dropna(subset=["Load"])

        return df

    @support_date_range(frequency="MONTH_START")
    def get_btm_solar(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns estimated BTM solar generation at a previous date in hourly
            intervals for system and each zone.

            Available ~8 hours after the end of the operating day.

        Parameters:
            date (str, pd.Timestamp, datetime.datetime): Date to get load for.
              Can be "latest", "today", or a date
            end (str, pd.Timestamp, datetime.datetime): End date for date range.
                Optional.
            verbose (bool): Whether to print verbose output. Optional.

        Returns:
            pandas.DataFrame: BTM solar data for NYISO system and each zone

        """
        if date == "latest":
            return self.get_load(date="today", verbose=verbose)

        data = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name=BTM_SOLAR_ACTUAL_DATASET,
            filename="BTMEstimatedActual",
            verbose=verbose,
        )

        df = data.pivot_table(
            index=["Time", "Interval Start", "Interval End"],
            columns="Zone Name",
            values="MW Value",
            aggfunc="first",
        )

        # move system to first column
        df.insert(0, "SYSTEM", df.pop("SYSTEM"))

        df = df.reset_index()

        df.columns.name = None

        return df

    @support_date_range(frequency="MONTH_START")
    def get_btm_solar_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            return self.get_load(date="today", verbose=verbose)

        data = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name=BTM_SOLAR_FORECAST_DATASET,
            verbose=verbose,
        )

        df = data.pivot_table(
            index=["Time", "Interval Start", "Interval End"],
            columns="Zone Name",
            values="MW Value",
            aggfunc="first",
        )

        # move system to first column
        df.insert(0, "SYSTEM", df.pop("SYSTEM"))

        df = df.reset_index()

        # Report is published day before the forecast at 7:55 AM in NYISO time
        df.insert(
            3,
            "Publish Time",
            df["Interval Start"].dt.floor("D")
            - pd.Timedelta(days=1, hours=-7, minutes=-55),
        )

        df.columns.name = None

        return df

    @support_date_range(frequency="MONTH_START")
    def get_load_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get load forecast for a date in 1 hour intervals"""
        # todo optimize this to accept a date range
        data = self._download_nyiso_archive(
            date,
            end=end,
            dataset_name=LOAD_FORECAST_DATASET,
            verbose=verbose,
        )

        data = data[
            ["Time", "Interval Start", "Interval End", "File Date", "NYISO"]
        ].rename(
            columns={
                "File Date": "Forecast Time",
                "NYISO": "Load Forecast",
                "Time": "Time",
            },
        )

        return data

    @support_date_range(frequency="MONTH_START")
    def get_zonal_load_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get zonal load forecast for a date in 1 hour intervals"""
        data = self._download_nyiso_archive(
            date,
            end=end,
            dataset_name=LOAD_FORECAST_DATASET,
            verbose=verbose,
        )

        data = data[
            [
                "Interval Start",
                "Interval End",
                "File Date",
                "NYISO",
                "Capitl",
                "Centrl",
                "Dunwod",
                "Genese",
                "Hud Vl",
                "Longil",
                "Mhk Vl",
                "Millwd",
                "N.Y.C.",
                "North",
                "West",
            ]
        ].rename(
            columns={
                "File Date": "Publish Time",
            },
        )

        return data

    @support_date_range(frequency="MONTH_START")
    def get_interface_limits_and_flows_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get interface limits and flows for a date"""
        if date == "latest":
            data = pd.read_csv(
                "https://mis.nyiso.com/public/csv/ExternalLimitsFlows/currentExternalLimitsFlows.csv",  # noqa
            )
            data = self._handle_time(
                data,
                INTERFACE_LIMITS_AND_FLOWS_DATASET,
                groupby="Interface Name",
            )
        else:
            data = self._download_nyiso_archive(
                date,
                end=end,
                dataset_name=INTERFACE_LIMITS_AND_FLOWS_DATASET,
                groupby="Interface Name",
                verbose=verbose,
            )

        # The source has these values as MWH but they are actually MW
        data = data.rename(
            columns={
                "Flow (MWH)": "Flow MW",
                "Positive Limit (MWH)": "Positive Limit MW",
                "Negative Limit (MWH)": "Negative Limit MW",
            },
        )

        data = data[
            [
                "Interval Start",
                "Interval End",
                "Interface Name",
                "Point ID",
                "Flow MW",
                "Positive Limit MW",
                "Negative Limit MW",
            ]
        ].sort_values(["Interval Start", "Interface Name"])

        return data

    @support_date_range(frequency="MONTH_START")
    def get_lake_erie_circulation_real_time(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        # No latest file available
        if date == "latest":
            return self.get_lake_erie_circulation_real_time(
                date="today",
                verbose=verbose,
            )

        data = self._download_nyiso_archive(
            date,
            end=end,
            dataset_name=LAKE_ERIE_CIRCULATION_REAL_TIME_DATASET,
            filename="ErieCirculationRT",
            verbose=verbose,
        )

        # The source has MWH in the column name but it's actually MW
        data = data.rename(columns={"Lake Erie Circulation (MWH)": "MW"})

        data = data[["Time", "MW"]].sort_values("Time")

        return data

    @support_date_range(frequency="MONTH_START")
    def get_lake_erie_circulation_day_ahead(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        # No latest file available
        if date == "latest":
            return self.get_lake_erie_circulation_day_ahead(
                date="today",
                verbose=verbose,
            )

        data = self._download_nyiso_archive(
            date,
            end=end,
            dataset_name=LAKE_ERIE_CIRCULATION_DAY_AHEAD_DATASET,
            filename="ErieCirculationDA",
            verbose=verbose,
        )

        data = data.rename(columns={"Lake Erie Circulation (MWH)": "MW"})

        data = data[["Time", "MW"]].sort_values("Time")

        return data

    @lmp_config(
        supports={
            Markets.REAL_TIME_5_MIN: ["latest", "today", "historical"],
            # TODO: add historical RTC data.
            # https://www.nyiso.com/custom-reports?report=ham_lbmp_gen
            Markets.REAL_TIME_15_MIN: ["latest", "today"],
            Markets.REAL_TIME_HOURLY: ["latest", "today", "historical"],
            Markets.DAY_AHEAD_HOURLY: ["latest", "today", "historical"],
        },
    )
    @support_date_range(frequency="MONTH_START")
    def get_lmp(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        market: Markets | None = None,
        locations: list | None = None,
        location_type: NYISOLocationType | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Supported Markets:
            - ``REAL_TIME_5_MIN`` (RTC)
            - ``REAL_TIME_15_MIN`` (RTD)
            - ``REAL_TIME_HOURLY`` (Real-time hourly LMP)
            - ``DAY_AHEAD_HOURLY``

        Supported Location Types:
            - ``zone``
            - ``generator``

        NOTE: the generator data contains the single Reference Bus location type.

        REAL_TIME_5_MIN is the Real Time Dispatch (RTD) market.
        REAL_TIME_15_MIN is the Real Time Commitment (RTC) market.
        REAL_TIME_HOURLY is the real-time hourly LMP market.
        For documentation on real time dispatch and real time commitment, see:
        https://www.nyiso.com/documents/20142/1404816/RTC-RTD%20Convergence%20Study.pdf/f3843982-dd30-4c66-6c21-e101c3cb85af
        """
        if location_type is None:
            location_type = NYISOLocationType.ZONE

        marketname = self._set_marketname(market)
        file_location_type = self._set_location_type_for_filename(location_type)

        if locations is None:
            locations = "ALL"

        if date == "latest":
            # The 5 minute data has a dedicated latest interval file
            if market != Markets.REAL_TIME_5_MIN:
                return self._latest_lmp_from_today(
                    market=market,
                    locations=locations,
                    location_type=location_type,
                    verbose=verbose,
                )
            else:
                url = f"https://mis.nyiso.com/public/realtime/realtime_{file_location_type}_lbmp.csv"
                df = pd.read_csv(url)
                df = self._handle_time(df, dataset_name=marketname)
                df["Market"] = market.value
        else:
            filename = marketname + f"_{file_location_type}"

            df = self._download_nyiso_archive(
                date=date,
                end=end,
                dataset_name=marketname,
                filename=filename,
                verbose=verbose,
            )

        return self._process_lmp_data(df, date, market, location_type, locations)

    def _process_lmp_data(
        self,
        df: pd.DataFrame,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None,
        market: Markets,
        location_type: NYISOLocationType,
        locations: list | str,
    ) -> pd.DataFrame:
        columns = {
            "Name": "Location",
            "LBMP ($/MWHr)": "LMP",
            "Marginal Cost Losses ($/MWHr)": "Loss",
            "Marginal Cost Congestion ($/MWHr)": "Congestion",
            "Marginal Cost Congestion ($/MWH": "Congestion",  # Deal with older data
        }

        df = df.rename(columns=columns)

        # In NYISO raw data, a negative congestion number means a higher LMP. We
        # flip the sign to make it consistent with other ISOs where a negative
        # congestion number means a lower LMP. Thus, LMP = Energy + Loss + Congestion
        # for NYISO, as in other ISOs.
        df["Congestion"] *= -1
        df["Energy"] = (df["LMP"] - df["Loss"] - df["Congestion"]).round(2)
        df["Market"] = market.value
        df["Location Type"] = NYISOLocationType(location_type).value.capitalize()

        # We manually update the location type for the reference bus location because
        # it's included in the generator data.
        if REFERENCE_BUS_LOCATION in df["Location"].unique():
            df.loc[
                df["Location"] == REFERENCE_BUS_LOCATION,
                "Location Type",
            ] = "Reference Bus"

        # NYISO includes both 5 min (RTD - Real Time Dispatch) and
        # 15 min (RTC - Real Time Commitment) data in the daily file
        if market == Markets.REAL_TIME_15_MIN or (
            # In this case, we've only used the latest 5 minute interval file so we
            # know the data is all REAL_TIME_5_MIN
            market == Markets.REAL_TIME_5_MIN and date != "latest"
        ):
            # The most recent 5 min file is sometimes published before the updated
            # daily file, so to label the 5 and 15 min daily data, we need to get
            # the most recent 5 min data and compare it to the data at the same
            # timestamp in the daily dataframe. If the data matches, then we know all
            # daily data after that timestamp is 15 minute. If the data doesn't match,
            # we assume all daily data on or after that timestamp is 15 min data.
            most_recent_5_min_data = self.get_lmp(
                date="latest",
                market=Markets.REAL_TIME_5_MIN,
                location_type=location_type,
            )

            most_recent_5_min_timestamp = most_recent_5_min_data["Interval Start"].max()

            daily_subset = (
                df[df["Interval Start"] == most_recent_5_min_timestamp]
                .sort_values(["Interval Start", "Location"])
                .reset_index(drop=True)
            )

            if daily_subset[["LMP", "Loss", "Congestion"]].equals(
                most_recent_5_min_data[["LMP", "Loss", "Congestion"]],
            ):
                # When equal, the daily data has the most recent 5 min data
                mask_15_min = df["Interval Start"] > most_recent_5_min_timestamp
            else:
                # When not equal, the data at this timestamp is 15 min data
                mask_15_min = df["Interval Start"] >= most_recent_5_min_timestamp

            df.loc[~mask_15_min, "Market"] = Markets.REAL_TIME_5_MIN.value
            df.loc[mask_15_min, "Market"] = Markets.REAL_TIME_15_MIN.value

            # For 15-min data, the original "Interval End" column contains the correct
            # end time (since the raw data has timestamps as interval END). However,
            # "Interval Start" was calculated assuming a 5-minute interval, so we need
            # to recalculate it for 15-minute intervals.
            # Interval Start = Interval End - 15 minutes
            df.loc[mask_15_min, "Interval Start"] = df.loc[
                mask_15_min,
                "Interval End",
            ] - pd.Timedelta(
                minutes=15,
            )

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

        df = utils.filter_lmp_locations(df, locations)

        return (
            df[df["Market"] == market.value]
            .sort_values(["Interval Start", "Location"])
            .reset_index(drop=True)
        )

    def get_raw_interconnection_queue(self) -> BinaryIO:
        url = "https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx"  # noqa

        logger.info(f"Downloading interconnection queue from {url}")
        response = requests.get(url)
        return utils.get_response_blob(response)

    def get_interconnection_queue(self) -> pd.DataFrame:
        """Return NYISO interconnection queue

        Additional Non-NYISO queue info: https://www3.dps.ny.gov/W/PSCWeb.nsf/All/286D2C179E9A5A8385257FBF003F1F7E?OpenDocument

        Returns:
            pandas.DataFrame: Interconnection queue containing, active, withdrawn, \
                and completed project

        """  # noqa

        # 5 sheets - ['Interconnection Queue', 'Cluster Projects', 'Withdrawn', 'Cluster Projects-Withdrawn', 'In Service']
        # harded coded for now. perhaps this url can be parsed from the html here:
        raw_data = self.get_raw_interconnection_queue()

        # Create ExcelFile so we only need to download file once
        excel_file = pd.ExcelFile(raw_data)

        # Drop extra rows at bottom
        active = (
            pd.read_excel(excel_file, sheet_name="Interconnection Queue")
            .dropna(
                subset=["Queue Pos.", "Project Name"],
            )
            .copy()
            # Active projects can have multiple values for "Points of Interconnection"
        ).rename(columns={"Points of Interconnection": "Interconnection Point"})

        cluster_active = (
            pd.read_excel(excel_file, sheet_name=" Cluster Projects")
            .dropna(
                subset=["Queue Pos.", "Project Name"],
            )
            .copy()
            # Active projects can have multiple values for "Points of Interconnection"
        ).rename(columns={"Points of Interconnection": "Interconnection Point"})

        active = pd.concat([active, cluster_active])

        active["Status"] = InterconnectionQueueStatus.ACTIVE.value

        withdrawn = pd.read_excel(excel_file, sheet_name="Withdrawn")
        cluster_withdrawn = pd.read_excel(
            excel_file,
            sheet_name="Cluster Projects-Withdrawn",
        )
        withdrawn = pd.concat([withdrawn, cluster_withdrawn])

        withdrawn["Status"] = InterconnectionQueueStatus.WITHDRAWN.value
        # assume it was withdrawn when last updated
        withdrawn["Withdrawn Date"] = withdrawn["Last Update"]
        withdrawn["Withdrawal Comment"] = None
        withdrawn = withdrawn.rename(columns={"Utility ": "Utility"})

        withdrawn = withdrawn.rename(columns={"Owner/Developer": "Developer Name"})

        # make completed look like the other two sheets
        completed = pd.read_excel(excel_file, sheet_name="In Service", header=[0, 1])
        completed.insert(15, "SGIA Tender Date", None)
        completed.insert(16, "CY Complete Date", None)
        completed.insert(17, "Proposed Initial-Sync Date", None)

        completed["Status"] = InterconnectionQueueStatus.COMPLETED.value

        if (
            "SGIA Tender Date" in active.columns
            and "SGIA Tender Date" not in completed.columns
        ):
            active = active.drop(columns=["SGIA Tender Date"])
        completed_colnames_map = {
            ("Queue", "Pos."): "Queue Pos.",
            ("Queue", "Owner/Developer"): "Developer Name",
            ("Queue", "Project Name"): "Project Name",
            ("Date", "of IR"): "Date of IR",
            ("SP", "(MW)"): "SP (MW)",
            ("WP", "(MW)"): "WP (MW)",
            ("Type/", "Fuel"): "Type/ Fuel",
            ("Location", "County"): "County",
            ("Location", "State"): "State",
            ("Z", "Unnamed: 9_level_1"): "Z",
            ("Interconnection", "Point"): "Interconnection Point",
            ("Interconnection", "Utility "): "Utility",
            ("Interconnection", "S"): "S",
            ("Last Update", "Unnamed: 13_level_1"): "Last Updated Date",
            ("Availability", "of Studies"): "Availability of Studies",
            ("SGIA Tender Date", ""): "SGIA Tender Date",
            ("CY Complete Date", ""): "CY Complete Date",
            ("Proposed Initial-Sync Date", ""): "Proposed Initial-Sync Date",
            ("Proposed", " In-Service"): "Proposed In-Service Date",
            ("Proposed", "COD"): "Proposed COD",
            ("Proposed", "COD.1"): "Proposed COD.1",
            ("Proposed", "COD.2"): "Proposed COD.2",
            ("Proposed", "COD.3"): "Proposed COD.3",
            ("Status", ""): "Status",
        }
        completed.columns = completed.columns.to_flat_index().map(
            lambda c: completed_colnames_map[c],
        )

        # assume it was finished when last updated
        completed["Actual Completion Date"] = completed["Last Updated Date"]

        dfs = [
            df
            for df in [active, withdrawn, completed]
            if not df.empty and not df.isna().all().all()
        ]
        queue = pd.concat(dfs)

        # fix extra space in column name

        queue["Type/ Fuel"] = queue["Type/ Fuel"].map(
            {
                "S": "Solar",
                "ES": "Energy Storage",
                "W": "Wind",
                "AC": "AC Transmission",
                "DC": "DC Transmission",
                "CT": "Combustion Turbine",
                "CC": "Combined Cycle",
                "M": "Methane",
                #    "CR": "",
                "H": "Hydro",
                "L": "Load",
                "ST": "Steam Turbine",
                "CC-NG": "Natural Gas",
                "FC": "Fuel Cell",
                "PS": "Pumped Storage",
                "NU": "Nuclear",
                "D": "Dual Fuel",
                #    "C": "",
                "NG": "Natural Gas",
                "Wo": "Wood",
                "F": "Flywheel",
                #    "CW": "",
                "CC-D": "Combined Cycle - Dual Fuel",
                "SW": "=Solid Waste",
                #    "CR=CSR - ES + Solar": "",
                "CT-NG": "Combustion Turbine - Natural Gas",
                "DC/AC": "DC/AC Transmission",
                "CT-D": "Combustion Turbine - Dual Fuel",
                "CS-NG": "Steam Turbine & Combustion Turbine-  Natural Gas",
                "ST-NG": "Steam Turbine - Natural Gas",
            },
        )

        queue["Capacity (MW)"] = (
            queue[["SP (MW)", "WP (MW)"]]
            .replace(
                "TBD",
                0,
            )
            .replace(" ", 0)
            .fillna(0)
            .astype(float)
            .max(axis=1)
        )

        queue["Date of IR"] = pd.to_datetime(queue["Date of IR"])
        queue["Proposed COD"] = pd.to_datetime(
            queue["Proposed COD"],
            errors="coerce",
        )
        queue["Proposed In-Service Date"] = pd.to_datetime(
            queue["Proposed In-Service Date"],
            errors="coerce",
        )
        queue["Proposed Initial-Sync Date"] = pd.to_datetime(
            queue["Proposed Initial-Sync Date"],
            errors="coerce",
        )

        # TODO handle other 2 sheets
        # TODO they publish past queues,
        # but not sure what data is in them that is relevant

        rename = {
            "Queue Pos.": "Queue ID",
            "Project Name": "Project Name",
            "County": "County",
            "State": "State",
            "Developer Name": "Interconnecting Entity",
            "Utility": "Transmission Owner",
            "Interconnection Point": "Interconnection Location",
            "Status": "Status",
            "Date of IR": "Queue Date",
            "Proposed COD": "Proposed Completion Date",
            "Type/ Fuel": "Generation Type",
            "Capacity (MW)": "Capacity (MW)",
            "SP (MW)": "Summer Capacity (MW)",
            "WP (MW)": "Winter Capacity (MW)",
        }

        extra_columns = [
            "Proposed In-Service Date",
            "Proposed Initial-Sync Date",
            "Last Updated Date",
            "Z",
            "S",
            "Availability of Studies",
            "SGIA Tender Date",
        ]

        queue = utils.format_interconnection_df(queue, rename, extra_columns)

        return queue

    def get_generators(self, verbose: bool = False) -> pd.DataFrame:
        """Get a list of generators in NYISO. When possible return capacity and fuel type information

        Returns:
            pandas.DataFrame: a DataFrame of generators and locations

            **Possible Columns**

            * Generator Name
            * PTID
            * Subzone
            * Zone
            * Latitude
            * Longitude
            * Owner, Operator, and / or Billing Organization
            * Station Unit
            * Town
            * County
            * State
            * In-Service Date
            * Name Plate Rating (V) MW
            * 2024 CRIS MW Summer
            * 2024 CRIS MW Winter
            * 2024 Capability MW Summer
            * 2024 Capability MW Winter
            * Is Dual Fuel
            * Unit Type
            * Fuel Type 1
            * Fuel Type 2
            * 2023 Net Energy GWh
            * Notes
            * Generator Type
        """
        generator_url = "http://mis.nyiso.com/public/csv/generator/generator.csv"

        logger.info(f"Requesting {generator_url}")

        df = pd.read_csv(generator_url)

        # need to be updated once a year. approximately around end of april
        # find it here: https://www.nyiso.com/gold-book-resources
        capacity_url_2024 = "https://www.nyiso.com/documents/20142/44474211/2024-NYCA-Generators.xlsx/41a5cba2-523a-9fe0-9830-a523839a2831"  # noqa

        logger.info(f"Requesting {capacity_url_2024}")

        generators = pd.read_excel(
            capacity_url_2024,
            sheet_name=[
                "Table III-2a",
                "Table III-2b",
            ],
            skiprows=3,
            header=[0, 1, 2, 3, 4],
        )

        generators["Table III-2a"]["Generator Type"] = "Market Generator"
        generators["Table III-2b"]["Generator Type"] = "Non-Market Generator"

        # manually transcribed column names (inspect spreadsheet for confirmation)
        mapped_columns = [
            "LINE REF. NO.",
            "Owner, Operator, and / or Billing Organization",
            "Station Unit",
            "Zone",
            "PTID",
            "Town",
            "County",
            "State",
            "In-Service Date",
            "Name Plate Rating (V) MW",
            "2024 CRIS MW Summer",
            "2024 CRIS MW Winter",
            "2024 Capability MW Summer",
            "2024 Capability MW Winter",
            "Is Dual Fuel",
            "Unit Type",
            "Fuel Type 1",
            "Fuel Type 2",
            "2023 Net Energy GWh",
            "Notes",
            "Generator Type",
        ]

        # Rename the columns separately, so they match on the concat
        generators["Table III-2a"].columns = mapped_columns
        generators["Table III-2b"].columns = mapped_columns

        # combine both sheets
        generators = pd.concat(generators.values())

        generators = generators.dropna(subset=["PTID"])

        generators["PTID"] = generators["PTID"].astype(int)

        # in other data
        generators = generators.drop(columns=["Zone", "LINE REF. NO."])

        # TODO: df has both Generator PTID and Aggregation PTID
        combined = pd.merge(
            df,
            generators,
            left_on="Generator PTID",
            right_on="PTID",
            how="left",
        )

        unit_type_map = {
            "CC": "Combined Cycle",
            "CG": "Cogeneration",
            "CT": "Combustion Turbine Portion (CC)",
            "CW": "Waste Heat Only (CC)",
            "ES": "Energy Storage",
            "FC": "Fuel Cell",
            "GT": "Combustion Turbine",
            "HY": "Conventional Hydro",
            "IC": "Internal Combustion",
            "JE": "Jet Engine",
            "NB": "Steam (BWR Nuclear)",
            "NP": "Steam (PWR Nuclear)",
            "PS": "Pumped Storage Hydro",
            "PV": "Photovoltaic",
            "ST": "Steam Turbine (Fossil)",
            "WT": "Wind Turbine",
        }
        combined["Unit Type"] = combined["Unit Type"].map(unit_type_map)

        fuel_type_map = {
            "BAT": "Battery",
            "BUT": "Butane",
            "FO2": "No. 2 Fuel Oil",
            "FO4": "No. 4 Fuel Oil",
            "FO6": "No. 6 Fuel Oil",
            "FW": "Fly Wheel",
            "JF": "Jet Fuel",
            "KER": "Kerosene",
            "MTE": "Methane (Bio Gas)",
            "NG": "Natural Gas",
            "OT": "Other (Describe In Footnote)",
            "REF": "Refuse (Solid Waste)",
            "SUN": "Sunlight",
            "UR": "Uranium",
            "WAT": "Water",
            "WD": "Wood and/or Wood Waste",
            "WND": "Wind",
        }
        combined["Fuel Type 1"] = combined["Fuel Type 1"].map(
            fuel_type_map,
        )
        combined["Fuel Type 2"] = combined["Fuel Type 2"].map(
            fuel_type_map,
        )

        combined["Is Dual Fuel"] = combined["Is Dual Fuel"] == "YES"

        state_code_map = {
            36: "New York",
            42: "Pennsylvania",
            25: "Massachusetts",
            34: "New Jersey",
        }
        combined["State"] = combined["State"].map(state_code_map)

        # todo map county codes to names. info on first sheet of excel

        return combined

    def get_loads(self) -> pd.DataFrame:
        """Get a list of loads in NYISO

        Returns:
            pandas.DataFrame: a DataFrame of loads and locations
        """

        url = "http://mis.nyiso.com/public/csv/load/load.csv"

        logger.info(f"Requesting {url}")

        df = pd.read_csv(url)

        return df

    def _set_marketname(self, market: Markets) -> str:
        if market in [Markets.REAL_TIME_5_MIN, Markets.REAL_TIME_15_MIN]:
            marketname = REAL_TIME_LMP_DATASET
        elif market == Markets.REAL_TIME_HOURLY:
            marketname = REAL_TIME_HOURLY_LMP_DATASET
        elif market == Markets.DAY_AHEAD_HOURLY:
            marketname = DAM_LMP_DATASET
        else:
            raise RuntimeError(f"LMP Market {market} is not supported")
        return marketname

    def _set_location_type_for_filename(self, location_type: NYISOLocationType) -> str:
        location_types = [NYISOLocationType.ZONE, NYISOLocationType.GENERATOR]
        if location_type == NYISOLocationType.ZONE:
            return NYISOLocationType.ZONE
        elif location_type == NYISOLocationType.GENERATOR:
            return "gen"
        else:
            raise ValueError(
                f"Invalid location type. Expected one of: {location_types}",
            )

    def _download_nyiso_archive(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        dataset_name: str | None = None,
        filename: str | None = None,
        groupby: str | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Download a dataset from NYISO's archive

        Arguments:
            date (str or datetime): the date to download.
                if end is provided, this is the start date
            end (str or datetime):
                the end date to download. if provided, date is the start date
            dataset_name (str):
                the name of the dataset to download
            filename (str): the name of the file to download.
                if not provided, dataset_name is used
            groupby (str): the column to group by when converting datetimes. Used
                to avoid ambiguous datetimes when dst ends
            verbose (bool): print out requested url

        Returns:
            pandas.DataFrame: the downloaded data

        """
        if filename is None:
            filename = dataset_name

        # NB: need to add the file date to the load forecast dataset to get the
        # forecast publish time.
        add_file_date = LOAD_FORECAST_DATASET == dataset_name

        date = gridstatus.utils._handle_date(date, self.default_timezone)
        month = date.strftime("%Y%m01")
        day = date.strftime("%Y%m%d")

        # NB: if requesting the same day then just download the single file
        if end is not None and date.normalize() == end.normalize():
            end = None
            date = date.normalize()

        # NB: the last 7 days of file are hosted directly as csv
        # todo this can probably be optimized to a single csv in
        # a range and all files are in the last 7 days
        if end is None and date > pd.Timestamp.now(
            tz=self.default_timezone,
        ).normalize() - pd.DateOffset(days=7):
            csv_filename = f"{day}{filename}.csv"
            csv_url = f"http://mis.nyiso.com/public/csv/{dataset_name}/{csv_filename}"
            logger.info(f"Requesting {csv_url}")

            df = pd.read_csv(csv_url)
            df = self._handle_time(df, dataset_name, groupby=groupby)
            if add_file_date:
                df["File Date"] = self._get_load_forecast_file_date(date, verbose)
        else:
            zip_url = f"http://mis.nyiso.com/public/csv/{dataset_name}/{month}{filename}_csv.zip"  # noqa: E501
            z = utils.get_zip_folder(zip_url, verbose=verbose)

            all_dfs = []
            if end is None:
                date_range = [date]
            else:
                date_range = pd.date_range(
                    date.date(),
                    end.date(),
                    freq="1D",
                    inclusive="left",
                ).tolist()

                # NB: this handles case where end is the first of the next month
                # this pops up from the support_date_range decorator
                # and that date will be handled in the next month's zip file
                if end.month == date.month:
                    date_range += [end]

            for d in date_range:
                d = gridstatus.utils._handle_date(d, tz=self.default_timezone)
                month = d.strftime("%Y%m01")
                day = d.strftime("%Y%m%d")

                csv_filename = f"{day}{filename}.csv"
                if csv_filename not in z.namelist():
                    logger.info(f"{csv_filename} not found in {zip_url}")
                    continue
                df = pd.read_csv(z.open(csv_filename))

                if add_file_date:
                    # NB: The File Date is the last modified time of the individual csv file
                    df["File Date"] = pd.Timestamp(
                        *z.getinfo(csv_filename).date_time,
                        tz=self.default_timezone,
                    )
                df = self._handle_time(df, dataset_name, groupby=groupby)
                all_dfs.append(df)

            df = pd.concat(all_dfs)

        return df.sort_values("Time").reset_index(drop=True)

    def _get_load_forecast_file_date(
        self,
        date: pd.Timestamp,
        verbose: bool = False,
    ) -> pd.Timestamp:
        """Retrieves the last updated time for load forecast file from the archive"""
        data = pd.read_html(
            "http://mis.nyiso.com/public/P-7list.htm",
            skiprows=2,
            header=0,
        )[0]

        last_updated_date = data.loc[
            data["CSV Files"] == date.strftime("%m-%d-%Y"),
            "Last Updated",
        ].iloc[0]

        return pd.Timestamp(last_updated_date, tz=self.default_timezone)

    def get_capacity_prices(
        self,
        date: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Pull the most recent capacity market report's market clearing prices

        Arguments:
            date (pandas.Timestamp): date that will be used to pull latest capacity
                report (will refer to month and year)

        Returns:
            a DataFrame of monthly capacity prices (all three auctions) for \
                each of the four capacity localities within NYISO
        """
        if date is None:
            date = pd.Timestamp.now(tz=self.default_timezone)
        else:
            date = utils._handle_date(date, tz=self.default_timezone)

        if date.year == 2014:
            year_code = 1410927
        elif date.year == 2015:
            year_code = 1410895
        elif date.year == 2016:
            year_code = 1410901
        if date.year == 2017:
            year_code = 1410883
        if date.year == 2018:
            year_code = 1410889
        if date.year == 2019:
            year_code = 4266869
        elif date.year == 2020:
            year_code = 10106066
        elif date.year == 2021:
            year_code = 18170164
        elif date.year == 2022:
            year_code = 27447313
        elif date.year == 2023:
            year_code = 35397361
        elif date.year == 2024:
            year_code = 42146126
        elif date.year == 2025:
            year_code = 48997190
        else:
            raise ValueError(
                "Year not currently supported. Please file an issue.",
            )

            # todo: it looks like the "27447313" component of the base URL changes
            # every year but I'm not sure what the link between that and the year
            # is...
        capacity_market_base_url = f"https://www.nyiso.com/documents/20142/{year_code}"

        url = f"{capacity_market_base_url}/ICAP-Market-Report-{date.month_name()}-{date.year}.xlsx"

        # Special case
        if date.month_name() == "December" and date.year == 2023:
            url = f"{capacity_market_base_url}/ICAP%20Market%20Report%20-%20{date.month_name()}%20{date.year}.xlsx"

        logger.info(f"Requesting {url}")

        df = pd.read_excel(url, sheet_name="MCP Table", header=[0, 1])

        df.rename(columns={"Unnamed: 0_level_0": "", "Date": ""}, inplace=True)
        df.set_index("", inplace=True)
        return df.dropna(how="any", axis="columns")

    @support_date_range(frequency="DAY_START")
    def get_as_prices_day_ahead_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Pull the most recent ancillary service market report's market clearing prices

        Arguments:
            date (pandas.Timestamp): date that will be used to pull latest capacity
                report (will refer to month and year)
        """
        if date == "latest":
            try:
                return self.get_as_prices_day_ahead_hourly(
                    (
                        pd.Timestamp.now(tz=self.default_timezone).normalize()
                        + pd.DateOffset(days=1)
                    ).strftime("%Y-%m-%d"),
                )
            except urllib.error.HTTPError:
                return self.get_as_prices_day_ahead_hourly("today")

        df = self._download_nyiso_archive(
            date=date,
            verbose=verbose,
            dataset_name="damasp",
        )
        df = self._handle_as_prices(df, rt_or_dam="dam")
        return df

    @support_date_range(frequency="DAY_START")
    def get_as_prices_real_time_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Pull the most recent ancillary service market report's market clearing prices

        Arguments:
            date (pandas.Timestamp): date that will be used to pull latest capacity
                report (will refer to month and year)
        """
        if date == "latest":
            return self.get_as_prices_real_time_5_min("today")

        df = self._download_nyiso_archive(
            date=date,
            verbose=verbose,
            dataset_name="rtasp",
        )
        df = self._handle_as_prices(df, rt_or_dam="rt")
        return df

    def _handle_as_prices(
        self,
        df: pd.DataFrame,
        rt_or_dam: Literal["rt", "dam"],
    ) -> pd.DataFrame:
        df = df.rename(
            columns={
                "Name": "Zone",
                "10 Min Spinning Reserve ($/MWHr)": "10 Min Spin Reserves",
                "10 Min Non-Synchronous Reserve ($/MWHr)": "10 Min Non-Spin Reserves",
                "30 Min Operating Reserve ($/MWHr)": "30 Min Reserves",
                "NYCA Regulation Capacity ($/MWHr)": "Regulation Capacity",
            },
        )
        if rt_or_dam == "rt":
            df["Interval End"] = df["Interval Start"]
            df["Interval Start"] = df["Interval Start"] - pd.Timedelta(minutes=5)
        else:
            df["Interval End"] = df["Interval Start"] + pd.Timedelta(
                minutes=60,
            )

        return df[
            [
                "Interval Start",
                "Interval End",
                "Zone",
                "10 Min Spin Reserves",
                "10 Min Non-Spin Reserves",
                "30 Min Reserves",
                "Regulation Capacity",
            ]
        ]

    @support_date_range(frequency="DAY_START")
    def get_limiting_constraints_real_time(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            data = pd.read_csv(
                "https://mis.nyiso.com/public/csv/LimitingConstraints/currentLimitingConstraints.csv",
            )
            data = self._handle_time(
                data,
                LIMITING_CONSTRAINTS_REAL_TIME_DATASET,
                groupby="Limiting Facility",
            )
        else:
            data = self._download_nyiso_archive(
                date,
                end=end,
                dataset_name=LIMITING_CONSTRAINTS_REAL_TIME_DATASET,
                groupby="Limiting Facility",
                verbose=verbose,
            )

        data = data.rename(columns={"Constraint Cost($)": "Constraint Cost"})

        data = (
            data[
                [
                    "Interval Start",
                    "Interval End",
                    "Limiting Facility",
                    "Facility PTID",
                    "Contingency",
                    "Constraint Cost",
                ]
            ]
            .sort_values(["Interval Start", "Limiting Facility", "Contingency"])
            .reset_index(
                drop=True,
            )
        )

        return data

    @support_date_range(frequency="DAY_START")
    def get_limiting_constraints_day_ahead(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            try:
                tomorrow = pd.Timestamp.now(
                    tz=self.default_timezone,
                ).normalize() + pd.DateOffset(days=1)
                return self.get_limiting_constraints_day_ahead(
                    date=tomorrow.strftime("%Y-%m-%d"),
                    verbose=verbose,
                )
            except urllib.error.HTTPError:
                return self.get_limiting_constraints_day_ahead(
                    date="today",
                    verbose=verbose,
                )

        df = self._download_nyiso_archive(
            date,
            end=end,
            dataset_name=LIMITING_CONSTRAINTS_DAY_AHEAD_DATASET,
            groupby="Limiting Facility",
            verbose=verbose,
        )

        df = df.rename(columns={"Constraint Cost($)": "Constraint Cost"})

        df = (
            df[
                [
                    "Interval Start",
                    "Interval End",
                    "Limiting Facility",
                    "Facility PTID",
                    "Contingency",
                    "Constraint Cost",
                ]
            ]
            .sort_values(["Interval Start", "Limiting Facility", "Contingency"])
            .reset_index(
                drop=True,
            )
        )

        return df
