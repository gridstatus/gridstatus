import warnings
from enum import StrEnum
from io import BytesIO
from typing import BinaryIO, Dict, Literal, NamedTuple

import pandas as pd
import polars as pl
import requests

import gridstatus
from gridstatus import utils
from gridstatus.base import (
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
ZONAL_LOAD_HOURLY_DATASET = "palIntegrated"
GENERATION_OUTAGES_FORECAST_URL = (
    "http://mis.nyiso.com/public/csv/genmaint/gen_maint_report.csv"
)
FUEL_MIX_DATASET = "rtfuelmix"
LOAD_FORECAST_DATASET = "isolf"
DAM_LMP_DATASET = "damlbmp"
REAL_TIME_LMP_DATASET = "realtime"
REAL_TIME_HOURLY_LMP_DATASET = "rtlbmp"
REAL_TIME_EVENTS_DATASET = "RealTimeEvents"
BTM_SOLAR_ACTUAL_DATASET = "btmactualforecast"
BTM_SOLAR_FORECAST_DATASET = "btmdaforecast"
BTM_INSTALLED_CAPACITY_DATASET = "btminstalledcapacitytracking"
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
    ZONAL_LOAD_HOURLY_DATASET: DatasetInterval("start", 60),
    FUEL_MIX_DATASET: DatasetInterval("instantaneous", None),
    LOAD_FORECAST_DATASET: DatasetInterval("start", 60),
    DAM_LMP_DATASET: DatasetInterval("start", 60),
    REAL_TIME_LMP_DATASET: DatasetInterval("end", 5),
    REAL_TIME_HOURLY_LMP_DATASET: DatasetInterval("start", 60),
    REAL_TIME_EVENTS_DATASET: DatasetInterval("instantaneous", None),
    BTM_SOLAR_ACTUAL_DATASET: DatasetInterval("start", 60),
    BTM_SOLAR_FORECAST_DATASET: DatasetInterval("start", 60),
    BTM_INSTALLED_CAPACITY_DATASET: DatasetInterval("start", 1440),
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

    def _parse_time_stamp_column(
        self,
        df: pl.DataFrame,
        time_stamp_col: str,
    ) -> pl.DataFrame:
        if df.schema[time_stamp_col] == pl.Utf8:
            stripped = pl.col(time_stamp_col).str.replace(r"\s+(EDT|EST)$", "")
            return df.with_columns(
                pl.coalesce(
                    stripped.str.to_datetime(
                        format="%m/%d/%Y %H:%M:%S",
                        strict=False,
                    ),
                    stripped.str.to_datetime(
                        format="%m/%d/%Y %H:%M",
                        strict=False,
                    ),
                ).alias(time_stamp_col),
            )
        return df.with_columns(
            pl.col(time_stamp_col).cast(pl.Datetime("us")).alias(time_stamp_col),
        )

    def _handle_time(
        self,
        df: pl.DataFrame,
        dataset_name: str,
        groupby: str | None = None,
    ) -> pl.DataFrame:
        time_type, interval_duration_minutes = DATASET_INTERVAL_MAP[dataset_name]

        if "Time Stamp" in df.columns:
            time_stamp_col = "Time Stamp"
        elif "Timestamp" in df.columns:
            time_stamp_col = "Timestamp"

        df = self._parse_time_stamp_column(df, time_stamp_col)

        if "Time Zone" in df.columns:
            df = df.with_columns(
                pl.when(pl.col("Time Zone") == "EDT")
                .then(pl.lit("earliest"))
                .otherwise(pl.lit("latest"))
                .alias("_ambiguous"),
            )
            df = df.with_columns(
                pl.col(time_stamp_col).dt.replace_time_zone(
                    self.default_timezone,
                    ambiguous=pl.col("_ambiguous"),
                ),
            ).drop("_ambiguous")
        elif "Name" in df.columns or groupby:
            groupby_col = groupby or "Name"
            df = utils.localize_ambiguous_infer_polars(
                df,
                time_stamp_col,
                self.default_timezone,
                group_cols=[groupby_col],
            )
        else:
            df = utils.localize_ambiguous_infer_polars(
                df,
                time_stamp_col,
                self.default_timezone,
            )

        df = df.rename({time_stamp_col: "Time"})

        if time_type != "instantaneous":
            interval_duration = pl.duration(minutes=interval_duration_minutes)
            if time_type == "start":
                df = df.with_columns(
                    pl.col("Time").alias("Interval Start"),
                    (pl.col("Time") + interval_duration).alias("Interval End"),
                )
            elif time_type == "end":
                df = df.with_columns(
                    (pl.col("Time") - interval_duration).alias("Interval Start"),
                    pl.col("Time").alias("Interval End"),
                ).with_columns(
                    pl.col("Interval Start").alias("Time"),
                )

            df = utils.move_cols_to_front(
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
    ) -> pl.DataFrame:
        status_df = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name=REAL_TIME_EVENTS_DATASET,
            verbose=verbose,
        )

        status_df = status_df.rename({"Message": "Status"})

        state_change = "**State Change. System now operating in "

        def _parse_status_fields(status: str) -> dict[str, str | list[str] | None]:
            notes: list[str] | None = None
            if status == "Start of day system state is NORMAL":
                return {"Status": "Normal", "Notes": [status]}
            if state_change in status:
                notes = [status]
                parsed_status = status[
                    status.index(state_change) + len(state_change) : -len(" state.**")
                ].capitalize()
                return {"Status": parsed_status, "Notes": notes}
            return {"Status": status, "Notes": None}

        status_df = status_df.rename({"Status": "_orig_status"})
        status_df = (
            status_df.with_columns(
                pl.col("_orig_status")
                .map_elements(
                    _parse_status_fields,
                    return_dtype=pl.Struct(
                        {
                            "Status": pl.Utf8,
                            "Notes": pl.List(pl.Utf8),
                        },
                    ),
                )
                .alias("_parsed"),
            )
            .unnest("_parsed")
            .drop("_orig_status")
        )

        return status_df.select(["Time", "Status", "Notes"])

    @support_date_range(frequency="MONTH_START")
    def get_fuel_mix(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        # note: this is simlar datastructure to pjm
        mix_df = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name=FUEL_MIX_DATASET,
            verbose=verbose,
        )

        mix_df = mix_df.pivot(
            index="Time",
            on="Fuel Category",
            values="Gen MW",
            aggregate_function="first",
        )

        fuel_cols = sorted(c for c in mix_df.columns if c != "Time")
        return mix_df.select(["Time", *fuel_cols])

    @support_date_range(frequency="MONTH_START")
    def get_load(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns load at a previous date in 5 minute intervals for
          each zone and total load

        Parameters:
            date (str): Date to get load for. Can be "today", or
              a date in the format YYYY-MM-DD
            end (str): End date for date range. Optional.
            verbose (bool): Whether to print verbose output. Optional.

        Returns:
            polars.DataFrame: Load data for NYISO and each zone

        """
        data = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name=LOAD_DATASET,
            verbose=verbose,
        )

        df = data.pivot(
            index="Time",
            on="Name",
            values="Load",
            aggregate_function="first",
        )

        value_cols = [c for c in df.columns if c != "Time"]
        df = df.with_columns(pl.sum_horizontal(value_cols).alias("Load"))
        return df.select(["Time", "Load", *value_cols])

    @support_date_range(frequency="MONTH_START")
    def get_btm_solar(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns estimated BTM solar generation at a previous date in hourly
            intervals for system and each zone.

            Available ~8 hours after the end of the operating day.

        Parameters:
            date (str, pd.Timestamp, datetime.datetime): Date to get load for.
              Can be "today", or a date
            end (str, pd.Timestamp, datetime.datetime): End date for date range.
                Optional.
            verbose (bool): Whether to print verbose output. Optional.

        Returns:
            polars.DataFrame: BTM solar data for NYISO system and each zone

        """
        data = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name=BTM_SOLAR_ACTUAL_DATASET,
            filename="BTMEstimatedActual",
            verbose=verbose,
        )

        df = data.pivot(
            index=["Time", "Interval Start", "Interval End"],
            on="Zone Name",
            values="MW Value",
            aggregate_function="first",
        )

        zone_cols = [
            c for c in df.columns if c not in ["Time", "Interval Start", "Interval End"]
        ]
        other_cols = [c for c in zone_cols if c != "SYSTEM"]
        return df.select(
            ["Time", "Interval Start", "Interval End", "SYSTEM", *other_cols],
        )

    @support_date_range(frequency="MONTH_START")
    def get_btm_solar_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        data = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name=BTM_SOLAR_FORECAST_DATASET,
            verbose=verbose,
        )

        df = data.pivot(
            index=["Time", "Interval Start", "Interval End"],
            on="Zone Name",
            values="MW Value",
            aggregate_function="first",
        )

        zone_cols = [
            c for c in df.columns if c not in ["Time", "Interval Start", "Interval End"]
        ]
        other_cols = [c for c in zone_cols if c != "SYSTEM"]
        df = df.select(
            ["Time", "Interval Start", "Interval End", "SYSTEM", *other_cols],
        )

        # Report is published day before the forecast at 7:55 AM in NYISO time
        df = df.with_columns(
            (
                pl.col("Interval Start").dt.truncate("1d")
                - pl.duration(days=1)
                + pl.duration(hours=7, minutes=55)
            ).alias("Publish Time"),
        )

        return df.select(
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Publish Time",
                "SYSTEM",
                *other_cols,
            ],
        )

    @support_date_range(frequency="DAY_START")
    def get_btm_installed_capacity(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns NYISO's daily estimate of installed behind-the-meter solar capacity
        by zone.

        Source: https://mis.nyiso.com/public/P-70Clist.htm
        """
        if date == "latest":
            raise ValueError("Latest not supported for BTM installed capacity")

        data = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name=BTM_INSTALLED_CAPACITY_DATASET,
            filename="btminstalledcapacitytracking",
            verbose=verbose,
        )

        return data.select(
            [
                "Interval Start",
                "Interval End",
                "Zone Name",
                "MW Value",
            ],
        ).rename(
            {
                "Zone Name": "Zone",
                "MW Value": "MW",
            },
        )

    @support_date_range(frequency="MONTH_START")
    def get_load_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get load forecast for a date in 1 hour intervals"""
        # todo optimize this to accept a date range
        data = self._download_nyiso_archive(
            date,
            end=end,
            dataset_name=LOAD_FORECAST_DATASET,
            verbose=verbose,
        )

        data = data.select(
            ["Time", "Interval Start", "Interval End", "File Date", "NYISO"],
        ).rename(
            {
                "File Date": "Forecast Time",
                "NYISO": "Load Forecast",
            },
        )

        return data

    @support_date_range(frequency="MONTH_START")
    def get_zonal_load_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get zonal load forecast for a date in 1 hour intervals"""
        data = self._download_nyiso_archive(
            date,
            end=end,
            dataset_name=LOAD_FORECAST_DATASET,
            verbose=verbose,
        )

        data = data.select(
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
            ],
        ).rename(
            {
                "File Date": "Publish Time",
            },
        )

        return data

    def get_generation_outages_forecast(
        self,
        date: str | pd.Timestamp | Literal["latest"] = "latest",
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get forecasted generation outage capacity for the next 30 days."""
        if end is not None:
            raise ValueError(
                "Date ranges are not supported for generation outages forecast. "
                "Use date='latest'.",
            )

        if verbose:
            logger.info(f"Requesting {GENERATION_OUTAGES_FORECAST_URL}")

        response = requests.get(GENERATION_OUTAGES_FORECAST_URL)
        response.raise_for_status()

        publish_time = pd.to_datetime(
            response.headers["Last-Modified"],
            utc=True,
        ).tz_convert(self.default_timezone)

        df = pl.read_csv(BytesIO(response.content), infer_schema_length=None)
        df = df.rename(
            {
                "Date": "Interval Start",
                "Forecasted Generation Outage (MW)": "Generation Outage",
            },
        )
        df = df.with_columns(
            pl.col("Interval Start")
            .str.to_datetime(strict=False)
            .dt.replace_time_zone(self.default_timezone)
            .alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(days=1)).alias("Interval End"),
            pl.lit(publish_time).alias("Publish Time"),
            pl.col("Generation Outage").cast(pl.Float64, strict=False),
        )

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Generation Outage",
            ],
        ).sort(["Interval Start"])

    @support_date_range(frequency="MONTH_START")
    def get_zonal_load_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get hourly integrated real-time load by zone."""
        data = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name=ZONAL_LOAD_HOURLY_DATASET,
            filename="palIntegrated",
            groupby="Name",
            verbose=verbose,
        )

        df = data.rename(
            {
                "Name": "Zone",
                "Integrated Load": "Load",
            },
        )

        df = df.with_columns(
            pl.col("PTID").cast(pl.Float64, strict=False),
            pl.col("Load").cast(pl.Float64, strict=False),
        )

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Zone",
                "PTID",
                "Load",
            ],
        ).sort(["Interval Start", "Zone"])

    @support_date_range(frequency="MONTH_START")
    def get_interface_limits_and_flows_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get interface limits and flows for a date"""
        if date == "latest":
            data = pl.read_csv(
                "https://mis.nyiso.com/public/csv/ExternalLimitsFlows/currentExternalLimitsFlows.csv",
                infer_schema_length=None,
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
            {
                "Flow (MWH)": "Flow MW",
                "Positive Limit (MWH)": "Positive Limit MW",
                "Negative Limit (MWH)": "Negative Limit MW",
            },
        )

        return data.select(
            [
                "Interval Start",
                "Interval End",
                "Interface Name",
                "Point ID",
                "Flow MW",
                "Positive Limit MW",
                "Negative Limit MW",
            ],
        ).sort(["Interval Start", "Interface Name"])

    @support_date_range(frequency="MONTH_START")
    def get_lake_erie_circulation_real_time(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        data = self._download_nyiso_archive(
            date,
            end=end,
            dataset_name=LAKE_ERIE_CIRCULATION_REAL_TIME_DATASET,
            filename="ErieCirculationRT",
            verbose=verbose,
        )

        # The source has MWH in the column name but it's actually MW
        data = data.rename({"Lake Erie Circulation (MWH)": "MW"})

        return data.select(["Time", "MW"]).sort("Time")

    @support_date_range(frequency="MONTH_START")
    def get_lake_erie_circulation_day_ahead(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        data = self._download_nyiso_archive(
            date,
            end=end,
            dataset_name=LAKE_ERIE_CIRCULATION_DAY_AHEAD_DATASET,
            filename="ErieCirculationDA",
            verbose=verbose,
        )

        data = data.rename({"Lake Erie Circulation (MWH)": "MW"})

        return data.select(["Time", "MW"]).sort("Time")

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
    def _get_lmp(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        market: Markets | None = None,
        locations: list | None = None,
        location_type: NYISOLocationType | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
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
                df = pl.read_csv(url, infer_schema_length=None)
                df = self._handle_time(df, dataset_name=marketname)
                df = df.with_columns(pl.lit(market.value).alias("Market"))
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
        df: pl.DataFrame,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None,
        market: Markets,
        location_type: NYISOLocationType,
        locations: list | str,
    ) -> pl.DataFrame:
        columns = {
            "Name": "Location",
            "LBMP ($/MWHr)": "LMP",
            "Marginal Cost Losses ($/MWHr)": "Loss",
            "Marginal Cost Congestion ($/MWHr)": "Congestion",
            "Marginal Cost Congestion ($/MWH": "Congestion",
        }

        df = df.rename(
            {k: v for k, v in columns.items() if k in df.columns},
        )

        df = df.with_columns(
            pl.col("LMP").cast(pl.Float64, strict=False),
            pl.col("Loss").cast(pl.Float64, strict=False),
            pl.col("Congestion").cast(pl.Float64, strict=False),
        )

        # In NYISO raw data, a negative congestion number means a higher LMP. We
        # flip the sign to make it consistent with other ISOs where a negative
        # congestion number means a lower LMP. Thus, LMP = Energy + Loss + Congestion
        # for NYISO, as in other ISOs.
        df = df.with_columns(
            (-pl.col("Congestion")).alias("Congestion"),
        )
        df = df.with_columns(
            (pl.col("LMP") - pl.col("Loss") - pl.col("Congestion"))
            .round(2)
            .alias(
                "Energy",
            ),
            pl.lit(market.value).alias("Market"),
            pl.lit(NYISOLocationType(location_type).value.capitalize()).alias(
                "Location Type",
            ),
        )

        # We manually update the location type for the reference bus location because
        # it's included in the generator data.
        if (
            REFERENCE_BUS_LOCATION
            in df["Location"].unique(maintain_order=True).to_list()
        ):
            df = df.with_columns(
                pl.when(pl.col("Location") == REFERENCE_BUS_LOCATION)
                .then(pl.lit("Reference Bus"))
                .otherwise(pl.col("Location Type"))
                .alias("Location Type"),
            )

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
            most_recent_5_min_data = self._get_lmp(
                date="latest",
                market=Markets.REAL_TIME_5_MIN,
                location_type=location_type,
            )

            most_recent_5_min_timestamp = most_recent_5_min_data["Interval Start"].max()

            daily_subset = df.filter(
                pl.col("Interval Start") == most_recent_5_min_timestamp,
            ).sort(["Interval Start", "Location"])

            most_recent_subset = most_recent_5_min_data.filter(
                pl.col("Interval Start") == most_recent_5_min_timestamp,
            ).sort(["Interval Start", "Location"])

            compare_cols = ["LMP", "Loss", "Congestion"]
            if daily_subset.select(compare_cols).equals(
                most_recent_subset.select(compare_cols),
            ):
                mask_15_min = pl.col("Interval Start") > most_recent_5_min_timestamp
            else:
                mask_15_min = pl.col("Interval Start") >= most_recent_5_min_timestamp

            df = df.with_columns(
                pl.when(~mask_15_min)
                .then(pl.lit(Markets.REAL_TIME_5_MIN.value))
                .when(mask_15_min)
                .then(pl.lit(Markets.REAL_TIME_15_MIN.value))
                .otherwise(pl.col("Market"))
                .alias("Market"),
            )

            # For 15-min data, the original "Interval End" column contains the correct
            # end time (since the raw data has timestamps as interval END). However,
            # "Interval Start" was calculated assuming a 5-minute interval, so we need
            # to recalculate it for 15-minute intervals.
            # Interval Start = Interval End - 15 minutes
            df = df.with_columns(
                pl.when(mask_15_min)
                .then(pl.col("Interval End") - pl.duration(minutes=15))
                .otherwise(pl.col("Interval Start"))
                .alias("Interval Start"),
            )

        df = df.select(
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
            ],
        )

        df = utils.filter_lmp_locations(df, locations)

        return df.filter(pl.col("Market") == market.value).sort(
            ["Interval Start", "Location"],
        )

    @lmp_config(
        supports={
            Markets.REAL_TIME_5_MIN: ["latest", "today", "historical"],
            Markets.REAL_TIME_15_MIN: ["latest", "today"],
            Markets.REAL_TIME_HOURLY: ["latest", "today", "historical"],
            Markets.DAY_AHEAD_HOURLY: ["latest", "today", "historical"],
        },
    )
    def get_lmp(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        market: Markets | None = None,
        locations: list | None = None,
        location_type: NYISOLocationType | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Deprecated. Use the per-dataset methods instead:
        :meth:`get_lmp_real_time_5_min`, :meth:`get_lmp_real_time_15_min`,
        :meth:`get_lmp_real_time_hourly`, :meth:`get_lmp_day_ahead_hourly`.
        """
        warnings.warn(
            "NYISO.get_lmp is deprecated; use the per-dataset methods "
            "get_lmp_real_time_5_min, get_lmp_real_time_15_min, "
            "get_lmp_real_time_hourly, or get_lmp_day_ahead_hourly instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._get_lmp(
            date,
            end=end,
            market=market,
            locations=locations,
            location_type=location_type,
            verbose=verbose,
        )

    def _get_lmp_zone_and_generator(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        market: Markets,
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Fetch both zone and generator LMPs for a market.

        The generator data also contains the single Reference Bus location.
        """
        zone = self._get_lmp(
            date,
            end=end,
            market=market,
            locations="ALL",
            location_type=NYISOLocationType.ZONE,
            verbose=verbose,
        )
        generator = self._get_lmp(
            date,
            end=end,
            market=market,
            locations="ALL",
            location_type=NYISOLocationType.GENERATOR,
            verbose=verbose,
        )
        return zone, generator

    def get_lmp_real_time_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get real-time 5-minute (RTD) LMPs for all zone and generator locations."""
        zone, generator = self._get_lmp_zone_and_generator(
            date,
            market=Markets.REAL_TIME_5_MIN,
            end=end,
            verbose=verbose,
        )

        df = pl.concat([zone, generator], how="diagonal")

        # Only keep intervals where both zone and generator data are present so the
        # combined dataset is internally consistent.
        maximum_timestamp = min(
            zone["Interval Start"].max(),
            generator["Interval Start"].max(),
        )
        df = df.filter(pl.col("Interval Start") <= maximum_timestamp)

        return df.sort(["Interval Start", "Location"])

    def get_lmp_real_time_15_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get real-time 15-minute (RTC) LMPs for all zone and generator locations."""
        zone, generator = self._get_lmp_zone_and_generator(
            date,
            market=Markets.REAL_TIME_15_MIN,
            end=end,
            verbose=verbose,
        )

        df = pl.concat([zone, generator], how="diagonal")

        return df.sort(["Interval Start", "Location"])

    def get_lmp_real_time_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get real-time hourly LMPs for all zone and generator locations."""
        zone, generator = self._get_lmp_zone_and_generator(
            date,
            market=Markets.REAL_TIME_HOURLY,
            end=end,
            verbose=verbose,
        )

        df = pl.concat([zone, generator], how="diagonal")

        return df.sort(["Interval Start", "Location"])

    def get_lmp_day_ahead_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get day-ahead hourly LMPs for all zone and generator locations."""
        zone, generator = self._get_lmp_zone_and_generator(
            date,
            market=Markets.DAY_AHEAD_HOURLY,
            end=end,
            verbose=verbose,
        )

        df = pl.concat([zone, generator], how="diagonal")

        return df.sort(["Interval Start", "Location"])

    def get_raw_interconnection_queue(self) -> BinaryIO:
        url = "https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx"  # noqa

        logger.info(f"Downloading interconnection queue from {url}")
        response = requests.get(url)
        return utils.get_response_blob(response)

    def get_interconnection_queue(self) -> pl.DataFrame:
        """Return NYISO interconnection queue

        Additional Non-NYISO queue info: https://www3.dps.ny.gov/W/PSCWeb.nsf/All/286D2C179E9A5A8385257FBF003F1F7E?OpenDocument

        Returns:
            polars.DataFrame: Interconnection queue containing, active, withdrawn, \
                and completed project

        """  # noqa

        # 5 sheets - ['Interconnection Queue', 'Cluster Projects', 'Withdrawn', 'Cluster Projects-Withdrawn', 'In Service']
        # harded coded for now. perhaps this url can be parsed from the html here:
        raw_data = self.get_raw_interconnection_queue()

        def _process_active_sheet(df: pd.DataFrame) -> pd.DataFrame:
            return (
                df.dropna(
                    subset=["Queue Pos.", "Project Name"],
                )
                .copy()
                .rename(columns={"Points of Interconnection": "Interconnection Point"})
            )

        active = utils.read_excel_via_pandas(
            raw_data,
            sheet_name="Interconnection Queue",
            process=_process_active_sheet,
        )
        cluster_active = utils.read_excel_via_pandas(
            raw_data,
            sheet_name=" Cluster Projects",
            process=_process_active_sheet,
        )
        active = pl.concat([active, cluster_active], how="diagonal")
        active = active.with_columns(
            pl.lit(InterconnectionQueueStatus.ACTIVE.value).alias("Status"),
        )

        withdrawn = utils.read_excel_via_pandas(raw_data, sheet_name="Withdrawn")
        cluster_withdrawn = utils.read_excel_via_pandas(
            raw_data,
            sheet_name="Cluster Projects-Withdrawn",
        )
        withdrawn = pl.concat([withdrawn, cluster_withdrawn], how="diagonal")
        withdrawn = withdrawn.with_columns(
            pl.lit(InterconnectionQueueStatus.WITHDRAWN.value).alias("Status"),
            pl.col("Last Update").alias("Withdrawn Date"),
            pl.lit(None).alias("Withdrawal Comment"),
        ).rename({"Utility ": "Utility", "Owner/Developer": "Developer Name"})

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

        def _process_completed_sheet(df: pd.DataFrame) -> pd.DataFrame:
            completed = df.copy()
            completed.insert(15, "SGIA Tender Date", None)
            completed.insert(16, "CY Complete Date", None)
            completed.insert(17, "Proposed Initial-Sync Date", None)
            completed["Status"] = InterconnectionQueueStatus.COMPLETED.value
            completed.columns = completed.columns.to_flat_index().map(
                lambda c: completed_colnames_map[c],
            )
            completed["Actual Completion Date"] = completed["Last Updated Date"]
            return completed

        completed = utils.read_excel_via_pandas(
            raw_data,
            sheet_name="In Service",
            header=[0, 1],
            process=_process_completed_sheet,
        )

        if (
            "SGIA Tender Date" in active.columns
            and "SGIA Tender Date" not in completed.columns
        ):
            active = active.drop("SGIA Tender Date")

        dfs = [
            df
            for df in [active, withdrawn, completed]
            if df.height > 0 and not df.select(pl.all().is_null().all()).row(0)[0]
        ]
        queue = pl.concat(dfs, how="diagonal")

        # fix extra space in column name

        queue = queue.with_columns(
            pl.col("Type/ Fuel").replace(
                {
                    "S": "Solar",
                    "ES": "Energy Storage",
                    "W": "Wind",
                    "AC": "AC Transmission",
                    "DC": "DC Transmission",
                    "CT": "Combustion Turbine",
                    "CC": "Combined Cycle",
                    "M": "Methane",
                    "H": "Hydro",
                    "L": "Load",
                    "ST": "Steam Turbine",
                    "CC-NG": "Natural Gas",
                    "FC": "Fuel Cell",
                    "PS": "Pumped Storage",
                    "NU": "Nuclear",
                    "D": "Dual Fuel",
                    "NG": "Natural Gas",
                    "Wo": "Wood",
                    "F": "Flywheel",
                    "CC-D": "Combined Cycle - Dual Fuel",
                    "SW": "=Solid Waste",
                    "CT-NG": "Combustion Turbine - Natural Gas",
                    "DC/AC": "DC/AC Transmission",
                    "CT-D": "Combustion Turbine - Dual Fuel",
                    "CS-NG": "Steam Turbine & Combustion Turbine-  Natural Gas",
                    "ST-NG": "Steam Turbine - Natural Gas",
                },
            ),
            pl.max_horizontal(
                pl.col("SP (MW)")
                .replace("TBD", 0)
                .replace(" ", 0)
                .fill_null(0)
                .cast(pl.Float64, strict=False),
                pl.col("WP (MW)")
                .replace("TBD", 0)
                .replace(" ", 0)
                .fill_null(0)
                .cast(pl.Float64, strict=False),
            ).alias("Capacity (MW)"),
            pl.col("Date of IR").str.to_datetime(strict=False),
            pl.col("Proposed COD").str.to_datetime(strict=False),
            pl.col("Proposed In-Service Date").str.to_datetime(strict=False),
            pl.col("Proposed Initial-Sync Date").str.to_datetime(strict=False),
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

    def get_generators(self, verbose: bool = False) -> pl.DataFrame:
        """Get a list of generators in NYISO. When possible return capacity and fuel type information

        Returns:
            polars.DataFrame: a DataFrame of generators and locations

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

        df = pl.read_csv(generator_url, infer_schema_length=None)

        # need to be updated once a year. approximately around end of april
        # find it here: https://www.nyiso.com/gold-book-resources
        capacity_url_2024 = "https://www.nyiso.com/documents/20142/44474211/2024-NYCA-Generators.xlsx/41a5cba2-523a-9fe0-9830-a523839a2831"  # noqa

        logger.info(f"Requesting {capacity_url_2024}")

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

        def _process_table_2a(sheet_df: pd.DataFrame) -> pd.DataFrame:
            sheet_df = sheet_df.copy()
            sheet_df.columns = mapped_columns
            sheet_df["Generator Type"] = "Market Generator"
            return sheet_df

        def _process_table_2b(sheet_df: pd.DataFrame) -> pd.DataFrame:
            sheet_df = sheet_df.copy()
            sheet_df.columns = mapped_columns
            sheet_df["Generator Type"] = "Non-Market Generator"
            return sheet_df

        gen_2a = utils.read_excel_via_pandas(
            capacity_url_2024,
            sheet_name="Table III-2a",
            skiprows=3,
            header=[0, 1, 2, 3, 4],
            process=_process_table_2a,
        )
        gen_2b = utils.read_excel_via_pandas(
            capacity_url_2024,
            sheet_name="Table III-2b",
            skiprows=3,
            header=[0, 1, 2, 3, 4],
            process=_process_table_2b,
        )

        generators = pl.concat([gen_2a, gen_2b], how="diagonal")
        generators = generators.drop_nulls(subset=["PTID"])
        generators = generators.with_columns(pl.col("PTID").cast(pl.Int64))
        generators = generators.drop(["Zone", "LINE REF. NO."])

        # TODO: df has both Generator PTID and Aggregation PTID
        combined = df.join(
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
        state_code_map = {
            36: "New York",
            42: "Pennsylvania",
            25: "Massachusetts",
            34: "New Jersey",
        }

        combined = combined.with_columns(
            pl.col("Unit Type").replace(unit_type_map),
            pl.col("Fuel Type 1").replace(fuel_type_map),
            pl.col("Fuel Type 2").replace(fuel_type_map),
            (pl.col("Is Dual Fuel") == "YES").alias("Is Dual Fuel"),
            pl.col("State").replace(state_code_map),
        )

        # todo map county codes to names. info on first sheet of excel

        return combined

    def get_loads(self) -> pl.DataFrame:
        """Get a list of loads in NYISO

        Returns:
            polars.DataFrame: a DataFrame of loads and locations
        """

        url = "http://mis.nyiso.com/public/csv/load/load.csv"

        logger.info(f"Requesting {url}")

        return pl.read_csv(url, infer_schema_length=None)

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
    ) -> pl.DataFrame:
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
            polars.DataFrame: the downloaded data

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

            df = pl.read_csv(csv_url, infer_schema_length=None)
            df = self._handle_time(df, dataset_name, groupby=groupby)
            if add_file_date:
                df = df.with_columns(
                    pl.lit(self._get_load_forecast_file_date(date, verbose)).alias(
                        "File Date",
                    ),
                )
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
                content = z.open(csv_filename).read()
                df = pl.read_csv(BytesIO(content), infer_schema_length=None)

                if add_file_date:
                    # NB: The File Date is the last modified time of the individual csv file
                    file_date = pd.Timestamp(
                        *z.getinfo(csv_filename).date_time,
                        tz=self.default_timezone,
                    )
                    df = df.with_columns(pl.lit(file_date).alias("File Date"))
                df = self._handle_time(df, dataset_name, groupby=groupby)
                all_dfs.append(df)

            df = pl.concat(all_dfs, how="diagonal")

        return df.sort("Time")

    def _get_load_forecast_file_date(
        self,
        date: pd.Timestamp,
        verbose: bool = False,
    ) -> pd.Timestamp:
        """Retrieves the last updated time for load forecast file from the archive"""
        data = utils.read_html_via_pandas(
            "http://mis.nyiso.com/public/P-7list.htm",
            skiprows=2,
            header=0,
        )[0]

        last_updated_date = data.filter(
            pl.col("CSV Files") == date.strftime("%m-%d-%Y"),
        )["Last Updated"][0]

        clean = str(last_updated_date).replace(" EDT", "").replace(" EST", "")
        return pd.Timestamp(clean, tz=self.default_timezone)

    def get_capacity_prices(
        self,
        date: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
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
        elif date.year == 2026:
            year_code = 56195933
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

        def _process_mcp_table(sheet_df: pd.DataFrame) -> pd.DataFrame:
            sheet_df = sheet_df.rename(
                columns={"Unnamed: 0_level_0": "", "Date": ""},
            )
            sheet_df = sheet_df.set_index("")
            return sheet_df.dropna(how="any", axis="columns").reset_index()

        return utils.read_excel_via_pandas(
            url,
            sheet_name="MCP Table",
            header=[0, 1],
            process=_process_mcp_table,
        )

    @support_date_range(frequency="DAY_START")
    def get_as_prices_day_ahead_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Pull the most recent ancillary service market report's market clearing prices

        Arguments:
            date (pandas.Timestamp): date that will be used to pull latest capacity
                report (will refer to month and year)
        """
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
    ) -> pl.DataFrame:
        """Pull the most recent ancillary service market report's market clearing prices

        Arguments:
            date (pandas.Timestamp): date that will be used to pull latest capacity
                report (will refer to month and year)
        """
        df = self._download_nyiso_archive(
            date=date,
            verbose=verbose,
            dataset_name="rtasp",
        )
        df = self._handle_as_prices(df, rt_or_dam="rt")
        return df

    def _handle_as_prices(
        self,
        df: pl.DataFrame,
        rt_or_dam: Literal["rt", "dam"],
    ) -> pl.DataFrame:
        df = df.rename(
            {
                "Name": "Zone",
                "10 Min Spinning Reserve ($/MWHr)": "10 Min Spin Reserves",
                "10 Min Non-Synchronous Reserve ($/MWHr)": "10 Min Non-Spin Reserves",
                "30 Min Operating Reserve ($/MWHr)": "30 Min Reserves",
                "NYCA Regulation Capacity ($/MWHr)": "Regulation Capacity",
            },
        )
        if rt_or_dam == "rt":
            df = df.with_columns(
                pl.col("Interval Start").alias("Interval End"),
                (pl.col("Interval Start") - pl.duration(minutes=5)).alias(
                    "Interval Start",
                ),
            )
        else:
            df = df.with_columns(
                (pl.col("Interval Start") + pl.duration(minutes=60)).alias(
                    "Interval End",
                ),
            )

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Zone",
                "10 Min Spin Reserves",
                "10 Min Non-Spin Reserves",
                "30 Min Reserves",
                "Regulation Capacity",
            ],
        )

    @support_date_range(frequency="DAY_START")
    def get_limiting_constraints_real_time(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        if date == "latest":
            data = pl.read_csv(
                "https://mis.nyiso.com/public/csv/LimitingConstraints/currentLimitingConstraints.csv",
                infer_schema_length=None,
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

        data = data.rename({"Constraint Cost($)": "Constraint Cost"})

        return data.select(
            [
                "Interval Start",
                "Interval End",
                "Limiting Facility",
                "Facility PTID",
                "Contingency",
                "Constraint Cost",
            ],
        ).sort(["Interval Start", "Limiting Facility", "Contingency"])

    @support_date_range(frequency="DAY_START")
    def get_limiting_constraints_day_ahead(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        df = self._download_nyiso_archive(
            date,
            end=end,
            dataset_name=LIMITING_CONSTRAINTS_DAY_AHEAD_DATASET,
            groupby="Limiting Facility",
            verbose=verbose,
        )

        df = df.rename({"Constraint Cost($)": "Constraint Cost"})

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Limiting Facility",
                "Facility PTID",
                "Contingency",
                "Constraint Cost",
            ],
        ).sort(["Interval Start", "Limiting Facility", "Contingency"])
