import io
import warnings
from typing import BinaryIO

import pandas as pd
import polars as pl
import requests
from bs4 import BeautifulSoup

from gridstatus import utils
from gridstatus.base import (
    GridStatus,
    InterconnectionQueueStatus,
    ISOBase,
    Markets,
    NotSupported,
)
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import log
from gridstatus.lmp_config import lmp_config


class ISONE(ISOBase):
    """ISO New England (ISONE)"""

    name = "ISO New England"
    iso_id = "isone"
    default_timezone = "US/Eastern"

    status_homepage = "https://www.iso-ne.com/markets-operations/system-forecast-status/current-system-status"  # noqa
    interconnection_homepage = "https://irtt.iso-ne.com/reports/external"

    markets = [
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_HOURLY,
        Markets.DAY_AHEAD_HOURLY,
    ]

    lmp_real_time_intervals = ["00-04", "04-08", "08-12", "12-16", "16-20", "20-24"]

    hubs = {"H.INTERNAL_HUB": 4000}
    zones = {
        ".Z.MAINE": 4001,
        ".Z.NEWHAMPSHIRE": 4002,
        ".Z.VERMONT": 4003,
        ".Z.CONNECTICUT": 4004,
        ".Z.RHODEISLAND": 4005,
        ".Z.SEMASS": 4006,
        ".Z.WCMASS": 4007,
        ".Z.NEMASSBOST": 4008,
    }
    interfaces = {
        ".I.SALBRYNB345": 4010,
        ".I.ROSETON 345": 4011,
        ".I.HQ_P1_P2345": 4012,
        ".I.HQHIGATE120": 4013,
        ".I.SHOREHAM138": 4014,
        ".I.NRTHPORT138": 4017,
    }

    def get_status(self, date, verbose=False):
        """Get latest status for ISO NE"""

        if date != "latest":
            raise NotSupported()

        # historical data available
        # https://www.iso-ne.com/markets-operations/system-forecast-status/current-system-status/power-system-status-list
        data = _make_wsclient_request(
            url="https://www.iso-ne.com/ws/wsclient",
            data={
                "_nstmp_requestType": "systemconditions",
                "_nstmp_requestUrl": "/powersystemconditions/current",
            },
        )

        # looks like it could return multiple entries
        condition = data[0]["data"]["PowerSystemConditions"]["PowerSystemCondition"][0]
        status = condition["SystemCondition"]
        note = condition["ActionDescription"]
        time = pd.Timestamp.now(tz=self.default_timezone).floor(freq="s")

        return GridStatus(
            time=time,
            status=status,
            reserves=None,
            iso=self,
            notes=[note],
        )

    # this return different date then other end point
    # lets just use the other one for now
    # def _get_latest_fuel_mix(self):
    #     data = _make_wsclient_request(
    #         url="https://www.iso-ne.com/ws/wsclient",
    #         data={"_nstmp_requestType": "fuelmix"},
    #     )
    #     mix_df = pd.DataFrame(data[0]["data"]["GenFuelMixes"]["GenFuelMix"])
    #     time = pd.Timestamp(
    #         mix_df["BeginDate"].max(),
    #         tz=self.default_timezone,
    #     )

    #     # todo has marginal flag
    #     mix_df = mix_df.set_index("FuelCategory")[
    #         ["GenMw"]].T.reset_index(drop=True)
    #     mix_df.insert(0, "Time", time)
    #     mix_df.columns.name = None
    #     return mix_df

    # NOTE(Kladar): This is a deprecated in favor of the ISONEAPI method.
    @support_date_range(frequency="DAY_START")
    def get_fuel_mix(self, date, end=None, verbose=False):
        """Return fuel mix at a previous date

        Provided at frequent, but irregular intervals by ISONE
        """
        if date == "latest":
            return self.get_fuel_mix("today", verbose=verbose).tail(1)

        url = "https://www.iso-ne.com/transform/csv/genfuelmix?start=" + date.strftime(
            "%Y%m%d",
        )

        df = _make_request(url, skiprows=[0, 1, 2, 3, 5], verbose=verbose)

        naive = pd.to_datetime(df["Date"] + " " + df["Time"])
        pl_df = pl.DataFrame(
            {
                "Date": naive.to_numpy(),
                "Fuel Category": df["Fuel Category"].astype(str).to_numpy(),
                "Gen Mw": pd.to_numeric(df["Gen Mw"], errors="coerce").to_numpy(),
            },
        )

        pl_df = utils.localize_ambiguous_infer_polars(
            pl_df,
            "Date",
            self.default_timezone,
            group_cols=["Fuel Category"],
        )

        mix_df = pl_df.pivot(
            "Fuel Category",
            index="Date",
            values="Gen Mw",
            aggregate_function="first",
        )

        mix_df = mix_df.rename({"Date": "Time"}).fill_null(0)

        fuel_cols = sorted(c for c in mix_df.columns if c != "Time")
        return mix_df.select(["Time", *fuel_cols]).sort("Time")

    @support_date_range(frequency="DAY_START")
    def get_load(self, date, end=None, verbose=False):
        """Return load at a previous date in 5 minute intervals"""
        # todo document the earliest supported date
        # supports a start and end date
        if date == "latest":
            return self.get_load("today", verbose=verbose)

        date_str = date.strftime("%Y%m%d")
        url = f"https://www.iso-ne.com/transform/csv/fiveminutesystemload?start={date_str}&end={date_str}"  # noqa
        data = _make_request(url, skiprows=[0, 1, 2, 3, 5], verbose=verbose)

        localized = pd.to_datetime(data["Date/Time"]).dt.tz_localize(
            self.default_timezone,
            ambiguous="infer",
        )
        pl_df = pl.from_pandas(
            pd.DataFrame(
                {
                    "Time": localized,
                    "Load": pd.to_numeric(data["Native Load"], errors="coerce"),
                },
            ),
        )

        pl_df = pl_df.with_columns(
            pl.col("Time").alias("Interval Start"),
            (pl.col("Time") + pl.duration(minutes=5)).alias("Interval End"),
        )

        return pl_df.select(["Time", "Interval Start", "Interval End", "Load"])

    @support_date_range(frequency="DAY_START")
    def get_btm_solar(self, date, end=None, verbose=False):
        """Return BTM solar at a previous date in 5 minute intervals"""
        df = self._get_system_load(
            date,
            end=date,
            series="actual",
            verbose=verbose,
        )

        return df.with_columns(
            (pl.col("NativeLoadBtmPv") - pl.col("Load")).alias("BTM Solar"),
            pl.col("Time").alias("Interval Start"),
            (pl.col("Time") + pl.duration(minutes=5)).alias("Interval End"),
        ).select(["Time", "Interval Start", "Interval End", "BTM Solar"])

    @support_date_range(frequency="DAY_START")
    def get_load_forecast(self, date, end=None, verbose=False):
        """Return forecast at a previous date"""

        df = self._get_system_load(
            date,
            end=date + pd.Timedelta(days=1),
            series="forecast",
            verbose=verbose,
        )

        return df.with_columns(
            pl.col("Time").alias("Interval Start"),
            (pl.col("Time") + pl.duration(hours=1)).alias("Interval End"),
        ).select(
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Forecast Time",
                "Load Forecast",
            ],
        )

    def get_solar_forecast(self, date, end=None, verbose=False):
        """Return solar forecast published on a specific date

        Forecast is published for 7 days and generated daily by 10 am.
        https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/seven-day-solar-power-forecast
        """
        return self._get_solar_or_wind_forecast(
            date,
            end,
            resource_type="Solar",
            verbose=verbose,
        ).sort(["Interval Start", "Publish Time"])

    def get_wind_forecast(self, date, end=None, verbose=False):
        """Return wind forecast published on a specific date

        Forecast is published for 7 days and generated daily by 10 am.
        https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/seven-day-wind-power-forecast
        """
        return self._get_solar_or_wind_forecast(
            date,
            end,
            resource_type="Wind",
            verbose=verbose,
        ).sort(["Interval Start", "Publish Time"])

    @support_date_range(frequency="DAY_START")
    def _get_solar_or_wind_forecast(
        self,
        date,
        end=None,
        resource_type="Wind",
        verbose=False,
    ):
        """Return solar or wind forecast published on a specific date

        Resource type can be "Solar" or "Wind"
        """
        if date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone)

        value_name = f"{resource_type.capitalize()} Forecast"
        file_designator = "wphf" if resource_type == "Wind" else "sphf"

        url = f"https://www.iso-ne.com/transform/csv/{file_designator}?start={date.strftime('%Y%m%d')}"  # noqa

        df = _make_request(url, skiprows=[0, 1, 2, 3, 5], verbose=verbose)

        df.columns = df.iloc[0]
        df = df.drop(columns=["D", "Date"], index=[0]).reset_index(drop=True)

        pl_df = pl.from_pandas(df)
        value_cols = [c for c in pl_df.columns if c != "Hour Ending"]
        data = pl_df.unpivot(
            index="Hour Ending",
            on=value_cols,
            variable_name="Date",
            value_name=value_name,
        ).drop_nulls(subset=[value_name])

        data = utils.create_interval_start_from_hour_start_polars(data)
        data = utils.localize_interval_start_polars(
            data,
            "Interval Start",
            self.default_timezone,
            date_col="Date",
        )

        data = data.with_columns(
            (pl.col("Interval Start") + pl.duration(hours=1)).alias("Interval End"),
        )

        report_datetime = date.normalize() + pd.Timedelta(hours=10)
        data = data.with_columns(pl.lit(report_datetime).alias("Publish Time"))

        data = utils.move_cols_to_front(
            data,
            ["Interval Start", "Interval End", "Publish Time", value_name],
        ).drop(["Date", "Hour Start", "Hour Ending"])

        return data.with_columns(
            pl.col(value_name).cast(pl.Float64),
        )

    def _get_latest_lmp(self, market: str, locations: list = None, verbose=False):
        """
        Find Node ID mapping: https://www.iso-ne.com/markets-operations/settlements/pricing-node-tables/
        """  # noqa
        if locations is None:
            locations = "ALL"

        if market == Markets.REAL_TIME_5_MIN:
            url = "https://www.iso-ne.com/transform/csv/fiveminlmp/current?type=prelim"  # noqa
            data = pl.from_pandas(
                _make_request(url, skiprows=[0, 1, 2, 4], verbose=verbose),
            )
            data = data.rename({"Local Time": "Interval Start"})

        elif market == Markets.REAL_TIME_HOURLY:
            url = "https://www.iso-ne.com/transform/csv/hourlylmp/current?type=prelim&market=rt"  # noqa
            data = pl.from_pandas(
                _make_request(url, skiprows=[0, 1, 2, 4], verbose=verbose),
            )

            data = data.rename(
                {
                    "Local Date": "Date",
                    "Local Time": "Hour Ending",
                },
            )

        else:
            raise RuntimeError("LMP Market is not supported")

        data = self._process_lmp(
            data,
            market,
            self.default_timezone,
            locations,
        )
        return data

    @lmp_config(
        supports={
            Markets.REAL_TIME_5_MIN: ["latest", "today", "historical"],
            Markets.REAL_TIME_HOURLY: ["latest", "today", "historical"],
            Markets.DAY_AHEAD_HOURLY: ["today", "historical"],
        },
    )
    @support_date_range(frequency="DAY_START")
    def _get_lmp(
        self,
        date,
        end=None,
        market: str = None,
        locations: list = None,
        include_id=False,
        verbose=False,
    ):
        """
        Find Node ID mapping:
            https://www.iso-ne.com/markets-operations/settlements/pricing-node-tables/
        """  # noqa
        if date == "latest":
            return self._get_latest_lmp(
                market=market,
                locations=locations,
                verbose=verbose,
            )

        date_str = date.strftime("%Y%m%d")

        if locations is None:
            locations = "ALL"

        now = pd.Timestamp.now(tz=self.default_timezone)

        if market == Markets.REAL_TIME_5_MIN:
            intervals = self.lmp_real_time_intervals[:]

            querying_for_today = date.date() == now.date()

            if querying_for_today:
                intervals = self._select_intervals_for_data_request(
                    date,
                    end,
                    self.lmp_real_time_intervals,
                )

            dfs = []
            for interval in intervals:
                msg = "Loading interval {}".format(interval)
                log(msg, verbose=verbose)
                u = f"https://www.iso-ne.com/static-transform/csv/histRpts/5min-rt-prelim/lmp_5min_{date_str}_{interval}.csv"  # noqa
                try:
                    dfs.append(
                        pl.from_pandas(
                            pd.read_csv(
                                u,
                                skiprows=[0, 1, 2, 3, 5],
                                skipfooter=1,
                                engine="python",
                            ),
                        ),
                    )
                except Exception as e:
                    log(f"Failed to load {u} with {e}", verbose=verbose)

            data_intervals = None

            if dfs:
                data_intervals = pl.concat(dfs, how="diagonal")
                data_intervals = data_intervals.with_columns(
                    (
                        pl.lit(date.strftime("%Y-%m-%d "))
                        + pl.col("Local Time").cast(pl.Utf8)
                    )
                    .str.to_datetime()
                    .alias("Local Time"),
                )

            if querying_for_today:
                url = "https://www.iso-ne.com/transform/csv/fiveminlmp/currentrollinginterval"  # noqa
                msg = "Loading current interval"
                log(msg, verbose=verbose)
                data_current = pl.from_pandas(
                    _make_request(
                        url,
                        skiprows=[0, 1, 2, 4],
                        verbose=verbose,
                    ),
                )

                data_current = data_current.with_columns(
                    pl.col("Local Time").str.to_datetime().alias("Local Time"),
                )

                if data_intervals is not None and data_intervals.height > 0:
                    max_time = data_intervals.select(
                        pl.col("Local Time").max(),
                    ).item()
                    data_current = data_current.filter(
                        pl.col("Local Time") > max_time,
                    )
                    data = pl.concat([data_intervals, data_current], how="diagonal")
                else:
                    data_current = data_current.filter(
                        pl.col("Local Time").dt.date() == now.date(),
                    )
                    data = data_current
            else:
                data = data_intervals

            data = data.rename({"Local Time": "Interval Start"})

        elif market == Markets.REAL_TIME_HOURLY:
            if date.date() > now.date():
                raise RuntimeError(
                    f"date {date.date()} is in the future and cannot be used to query real-time data",
                )

            url = f"https://www.iso-ne.com/static-transform/csv/histRpts/rt-lmp/lmp_rt_prelim_{date_str}.csv"  # noqa
            data = pl.from_pandas(
                _make_request(
                    url,
                    skiprows=[0, 1, 2, 3, 5],
                    verbose=verbose,
                ),
            )

        elif market == Markets.DAY_AHEAD_HOURLY:
            url = f"https://www.iso-ne.com/static-transform/csv/histRpts/da-lmp/WW_DALMP_ISO_{date_str}.csv"  # noqa
            data = pl.from_pandas(
                _make_request(
                    url,
                    skiprows=[0, 1, 2, 3, 5],
                    verbose=verbose,
                ),
            )

        else:
            raise RuntimeError("LMP Market is not supported")

        data = self._process_lmp(
            data,
            market,
            self.default_timezone,
            locations,
            include_id=include_id,
        )

        return data

        # daily historical fuel mix
        # https://www.iso-ne.com/static-assets/documents/2022/01/2022_daygenbyfuel.xlsx
        # a bunch more here: https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/daily-gen-fuel-type

    @lmp_config(
        supports={
            Markets.REAL_TIME_5_MIN: ["latest", "today", "historical"],
            Markets.REAL_TIME_HOURLY: ["latest", "today", "historical"],
            Markets.DAY_AHEAD_HOURLY: ["today", "historical"],
        },
    )
    def get_lmp(
        self,
        date,
        end=None,
        market: str = None,
        locations: list = None,
        include_id=False,
        verbose=False,
    ):
        """Deprecated. Use the per-dataset methods instead:
        :meth:`get_lmp_real_time_5_min`, :meth:`get_lmp_real_time_hourly`,
        :meth:`get_lmp_day_ahead_hourly`.
        """
        warnings.warn(
            "ISONE.get_lmp is deprecated; use the per-dataset methods "
            "get_lmp_real_time_5_min, get_lmp_real_time_hourly, or "
            "get_lmp_day_ahead_hourly instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._get_lmp(
            date,
            end=end,
            market=market,
            locations=locations,
            include_id=include_id,
            verbose=verbose,
        )

    def get_lmp_real_time_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get real-time 5-minute LMPs for all locations."""
        return self._get_lmp(
            date,
            end=end,
            market=Markets.REAL_TIME_5_MIN,
            locations="ALL",
            verbose=verbose,
        )

    def get_lmp_real_time_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get real-time hourly LMPs for all locations."""
        return self._get_lmp(
            date,
            end=end,
            market=Markets.REAL_TIME_HOURLY,
            locations="ALL",
            verbose=verbose,
        )

    def get_lmp_day_ahead_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get day-ahead hourly LMPs for all locations."""
        return self._get_lmp(
            date,
            end=end,
            market=Markets.DAY_AHEAD_HOURLY,
            locations="ALL",
            verbose=verbose,
        )

    def _process_lmp(self, data, market, timezone, locations, include_id=False):
        rename = {
            "Location Name": "Location",
            "Location ID": "Location Id",
            "Location Type": "Location Type",
            "Local Time": "Time",
            "Locational Marginal Price": "LMP",
            "LMP": "LMP",
            "Energy Component": "Energy",
            "Congestion Component": "Congestion",
            "Loss Component": "Loss",
            "Marginal Loss Component": "Loss",
        }

        data = data.rename(
            {k: v for k, v in rename.items() if k in data.columns},
        )
        data = data.with_columns(pl.lit(market.value).alias("Market"))

        if market == Markets.REAL_TIME_5_MIN:
            interval_duration = pl.duration(minutes=5)
        else:
            interval_duration = pl.duration(hours=1)

        if "Hour Ending" in data.columns:
            data = utils.create_interval_start_from_hour_start_polars(data)
        elif not data.schema["Interval Start"].is_temporal():
            data = data.with_columns(
                pl.col("Interval Start").str.to_datetime().alias("Interval Start"),
            )

        location_groupby = "Location" if "Location" in data.columns else "Location Id"
        data = utils.localize_ambiguous_infer_polars(
            data,
            "Interval Start",
            timezone,
            group_cols=[location_groupby],
        )

        data = data.with_columns(
            (pl.col("Interval Start") + interval_duration).alias("Interval End"),
            pl.col("Interval Start").alias("Time"),
        )

        if market != Markets.DAY_AHEAD_HOURLY:
            min_interval = data.select(pl.col("Interval Start").min()).item()
            day_ahead = self._get_lmp(
                date=min_interval.date(),
                market=Markets.DAY_AHEAD_HOURLY,
                locations=locations,
                include_id=True,
            )
            location_mapping = day_ahead.unique(
                subset=["Location Id"],
                keep="first",
            ).select(["Location", "Location Id", "Location Type"])

            if "Location Id" in data.columns:
                data = data.join(location_mapping, on="Location Id", how="left")
            elif "Location" in data.columns:
                data = data.join(location_mapping, on="Location", how="left")

        data = data.select(
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Market",
                "Location",
                "Location Id",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ],
        )

        if not include_id:
            data = data.drop("Location Id")

        data = utils.filter_lmp_locations(data, locations)
        return data

    def get_raw_interconnection_queue(self, verbose=False) -> BinaryIO:
        """Extract raw ISONE interconnection queue data.

        ISONE interconnection queue data is available on a webpage
        as an HTML table or you can download it as an excel file.
        Obviously an excel file would be much easier to work with however,
        the helpful generalized "Status" column (Withdrawn, Active, Commercial)
        and the "Jurisdiction" column are only available as HTML.

        Also, there is helpful detailed status information in the
        FS, SIS, OS, FAC, IA columns that are represented as <img>
        tags in the HTML.

        This function replaces the <img> tags that convey detailed
        status information as text and extracts the html as a dataframe.
        You can see the image to text mapping in the upper left hand
        corner of the ISONE Queue data page: https://irtt.iso-ne.com/reports/external.
        """
        r = requests.get("https://irtt.iso-ne.com/reports/external")

        soup = BeautifulSoup(r.text, "html.parser")

        status_strings = [
            "Under Study",
            "Under Construction",
            "Partially in Service",
            "In Service",
            "Suspended",
            "In Progress",
            "Document Posted",
            "Interim Study",
            "ISA Not Executed",
            "Not Required",
            "Not Started",
            "Executed",
        ]

        for status_string in status_strings:
            img_tags = soup.find_all("img", title=status_string)

            for img_tag in img_tags:
                new_text_tag = soup.new_tag("span")
                new_text_tag.string = status_string
                img_tag.replace_with(new_text_tag)

        return io.BytesIO(str(soup).encode("utf-8"))

    # TODO: this no longer works due to a error in requesting the file
    def get_interconnection_queue(self, verbose=False):
        """Get the interconnection queue. Contains active and withdrawm applications.

        More information: https://www.iso-ne.com/system-planning/interconnection-service/interconnection-request-queue/


        Returns:
            pandas.DataFrame: interconnection queue

        """  # noqa

        # determine report date from homepage

        msg = f"Loading queue {self.interconnection_homepage}"
        log(msg, verbose)

        raw_data = self.get_raw_interconnection_queue(verbose)
        queue = pl.from_pandas(
            pd.read_html(raw_data, attrs={"id": "publicqueue"})[0],
        )

        queue = queue.with_columns(
            pl.col("Type").replace(
                {
                    "G": "Generation",
                    "ETU": "Elective Transmission Upgrade",
                    "TS": "Transmission Service",
                },
            ),
            pl.col("Status").replace(
                {
                    "W": InterconnectionQueueStatus.WITHDRAWN.value,
                    "A": InterconnectionQueueStatus.ACTIVE.value,
                    "C": InterconnectionQueueStatus.COMPLETED.value,
                },
            ),
            pl.col("Sync Date").alias("Proposed Completion Date"),
        )

        rename = {
            "QP": "Queue ID",
            "Alternative Name": "Project Name",
            "Fuel Type": "Generation Type",
            "Requested": "Queue Date",
            "County": "County",
            "ST": "State",
            "Status": "Status",
            "POI": "Interconnection Location",
            "W/D Date": "Withdrawn Date",
            "Net MW": "Capacity (MW)",
            "Summer MW": "Summer Capacity (MW)",
            "Winter MW": "Winter Capacity (MW)",
            "TO Report": "Transmission Owner",
            # Full status column names are from ISONE Video:
            # https://iso-ne.my.site.com/s/article/How-do-I-read-the-public-version-of-the-ISO-Queue
            "SIS": "System Impact Study Completed",
            "FS": "Feasiblity Study Status",
            "SIS.1": "System Impact Study Status",
            "OS": "Optional Interconnection Study Status",
            "FAC": "Facilities Study Status",
            "IA": "Interconnection Agreement Status",
        }

        # todo: there are a few columns being parsed as "unamed"
        # that aren't being included but should
        extra_columns = [
            "Updated",
            "Unit",
            "Op Date",
            "Sync Date",
            "Serv",
            "I39",
            "Dev",
            "Zone",
            "System Impact Study Completed",
            "Feasiblity Study Status",
            "System Impact Study Status",
            "Optional Interconnection Study Status",
            "Facilities Study Status",
            "Interconnection Agreement Status",
            "Project Status",
        ]

        missing = [
            "Interconnecting Entity",
            "Actual Completion Date",
            # because there are only activate and withdrawn projects
            "Withdrawal Comment",
        ]

        queue = utils.format_interconnection_df(
            queue=queue,
            rename=rename,
            extra=extra_columns,
            missing=missing,
        )

        queue = queue.sort("Queue ID", descending=True)

        return queue

    def _get_system_load(self, date, end, series, verbose=False):
        start_str = date.strftime("%m/%d/%Y")
        end_str = end.strftime("%m/%d/%Y")
        params = {
            "_nstmp_startDate": start_str,
            "_nstmp_endDate": end_str,
            "_nstmp_twodays": True,
            "_nstmp_twodaysCheckbox": False,
            "_nstmp_requestType": "systemload",
            "_nstmp_forecast": True,
            "_nstmp_actual": True,
            "_nstmp_cleared": True,
            "_nstmp_priorDay": False,
            "_nstmp_inclPumpLoad": True,
            "_nstmp_inclBtmPv": True,
        }

        raw_data = _make_wsclient_request(
            url="https://www.iso-ne.com/ws/wsclient",
            data=params,
            verbose=verbose,
        )

        if series == "actual":
            mw_rename = "Load"
        elif series == "forecast":
            mw_rename = "Load Forecast"
        else:
            raise ValueError(f"Unrecognized series: {series}")

        records = raw_data[0]["data"][series]

        pl_df = pl.DataFrame(records)

        # ISONE returns offset-aware ISO timestamps (e.g. 2024-01-01T00:00:00.000-05:00).
        # polars requires an explicit format when the offset is part of the data.
        iso_offset_format = "%Y-%m-%dT%H:%M:%S%.f%z"

        pl_df = pl_df.with_columns(
            pl.col("BeginDate")
            .str.to_datetime(format=iso_offset_format)
            .dt.convert_time_zone(self.default_timezone),
        )

        if "CreationDate" in pl_df.columns:
            pl_df = pl_df.with_columns(
                pl.col("CreationDate")
                .str.to_datetime(format=iso_offset_format)
                .dt.convert_time_zone(self.default_timezone),
            )

        rename = {
            "BeginDate": "Time",
            "Mw": mw_rename,
            "CreationDate": "Forecast Time",
        }
        rename = {k: v for k, v in rename.items() if k in pl_df.columns}

        return pl_df.rename(rename)

    def _select_intervals_for_data_request(self, date, end, intervals):
        """Filters intervals given a start and end datetime. All completed intervals
        are included as well as any intervals that include the start datetime.

        Args:
            date (Datetime): Start datetime. Must be in self.default_timezone
            end (Optional[Datetime]): End datetime. Must be in self.default_timezone
                if provided
            intervals (list): List of intervals in the format "HH-HH"
        """
        now = self.local_now()

        # Converts hour into the index in 4 hour interval array
        def _hour_to_interval_index(hour):
            if not 0 <= hour <= 23:
                raise ValueError("Hour must be between 0 and 23")
            return hour // 4

        # No need to get data for future intervals. This method will never return
        # the 20-24 interval.
        last_interval_hour = min(now.hour, end.hour) if end else now.hour
        selected_intervals = intervals[
            _hour_to_interval_index(date.hour) : _hour_to_interval_index(
                last_interval_hour,
            )
        ]

        return selected_intervals

    @support_date_range(frequency=None)
    def get_reserve_zone_prices_designations_real_time_5_min_final(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        """Return final five-minute reserve zone requirements, prices, and designations

        Published and sometimes updated in the days following the operating day.

        Args:
            date: Date to query. Supports "latest" and "today"
            end: End date for date range queries
            verbose: Enable verbose logging

        Returns:
            DataFrame with columns: Interval Start, Interval End, Reserve Zone ID,
            Reserve Zone Name, Ten Min Spin Requirement, Ten Min Requirement,
            Total Requirement, TMSR Designated MW, TMNSR Designated MW,
            TMOR Designated MW, TMSR Clearing Price, TMR Clearing Price,
            Total Reserve Clearing Price
        """
        if date == "latest":
            df = self.get_reserve_zone_prices_designations_real_time_5_min_final(
                "today",
                verbose=verbose,
            )
            if df.is_empty():
                df = self.get_reserve_zone_prices_designations_real_time_5_min_final(
                    pd.Timestamp.now(tz=self.default_timezone).normalize()
                    - pd.Timedelta(days=1),
                    verbose=verbose,
                )
                if df.is_empty():
                    df = (
                        self.get_reserve_zone_prices_designations_real_time_5_min_final(
                            pd.Timestamp.now(tz=self.default_timezone).normalize()
                            - pd.Timedelta(days=2),
                            verbose=verbose,
                        )
                    )

            return df

        date_str = date.strftime("%Y%m%d")

        # If end is provided, query the range; otherwise query single day
        if end is not None:
            end_str = end.strftime("%Y%m%d")
            url = f"https://www.iso-ne.com/transform/csv/fiveminreserveprice?type=final&start={date_str}&end={end_str}"
        else:
            url = f"https://www.iso-ne.com/transform/csv/fiveminreserveprice?type=final&start={date_str}&end={date_str}"

        df = _make_request(url, skiprows=[0, 1, 2, 3, 5], verbose=verbose)

        pl_df = pl.from_pandas(df)
        pl_df = pl_df.rename(
            {c: c.strip().replace('"', "") for c in pl_df.columns},
        )

        pl_df = pl_df.with_columns(
            pl.col("Local Time").str.to_datetime().alias("_naive_time"),
        )
        pl_df = utils.localize_ambiguous_infer_polars(
            pl_df,
            "_naive_time",
            self.default_timezone,
            group_cols=["Reserve Zone ID"],
        )
        pl_df = pl_df.with_columns(
            pl.col("_naive_time").alias("Interval Start"),
            (pl.col("_naive_time") + pl.duration(minutes=5)).alias("Interval End"),
        ).drop("_naive_time")

        pl_df = pl_df.rename(
            {
                "Ten-Minute Spinning Requirement": "Ten Min Spin Requirement",
                "Ten-Minute Requirement": "Ten Min Requirement",
                "Ten Minute Spinning Reserve Designated MW": "TMSR Designated MW",
                "Ten Minute Non Spinning Reserve Designated MW": "TMNSR Designated MW",
                "Thirty Minute OperatingReserve Designated MW": "TMOR Designated MW",
                "Ten-Minute Spinning Reserve Clearing Price": "TMSR Clearing Price",
                "Ten-Minute Reserve Clearing Price": "TMR Clearing Price",
            },
        )

        return pl_df.select(
            [
                "Interval Start",
                "Interval End",
                "Reserve Zone ID",
                "Reserve Zone Name",
                "Ten Min Spin Requirement",
                "Ten Min Requirement",
                "Total Requirement",
                "TMSR Designated MW",
                "TMNSR Designated MW",
                "TMOR Designated MW",
                "TMSR Clearing Price",
                "TMR Clearing Price",
                "Total Reserve Clearing Price",
            ],
        )


def _make_request(url, skiprows, verbose):
    attempt = 0
    while attempt < 3:
        with requests.Session() as s:
            # make first get request to get cookies set
            s.get(
                "https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/gen-fuel-mix",
            )

            # in testing, never takes more than 2 attempts
            msg = f"Loading data from {url}"
            log(msg, verbose)

            response = s.get(url)
            content_type = response.headers["Content-Type"]

            if response.status_code == 200 and content_type == "text/csv":
                break

            print(f"Attempt {attempt + 1} failed. Retrying...")
            attempt += 1

    if response.status_code != 200 or content_type != "text/csv":
        raise RuntimeError(
            f"Failed to get data from {url}. Check if ISONE is down and \
                try again later",
        )

    df = pd.read_csv(
        io.StringIO(response.content.decode("utf8")),
        skiprows=skiprows,
        skipfooter=1,
        engine="python",
    ).drop_duplicates()
    return df


def _make_wsclient_request(url, data, verbose=False):
    """Make request to ISO NE wsclient"""

    msg = f"Requesting data from {url}"
    log(msg, verbose)

    r = requests.post(
        "https://www.iso-ne.com/ws/wsclient",
        data=data,
    )

    if r.status_code != 200:
        raise RuntimeError(
            f"Failed to get data from {url}. Check if ISONE is down and \
                try again later",
        )

    return r.json()


if __name__ == "__main__":
    iso = ISONE()
    df = iso.get_fuel_mix("today", verbose=True)
