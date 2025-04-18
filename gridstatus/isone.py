import io
from typing import BinaryIO

import pandas as pd
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
            return (
                self.get_fuel_mix("today", verbose=verbose)
                .tail(1)
                .reset_index(drop=True)
            )

        url = "https://www.iso-ne.com/transform/csv/genfuelmix?start=" + date.strftime(
            "%Y%m%d",
        )

        df = _make_request(url, skiprows=[0, 1, 2, 3, 5], verbose=verbose)

        df["Date"] = pd.to_datetime(df["Date"] + " " + df["Time"])

        # groupby FuelCategory to make it possible to infer DST changes
        df["Date"] = df.groupby("Fuel Category", group_keys=False)["Date"].apply(
            lambda x: x.dt.tz_localize(
                self.default_timezone,
                ambiguous="infer",
            ),
        )

        mix_df = df.pivot_table(
            index="Date",
            columns="Fuel Category",
            values="Gen Mw",
            aggfunc="first",
        ).reset_index()
        mix_df.columns.name = None

        # assume instant in time, unclear if this is correct
        mix_df = mix_df.rename(columns={"Date": "Time"})

        mix_df = mix_df.fillna(0)

        # move time columns front
        mix_df = utils.move_cols_to_front(
            mix_df,
            ["Time"],
        )

        return mix_df

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

        data["Date/Time"] = pd.to_datetime(data["Date/Time"]).dt.tz_localize(
            self.default_timezone,
            ambiguous="infer",
        )

        # todo what is the difference between Native Load and Asset Related Load?
        df = data[["Date/Time", "Native Load"]].rename(
            columns={"Date/Time": "Time", "Native Load": "Load"},
        )

        df["Interval Start"] = df["Time"]
        df["Interval End"] = df["Time"] + pd.Timedelta(minutes=5)

        df = df[["Time", "Interval Start", "Interval End", "Load"]]

        return df

    @support_date_range(frequency="DAY_START")
    def get_btm_solar(self, date, end=None, verbose=False):
        """Return BTM solar at a previous date in 5 minute intervals"""
        df = self._get_system_load(
            date,
            end=date,
            series="actual",
            verbose=verbose,
        )

        df["BTM Solar"] = df["NativeLoadBtmPv"] - df["Load"]

        df["Interval Start"] = df["Time"]
        df["Interval End"] = df["Time"] + pd.Timedelta(minutes=5)

        return df[["Time", "Interval Start", "Interval End", "BTM Solar"]]

    @support_date_range(frequency="DAY_START")
    def get_load_forecast(self, date, end=None, verbose=False):
        """Return forecast at a previous date"""

        df = self._get_system_load(
            date,
            end=date + pd.Timedelta(days=1),
            series="forecast",
            verbose=verbose,
        )

        df["Interval Start"] = df["Time"]
        df["Interval End"] = df["Time"] + pd.Timedelta(hours=1)

        df = df[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Forecast Time",
                "Load Forecast",
            ]
        ]

        return df

    def get_solar_forecast(self, date, end=None, verbose=False):
        """Return solar forecast published on a specific date

        Forecast is published for 7 days and generated daily by 10 am.
        https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/seven-day-solar-power-forecast
        """
        return (
            self._get_solar_or_wind_forecast(
                date,
                end,
                resource_type="Solar",
                verbose=verbose,
            )
            .reset_index(drop=True)
            .sort_values(["Interval Start", "Publish Time"])
        )

    def get_wind_forecast(self, date, end=None, verbose=False):
        """Return wind forecast published on a specific date

        Forecast is published for 7 days and generated daily by 10 am.
        https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/seven-day-wind-power-forecast
        """
        return (
            self._get_solar_or_wind_forecast(
                date,
                end,
                resource_type="Wind",
                verbose=verbose,
            )
            .reset_index(drop=True)
            .sort_values(["Interval Start", "Publish Time"])
        )

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

        data = df.melt(
            id_vars=["Hour Ending"],
            var_name="Date",
            value_name=value_name,
        ).dropna(subset=[value_name])

        data = self._create_interval_start_from_hour_start(data)

        data["Interval Start"] = data["Interval Start"].dt.tz_localize(
            self.default_timezone,
            ambiguous="infer",
            nonexistent="NaT",
        )

        # Handle start of DST since the hour ending in the raw data does not exist
        if data["Interval Start"].isna().any():
            hour_start = data.loc[data["Interval Start"].isna(), "Hour Start"] - 1

            data.loc[data["Interval Start"].isna(), "Interval Start"] = (
                pd.to_datetime(data.loc[data["Interval Start"].isna(), "Date"])
                + hour_start.astype("timedelta64[h]")
            ).dt.tz_localize(
                self.default_timezone,
            )

        data["Interval End"] = data["Interval Start"] + pd.Timedelta(hours=1)

        # Website says report is generally available by 10 am.
        report_datetime = date.normalize() + pd.Timedelta(hours=10)
        data["Publish Time"] = report_datetime

        data = utils.move_cols_to_front(
            data,
            ["Interval Start", "Interval End", "Publish Time", value_name],
        ).drop(columns=["Date", "Hour Start", "Hour Ending"])

        data[value_name] = data[value_name].astype(float)

        return data

    def _get_latest_lmp(self, market: str, locations: list = None, verbose=False):
        """
        Find Node ID mapping: https://www.iso-ne.com/markets-operations/settlements/pricing-node-tables/
        """  # noqa
        if locations is None:
            locations = "ALL"

        if market == Markets.REAL_TIME_5_MIN:
            url = "https://www.iso-ne.com/transform/csv/fiveminlmp/current?type=prelim"  # noqa
            data = _make_request(url, skiprows=[0, 1, 2, 4], verbose=verbose)
            data.rename(
                columns={
                    "Local Time": "Interval Start",
                },
                inplace=True,
            )

        elif market == Markets.REAL_TIME_HOURLY:
            url = "https://www.iso-ne.com/transform/csv/hourlylmp/current?type=prelim&market=rt"  # noqa
            data = _make_request(url, skiprows=[0, 1, 2, 4], verbose=verbose)

            # reformat this data so it looks like other endpoints
            # this way it works with process_lmp below
            data.rename(
                columns={
                    "Local Date": "Date",
                    "Local Time": "Hour Ending",
                },
                inplace=True,
            )

            # data["Hour Ending"] = data["Hour Ending"].astype(str).str.zfill(2)

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
    def get_lmp(
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
                # Use a try and except in case the data for previous intervals is not
                # published yet.
                try:
                    dfs.append(
                        pd.read_csv(
                            u,
                            skiprows=[0, 1, 2, 3, 5],
                            skipfooter=1,
                            engine="python",
                        ),
                    )
                except Exception as e:
                    log(f"Failed to load {u} with {e}", verbose=verbose)

            data_intervals = None

            if dfs:
                data_intervals = pd.concat(dfs)
                data_intervals["Local Time"] = pd.to_datetime(
                    date.strftime("%Y-%m-%d") + " " + data_intervals["Local Time"],
                )

            if querying_for_today:
                url = "https://www.iso-ne.com/transform/csv/fiveminlmp/currentrollinginterval"  # noqa
                msg = "Loading current interval"
                log(msg, verbose=verbose)
                # this request is very very slow for some reason.
                # I suspect b/c the server is making the response dynamically
                data_current = _make_request(
                    url,
                    skiprows=[0, 1, 2, 4],
                    verbose=verbose,
                )

                data_current["Local Time"] = pd.to_datetime(data_current["Local Time"])

                if data_intervals is not None:
                    data_current = data_current[
                        data_current["Local Time"] > data_intervals["Local Time"].max()
                    ]

                    data = pd.concat([data_intervals, data_current])
                else:
                    # Only keep data from today
                    data_current = data_current[
                        data_current["Local Time"].dt.date == now.date()
                    ]
                    data = data_current.copy()
            else:
                data = data_intervals.copy()

            data = data.rename(columns={"Local Time": "Interval Start"})

        elif market == Markets.REAL_TIME_HOURLY:
            if date.date() > now.date():
                raise RuntimeError(
                    f"date {date.date()} is in the future and cannot be used to query real-time data",
                )

            url = f"https://www.iso-ne.com/static-transform/csv/histRpts/rt-lmp/lmp_rt_prelim_{date_str}.csv"  # noqa
            data = _make_request(
                url,
                skiprows=[0, 1, 2, 3, 5],
                verbose=verbose,
            )

        elif market == Markets.DAY_AHEAD_HOURLY:
            url = f"https://www.iso-ne.com/static-transform/csv/histRpts/da-lmp/WW_DALMP_ISO_{date_str}.csv"  # noqa
            data = _make_request(
                url,
                skiprows=[0, 1, 2, 3, 5],
                verbose=verbose,
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

    def _process_lmp(self, data, market, timezone, locations, include_id=False):
        # each market returns a slight different set of columns
        # real time 5 minute has "Location ID"
        # real time hourly has "Location" that represent location name
        # day ahead hourly has "Location ID" and "Location Name

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

        data.rename(columns=rename, inplace=True)

        data["Market"] = market.value
        interval = pd.Timedelta(hours=1)
        if market == Markets.REAL_TIME_5_MIN:
            interval = pd.Timedelta(minutes=5)

        if "Hour Ending" in data.columns:
            data = self._create_interval_start_from_hour_start(data)

        def handle_date_time(s):
            return pd.to_datetime(s).dt.tz_localize(
                timezone,
                ambiguous="infer",
            )

        # Location seems to be more unique than Location ID refer to #171
        location_groupby = "Location" if "Location" in data.columns else "Location Id"
        # groupby location so that hours are increasing monotonically and can infer dst
        data["Interval Start"] = data.groupby(location_groupby)[
            "Interval Start"
        ].transform(handle_date_time)

        data["Interval End"] = data["Interval Start"] + interval
        data["Time"] = data["Interval Start"]

        # handle missing location information for some markets
        if market != Markets.DAY_AHEAD_HOURLY:
            day_ahead = self.get_lmp(
                # query for same day in case it matters
                date=data["Interval Start"].min().date(),
                market=Markets.DAY_AHEAD_HOURLY,
                locations=locations,
                include_id=True,
            )
            location_mapping = day_ahead.drop_duplicates("Location Id")[
                ["Location", "Location Id", "Location Type"]
            ]

            if "Location Id" in data.columns:
                data = data.merge(
                    location_mapping,
                    how="left",
                    on="Location Id",
                )
            elif "Location" in data.columns:
                data = data.merge(location_mapping, how="left", on="Location")

        data = data[
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
            ]
        ]

        if not include_id:
            data = data.drop(columns=["Location Id"])

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
        queue = pd.read_html(raw_data, attrs={"id": "publicqueue"})[0]

        # only keep generator interconnection requests
        queue["Type"] = queue["Type"].map(
            {
                "G": "Generation",
                "ETU": "Elective Transmission Upgrade",
                "TS": "Transmission Service",
            },
        )

        queue["Status"] = queue["Status"].map(
            {
                "W": InterconnectionQueueStatus.WITHDRAWN.value,
                "A": InterconnectionQueueStatus.ACTIVE.value,
                "C": InterconnectionQueueStatus.COMPLETED.value,
            },
        )

        queue["Proposed Completion Date"] = queue["Sync Date"]

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

        queue = queue.sort_values(
            "Queue ID",
            ascending=False,
        ).reset_index(drop=True)

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

        data = pd.DataFrame(raw_data[0]["data"][series])

        # must convert this way rather than use pd.to_datetime
        # to handle DST transitions
        data["BeginDate"] = data["BeginDate"].apply(
            lambda x: pd.Timestamp(x).tz_convert(ISONE.default_timezone),
        )

        # for times earlier this creation date is after the forecasted interval
        # for all historical data
        if "CreationDate" in data.columns:
            data["CreationDate"] = data["CreationDate"].apply(
                lambda x: pd.Timestamp(x).tz_convert(ISONE.default_timezone),
            )
        if series == "actual":
            mw_rename = "Load"
        elif series == "forecast":
            mw_rename = "Load Forecast"
        else:
            raise ValueError(f"Unrecognized series: {series}")

        data = data.rename(
            columns={
                "BeginDate": "Time",
                "Mw": mw_rename,
                "CreationDate": "Forecast Time",
            },
        )

        return data

    def _create_interval_start_from_hour_start(self, data):
        # for DST end transitions isone uses 02X to represent repeated 1am hour
        data["Hour Start"] = (
            data["Hour Ending"]
            .replace(
                "02X",
                "02",
            )
            .astype(int)
            - 1
        )

        data["Interval Start"] = pd.to_datetime(data["Date"]) + data[
            "Hour Start"
        ].astype(
            "timedelta64[h]",
        )

        return data

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
