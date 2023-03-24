import io
import math

import pandas as pd
import requests

from gridstatus import utils
from gridstatus.base import (
    GridStatus,
    InterconnectionQueueStatus,
    ISOBase,
    Markets,
    NotSupported,
)
from gridstatus.decorators import support_date_range
from gridstatus.lmp_config import lmp_config
from gridstatus.logging import log


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

    @support_date_range(frequency="1D")
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

        # assume interval end. unclear based on data
        mix_df = mix_df.rename(columns={"Date": "Interval End"})

        mix_df = mix_df.fillna(0)

        mix_df["Interval Start"] = (
            mix_df["Interval End"] - mix_df["Interval End"].diff()
        )

        # add midnight to first row of Interval Start
        mix_df.loc[0, "Interval Start"] = mix_df["Interval Start"].min().normalize()

        # if historical data, add row at end to go to midnight of next day
        # todo manually verified works, but add test for this
        if not utils.is_today(date, self.default_timezone):
            new_row = pd.DataFrame(
                {
                    "Interval Start": [mix_df["Interval End"].max()],
                    "Interval End": [
                        mix_df["Interval End"].max().normalize() + pd.Timedelta(days=1),
                    ],
                },
            )
            mix_df = pd.concat([mix_df, new_row], ignore_index=True).ffill()

        mix_df["Time"] = mix_df["Interval Start"]

        # move time columns front
        mix_df = utils.move_cols_to_front(
            mix_df,
            ["Time", "Interval Start", "Interval End"],
        )

        return mix_df

    @support_date_range(frequency="1D")
    def get_load(self, date, verbose=False):
        """Return load at a previous date in 5 minute intervals"""
        # todo document the earliest supported date
        # supports a start and end date
        if date == "latest":
            return self._latest_from_today(self.get_load)

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

    @support_date_range(frequency="1D")
    def get_load_forecast(self, date, end=None, verbose=False):
        """Return forecast at a previous date"""
        start_str = date.strftime("%m/%d/%Y")
        end_str = (date + pd.Timedelta(days=1)).strftime("%m/%d/%Y")
        data = {
            "_nstmp_startDate": start_str,
            "_nstmp_endDate": end_str,
            "_nstmp_twodays": True,
            "_nstmp_twodaysCheckbox": False,
            "_nstmp_requestType": "systemload",
            "_nstmp_forecast": True,
            "_nstmp_actual": False,
            "_nstmp_cleared": False,
            "_nstmp_priorDay": False,
            "_nstmp_inclPumpLoad": True,
            "_nstmp_inclBtmPv": True,
        }

        data = _make_wsclient_request(
            url="https://www.iso-ne.com/ws/wsclient",
            data=data,
            verbose=verbose,
        )

        data = pd.DataFrame(data[0]["data"]["forecast"])

        # must convert this way rather than use pd.to_datetime
        # to handle DST transitions
        data["BeginDate"] = data["BeginDate"].apply(
            lambda x: pd.Timestamp(x).tz_convert(ISONE.default_timezone),
        )

        data["CreationDate"] = data["BeginDate"].apply(
            lambda x: pd.Timestamp(x).tz_convert(ISONE.default_timezone),
        )

        df = data[["CreationDate", "BeginDate", "Mw"]].rename(
            columns={
                "CreationDate": "Forecast Time",
                "BeginDate": "Time",
                "Mw": "Load Forecast",
            },
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
    @support_date_range(frequency="1D")
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
            # todo handle intervals for current day
            intervals = ["00-04", "04-08", "08-12", "12-16", "16-20", "20-24"]

            # optimze for current day
            if now.date() == date.date():
                hour = now.hour
                # select completed 4 hour intervals based on current hour
                intervals = intervals[: math.ceil((hour + 1) / 4) - 1]

            dfs = []
            for interval in intervals:
                msg = "Loading interval {}".format(interval)
                log(msg, verbose=verbose)
                u = f"https://www.iso-ne.com/static-transform/csv/histRpts/5min-rt-prelim/lmp_5min_{date_str}_{interval}.csv"  # noqa
                dfs.append(
                    pd.read_csv(
                        u,
                        skiprows=[0, 1, 2, 3, 5],
                        skipfooter=1,
                        engine="python",
                    ),
                )

            data = pd.concat(dfs)

            data["Local Time"] = (
                date.strftime(
                    "%Y-%m-%d",
                )
                + " "
                + data["Local Time"]
            )

            # add current interval
            if now.date() == date.date():
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
                data_current = data_current[
                    data_current["Local Time"] > data["Local Time"].max()
                ]
                data = pd.concat([data, data_current])

            data.rename(
                columns={
                    "Local Time": "Interval Start",
                },
                inplace=True,
            )

        elif market == Markets.REAL_TIME_HOURLY:
            if date.date() < now.date():
                url = f"https://www.iso-ne.com/static-transform/csv/histRpts/rt-lmp/lmp_rt_prelim_{date_str}.csv"  # noqa
                data = _make_request(
                    url,
                    skiprows=[0, 1, 2, 3, 5],
                    verbose=verbose,
                )
            else:
                # iso only publishes rolling 3 hours of data for current
                # day real time hourly. idk why
                raise RuntimeError(
                    "Today not supported for hourly lmp. Try latest",
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
            ].astype("timedelta64[h]")

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

    def get_interconnection_queue(self, verbose=False):
        """Get the interconnection queue. Contains active and withdrawm applications.

        More information: https://www.iso-ne.com/system-planning/interconnection-service/interconnection-request-queue/


        Returns:
            pandas.DataFrame: interconnection queue

        """  # noqa

        # determine report date from homepage

        msg = f"Loading queue {self.interconnection_homepage}"
        log(msg, verbose)

        r = requests.get("https://irtt.iso-ne.com/reports/external")
        queue = pd.read_html(r.text, attrs={"id": "publicqueue"})[0]

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
            "FS",
            "SIS",
            "OS",
            "FAC",
            "IA",
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

            print(
                f"Attempt {attempt+1} failed. Retrying...",
            )
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
