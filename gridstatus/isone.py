import io
import math
import re
from heapq import merge
from tabnanny import verbose

import pandas as pd
import requests

import gridstatus
from gridstatus import utils
from gridstatus.base import (
    FuelMix,
    GridStatus,
    InterconnectionQueueStatus,
    ISOBase,
    Markets,
    NotSupported,
)
from gridstatus.decorators import support_date_range


class ISONE(ISOBase):
    """ISO New England (ISONE)"""

    name = "ISO New England"
    iso_id = "isone"
    default_timezone = "US/Eastern"

    status_homepage = "https://www.iso-ne.com/markets-operations/system-forecast-status/current-system-status"
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

    def _get_latest_fuel_mix(self):
        data = _make_wsclient_request(
            url="https://www.iso-ne.com/ws/wsclient",
            data={"_nstmp_requestType": "fuelmix"},
        )
        mix_df = pd.DataFrame(data[0]["data"]["GenFuelMixes"]["GenFuelMix"])
        time = pd.Timestamp(
            mix_df["BeginDate"].max(),
            tz=self.default_timezone,
        )

        # todo has marginal flag
        mix_dict = mix_df.set_index("FuelCategory")["GenMw"].to_dict()

        return FuelMix(time, mix_dict, self.name)

    @support_date_range(frequency="1D")
    def get_fuel_mix(self, date, end=None, verbose=False):
        """Return fuel mix at a previous date

        Provided at frequent, but irregular intervals by ISONE
        """
        if date == "latest":
            return self._get_latest_fuel_mix()

        # todo should getting day today use the latest endpoint?

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

        mix_df = mix_df.rename(columns={"Date": "Time"})

        mix_df = mix_df.fillna(0)

        return mix_df

    @support_date_range(frequency="1D")
    def get_load(self, date, verbose=False):
        """Return load at a previous date in 5 minute intervals"""
        # todo document the earliest supported date
        # supports a start and end date
        if date == "latest":
            return self._latest_from_today(self.get_load)

        date_str = date.strftime("%Y%m%d")
        url = f"https://www.iso-ne.com/transform/csv/fiveminutesystemload?start={date_str}&end={date_str}"
        data = _make_request(url, skiprows=[0, 1, 2, 3, 5], verbose=verbose)

        data["Date/Time"] = pd.to_datetime(data["Date/Time"]).dt.tz_localize(
            self.default_timezone,
            ambiguous="infer",
        )

        df = data[["Date/Time", "Native Load"]].rename(
            columns={"Date/Time": "Time", "Native Load": "Load"},
        )

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

        return df

    def _get_latest_lmp(self, market: str, locations: list = None, verbose=False):
        """
        Find Node ID mapping: https://www.iso-ne.com/markets-operations/settlements/pricing-node-tables/
        """
        if locations is None:
            locations = "ALL"
        market = Markets(market)
        if market == Markets.REAL_TIME_5_MIN:
            url = "https://www.iso-ne.com/transform/csv/fiveminlmp/current?type=prelim"
            data = _make_request(url, skiprows=[0, 1, 2, 4], verbose=verbose)
        elif market == Markets.REAL_TIME_HOURLY:
            url = "https://www.iso-ne.com/transform/csv/hourlylmp/current?type=prelim&market=rt"
            data = _make_request(url, skiprows=[0, 1, 2, 4], verbose=verbose)

            # todo does this handle single digital hours?
            data["Local Time"] = (
                data["Local Date"]
                + " "
                + data["Local Time"].astype(str).str.zfill(2)
                + ":00"
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
        """Find Node ID mapping: https://www.iso-ne.com/markets-operations/settlements/pricing-node-tables/"""

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
        market = Markets(market)
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
                print("Loading interval {}".format(interval))
                u = f"https://www.iso-ne.com/static-transform/csv/histRpts/5min-rt-prelim/lmp_5min_{date_str}_{interval}.csv"
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
                url = "https://www.iso-ne.com/transform/csv/fiveminlmp/currentrollinginterval"
                print("Loading current interval")
                # this request is very very slow for some reason. I suspect because the server is making the response dynamically
                data_current = _make_request(
                    url,
                    skiprows=[0, 1, 2, 4],
                    verbose=verbose,
                )
                data_current = data_current[
                    data_current["Local Time"] > data["Local Time"].max()
                ]
                data = pd.concat([data, data_current])

        elif market == Markets.REAL_TIME_HOURLY:
            if date.date() < now.date():
                url = f"https://www.iso-ne.com/static-transform/csv/histRpts/rt-lmp/lmp_rt_prelim_{date_str}.csv"
                data = _make_request(
                    url,
                    skiprows=[0, 1, 2, 3, 5],
                    verbose=verbose,
                )
                # todo document hour starting vs ending
                # for DST end transitions they use 02X to represent repeated 1am hour
                data["Hour Ending"] = (
                    data["Hour Ending"]
                    .replace(
                        "02X",
                        "02",
                    )
                    .astype(int)
                )
                data["Local Time"] = (
                    data["Date"]
                    + " "
                    + (data["Hour Ending"] - 1).astype(str).str.zfill(2)
                    + ":00"
                )
            else:
                raise RuntimeError(
                    "Today not supported for hourly lmp. Try latest",
                )

        elif market == Markets.DAY_AHEAD_HOURLY:
            url = f"https://www.iso-ne.com/static-transform/csv/histRpts/da-lmp/WW_DALMP_ISO_{date_str}.csv"
            data = _make_request(
                url,
                skiprows=[0, 1, 2, 3, 5],
                verbose=verbose,
            )
            # todo document hour starting vs ending

            # for DST end transitions they use 02X to represent repeated 1am hour
            data["Hour Ending"] = (
                data["Hour Ending"]
                .replace(
                    "02X",
                    "02",
                )
                .astype(int)
            )

            data["Local Time"] = (
                data["Date"]
                + " "
                + (data["Hour Ending"] - 1).astype(str).str.zfill(2)
                + ":00"
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

        location_groupby = (
            "Location Id" if "Location Id" in data.columns else "Location"
        )
        data["Time"] = data.groupby(location_groupby)["Time"].transform(
            lambda x, timezone=timezone: pd.to_datetime(x).dt.tz_localize(
                timezone,
                ambiguous="infer",
            ),
        )

        # handle missing location information for some markets
        if market != Markets.DAY_AHEAD_HOURLY:
            day_ahead = self.get_lmp(
                date="today",
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
            pd.DataFrame -- interconnection queue

        """
        # not sure what the reportdate value is. it is hardcode into the javascript to add and doesnt work without
        url = "https://irtt.iso-ne.com/reports/exportpublicqueue?ReportDate=638005248000000000&Status=&Jurisdiction="

        if verbose:
            print("Loading interconnection queue from {}".format(url))

        r = requests.get(url)
        queue = pd.read_excel(io.BytesIO(r.content), skiprows=4)

        # only keep generator interconnection requests
        queue = queue[queue["Type"] == "G"]

        queue["Status"] = (
            queue["W/ D Date"]
            .isna()
            .map(
                {
                    True: InterconnectionQueueStatus.WITHDRAWN.value,
                    False: InterconnectionQueueStatus.ACTIVE.value,
                },
            )
        )

        queue["Proposed Completion Date"] = queue["Sync Date"]

        rename = {
            "Position": "Queue ID",
            "Alternative Name": "Project Name",
            "Fuel Type": "Generation Type",
            "Requested": "Queue Date",
            "County": "County",
            "State": "State",
            "Status": "Status",
            "Interconnection Location": "Interconnection Location",
            "W/ D Date": "Withdrawn Date",
            "Net MW": "Capacity (MW)",
            "Summer MW": "Summer Capacity (MW)",
            "Winter MW": "Winter Capacity (MW)",
            "TO Report": "Transmission Owner",
        }

        # todo: there are a few columns being parsed as "unamed" that aren't being included but should
        extra_columns = [
            "Updated",
            "Unit",
            "Op Date",
            "Sync Date",
            "Serv",
            "SIS Complete",
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
            "Actual Completion Date",  # because there are only activate and withdrawn projects
            "Withdrawal Comment",
        ]

        queue = utils.format_interconnection_df(
            queue=queue,
            rename=rename,
            extra=extra_columns,
            missing=missing,
        )

        queue = queue.sort_values("Queue ID").reset_index(drop=True)

        return queue


def _make_request(url, skiprows, verbose):
    with requests.Session() as s:
        # in testing, never takes more than 2 attempts
        attempt = 0
        while attempt < 3:
            # make first get request to get cookies set
            r1 = s.get(
                "https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/gen-fuel-mix",
            )

            if verbose:
                print("Loading data from {}".format(url))

            r2 = s.get(url)

            if r2.status_code == 200:
                break

            print("Attempt {} failed. Retrying...".format(attempt + 1))
            attempt += 1

        if r2.status_code != 200:
            raise RuntimeError(
                "Failed to get data from {}. Check if ISONE is down and try again later".format(
                    url,
                ),
            )

        df = pd.read_csv(
            io.StringIO(r2.content.decode("utf8")),
            skiprows=skiprows,
            skipfooter=1,
            engine="python",
        )
        return df


def _make_wsclient_request(url, data, verbose=False):
    """Make request to ISO NE wsclient"""
    if verbose:
        print("Requesting data from {}".format(url))

    r = requests.post(
        "https://www.iso-ne.com/ws/wsclient",
        data=data,
    )

    if r.status_code != 200:
        raise RuntimeError(
            "Failed to get data from {}. Check if ISONE is down and try again later".format(
                url,
            ),
        )

    return r.json()


if __name__ == "__main__":
    iso = ISONE()
    df = iso.get_fuel_mix("today", verbose=True)
