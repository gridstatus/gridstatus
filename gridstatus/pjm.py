import math
import os
import warnings
from datetime import datetime
from typing import BinaryIO

import pandas as pd
import pytz
import requests
import tqdm

from gridstatus import utils
from gridstatus.base import ISOBase, Markets, NoDataFoundException, NotSupported
from gridstatus.decorators import (
    _get_pjm_archive_date,
    pjm_update_dates,
    support_date_range,
)
from gridstatus.gs_logging import logger
from gridstatus.lmp_config import lmp_config
from gridstatus.pjm_constants import (
    DEFAULT_RETRIES,
    HUB_NODE_IDS,
    LOCATION_TYPES,
    PRICE_NODE_IDS,
    ZONE_NODE_IDS,
)


class PJM(ISOBase):
    """PJM"""

    name = "PJM"
    iso_id = "pjm"
    default_timezone = "US/Eastern"

    interconnection_homepage = (
        "https://www.pjm.com/planning/service-requests/services-request-status"
    )

    location_types = LOCATION_TYPES
    hub_node_ids = HUB_NODE_IDS
    zone_node_ids = ZONE_NODE_IDS
    price_node_ids = PRICE_NODE_IDS
    markets = [
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_HOURLY,
        Markets.DAY_AHEAD_HOURLY,
    ]

    locale_abbreviated_to_full = {
        "PJM_RTO": "PJM RTO Reserve Zone",
        "MAD": "Mid-Atlantic/Dominion Reserve Subzone",
    }

    service_type_abbreviated_to_full = {
        "30MIN": "Thirty Minutes Reserve",
        "PR": "Primary Reserve",
        "REG": "Regulation",
        "SR": "Synchronized Reserve",
    }

    load_forecast_endpoint_name = "load_frcstd_7_day"
    load_forecast_historical_endpoint_name = "load_frcstd_hist"
    load_forecast_5_min_endpoint_name = "very_short_load_frcst"

    def __init__(
        self,
        api_key: str | None = None,
        retries: int = DEFAULT_RETRIES,
    ) -> None:
        """
        Arguments:
            api_key (str, optional): PJM API key. Alternatively, can be set
                in PJM_API_KEY environment variable. Register for an API key
                at https://www.pjm.com/
        """
        super().__init__()
        self.retries = retries
        self.api_key = api_key or os.getenv("PJM_API_KEY")

        if not self.api_key:
            raise ValueError("api_key must be provided or set in PJM_API_KEY env var")

    @support_date_range(frequency="365D")
    def get_fuel_mix(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get fuel mix for a date or date range  in hourly intervals"""

        if date == "latest":
            mix = self.get_fuel_mix("today")
            return mix.tail(1).reset_index(drop=True)

        # earliest date available appears to be 1/1/2016
        data = {
            "fields": "datetime_beginning_utc,fuel_type,is_renewable,mw",
            "sort": "datetime_beginning_utc",
            "order": "Asc",
        }

        mix_df = self._get_pjm_json(
            "gen_by_fuel",
            start=date,
            end=end,
            params=data,
            interval_duration_min=60,
        )

        mix_df = mix_df.pivot_table(
            index=["Time", "Interval Start", "Interval End"],
            columns="fuel_type",
            values="mw",
            aggfunc="first",
        ).reset_index()

        mix_df.columns.name = None

        return mix_df

    @support_date_range(frequency="30D")
    def get_load(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns load at a previous date at 5 minute intervals

        Arguments:
            date (datetime.date, str): date to get load for. must be in last 30 days

        Returns:
            pd.DataFrame: Load data time series. Columns: Time, Load, and all areas

            * Load columns represent PJM-wide load
            * Returns data for the following areas: AE, AEP, APS, ATSI,
            BC, COMED, DAYTON, DEOK, DOM, DPL, DUQ, EKPC, JC,
            ME, PE, PEP, PJM MID ATLANTIC REGION, PJM RTO,
            PJM SOUTHERN REGION, PJM WESTERN REGION, PL, PN, PS, RECO
        """

        if date == "latest":
            return self.get_load("today", verbose=verbose)

        # more hourly historical load here: https://dataminer2.pjm.com/feed/hrl_load_metered/definition

        # todo can support a load area
        data = {
            "order": "Asc",
            "sort": "datetime_beginning_utc",
            "isActiveMetadata": "true",
            "fields": "area,datetime_beginning_utc,instantaneous_load",
        }
        load = self._get_pjm_json(
            "inst_load",
            # one minute earlier to hand off by a few seconds seconds
            start=date - pd.Timedelta(minutes=1),
            end=end,
            params=data,
            verbose=verbose,
        )

        # round to nearest minute before the pivot
        # need to round in utc time
        load["Interval Start"] = (
            load["Interval Start"]
            .dt.tz_convert("UTC")
            .dt.round("1min")
            .dt.tz_convert(self.default_timezone)
        )
        load["Time"] = load["Interval Start"]

        # pivot on area
        load = load.pivot_table(
            index=["Time", "Interval Start"],
            columns="area",
            values="instantaneous_load",
            aggfunc="first",
        ).reset_index()

        load["Interval End"] = load["Interval Start"] + pd.Timedelta(minutes=5)

        load.columns.name = None

        # set Load column name to match return column of other ISOs
        load["Load"] = load["PJM RTO"]

        load = utils.move_cols_to_front(
            load,
            ["Time", "Interval Start", "Interval End", "Load"],
        )

        return load

    @support_date_range(frequency=None)
    def get_load_forecast(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Load forecast made today extending for six days in hourly intervals.

        Today's forecast updates every every half hour on the quarter E.g. 1:15 and 1:45
        """
        if date == "latest":
            return self.get_load_forecast("today", verbose=verbose)

        if not utils.is_today(date, tz=self.default_timezone):
            raise NotSupported(
                "Only today's forecast is available through"
                " get_load_forecast. Try get_load_forecast_historical instead.",
            )

        params = {
            "fields": (
                "evaluated_at_datetime_utc,forecast_area,forecast_datetime_beginning_utc,forecast_datetime_ending_utc,forecast_area,forecast_load_mw"  # noqa: E501
            ),
        }

        filter_timestamp_name = "datetime_beginning"

        data = self._get_pjm_json(
            self.load_forecast_endpoint_name,
            start=None,
            end=end,
            params=params,
            verbose=verbose,
            filter_timestamp_name=filter_timestamp_name,
        )

        return self._handle_load_forecast(data)

    @support_date_range(frequency=None)
    def get_load_forecast_historical(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Historical load forecast in hourly intervals. Historical forecasts include all
        vintages of the forecast but has fewer regions than the current forecast.
        """
        # Historical data uses a different endpoint with slightly different fields.
        params = {
            "fields": (
                "evaluated_at_utc,forecast_area,forecast_hour_beginning_utc,forecast_area,forecast_load_mw"  # noqa: E501
            ),
        }

        filter_timestamp_name = "forecast_hour_beginning"

        if end:
            end = utils._handle_date(end, tz=self.default_timezone) + pd.DateOffset(
                days=1,
            )
        else:
            end = date + pd.DateOffset(days=1)

        data = self._get_pjm_json(
            self.load_forecast_historical_endpoint_name,
            start=date,
            end=end,
            params=params,
            verbose=verbose,
            filter_timestamp_name=filter_timestamp_name,
        )

        return self._handle_load_forecast(data)

    @support_date_range(frequency=None)
    def get_load_forecast_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Load forecast made today extending for 2 hours in 5 minute intervals.
        """
        if date == "latest":
            return self.get_load_forecast_5_min(
                "today",
                verbose=verbose,
            )

        params = {
            "fields": (
                "evaluated_at_utc,forecast_datetime_beginning_utc,forecast_datetime_ending_utc,forecast_area,forecast_load_mw"
            ),
        }

        filter_timestamp_name = "evaluated_at"

        data = self._get_pjm_json(
            self.load_forecast_5_min_endpoint_name,
            start=date,
            end=end,
            params=params,
            verbose=verbose,
            filter_timestamp_name=filter_timestamp_name,
        )

        return self._handle_load_forecast(data)

    def _handle_load_forecast(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.rename(
            columns={
                "evaluated_at_utc": "Publish Time",
                "evaluated_at_datetime_utc": "Publish Time",
                "forecast_load_mw": "Load Forecast",
                "forecast_datetime_beginning_utc": "Interval Start",
                "forecast_hour_beginning_utc": "Interval Start",
                "forecast_datetime_ending_utc": "Interval End",
                "forecast_area": "Forecast Area",
            },
        )

        data = data.pivot(
            columns="Forecast Area",
            values="Load Forecast",
            index=["Publish Time", "Interval Start"],
        ).reset_index()

        # Replace & with "" and / with _ in column names
        data.columns = data.columns.str.replace("&", "").str.replace("/", "_")

        data["Publish Time"] = pd.to_datetime(
            data["Publish Time"],
            utc=True,
        ).dt.tz_convert(
            self.default_timezone,
        )

        data["Interval Start"] = pd.to_datetime(
            data["Interval Start"],
            utc=True,
        ).dt.tz_convert(
            self.default_timezone,
        )

        # Only real-time data has Interval End
        if "Interval End" in data.columns:
            data["Interval End"] = pd.to_datetime(
                data["Interval End"],
                utc=True,
            ).dt.tz_convert(
                self.default_timezone,
            )
        else:
            data["Interval End"] = data["Interval Start"] + pd.Timedelta(hours=1)

        if "RTO" in data.columns:
            data["Load Forecast"] = data["RTO"]
        elif "RTO_COMBINED" in data.columns:
            data["Load Forecast"] = data["RTO_COMBINED"]

        data = utils.move_cols_to_front(
            data,
            ["Interval Start", "Interval End", "Publish Time", "Load Forecast"],
        )

        return data.sort_values(["Interval Start", "Publish Time"]).reset_index(
            drop=True,
        )

    def get_pnode_ids(self) -> pd.DataFrame:
        data = {
            "fields": "effective_date,pnode_id,pnode_name,pnode_subtype,pnode_type\
                ,termination_date,voltage_level,zone",
            "termination_date": "12/31/9999exact",
        }
        nodes = self._get_pjm_json("pnode", start=None, params=data)

        # only keep most recent effective date for each id
        # return sorted by pnode_id
        nodes = (
            nodes.sort_values("effective_date", ascending=False)
            .drop_duplicates(
                "pnode_id",
            )
            .sort_values("pnode_id")
            .reset_index(drop=True)
        )

        # NB: this is needed because rt_unverified_fivemin_lmps
        # doesn't have short name
        # so we need to extract it from full name
        # other LMP datasets have but do it this way
        # for consistent logic
        def extract_short_name(row: pd.Series) -> str:
            if row["voltage_level"] is None or pd.isna(row["voltage_level"]):
                return row["pnode_name"]
            else:
                # Find the index where voltage_level starts
                # and extract everything before it
                index = row["pnode_name"].find(row["voltage_level"])
                # if not found, return full name
                if index == -1:
                    return row["pnode_name"]
                return row["pnode_name"][:index].strip()

        nodes["pnode_short_name"] = nodes.apply(extract_short_name, axis=1)

        return nodes

    @lmp_config(
        supports={
            Markets.REAL_TIME_5_MIN: ["latest", "today", "historical"],
            Markets.REAL_TIME_HOURLY: ["today", "historical"],
            Markets.DAY_AHEAD_HOURLY: ["today", "historical"],
        },
    )
    @support_date_range(frequency="365D", update_dates=pjm_update_dates)
    def get_lmp(
        self,
        date: str | pd.Timestamp,
        market: str,
        end: str | pd.Timestamp | None = None,
        locations: str = "hubs",
        location_type: str | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns LMP at a previous date

        Notes:
            * If start date is prior to the PJM archive date, all data
            must be downloaded before location filtering can be performed
            due to limitations of PJM API. The archive date is
            186 days (~6 months) before today for the 5 minute real time
            market and 731 days (~2 years) before today for the Hourly
            Real Time and Day Ahead Hourly markets. Node type filter can be
            performed for Real Time Hourly and Day Ahead Hourly markets.

            * If location_type is provided, it is filtered after data
            is retrieved for Real Time 5 Minute market regardless of the
            date. This is due to PJM api limitations

            *  Return `Location Id`, `Location Name`, `Location Short Name`.

        Arguments:
            date (datetime.date, str): date to get LMPs for

            end (datetime.date, str): end date to get LMPs for

            market (str):  Supported Markets:
                REAL_TIME_5_MIN, REAL_TIME_HOURLY, DAY_AHEAD_HOURLY

            locations (list, optional):  list of pnodeid to get LMPs for.
                Defaults to "hubs". Use get_pnode_ids() to get
                a list of possible pnode ids. If "all", will
                return data from all p nodes (warning there are
                over 10,000 unique pnodes, so expect millions or billions of rows!)

            location_type (str, optional):  If specified,
                will only return data for nodes of this type.
                Defaults to None. Possible location types are: 'ZONE',
                'LOAD', 'GEN', 'AGGREGATE', 'INTERFACE', 'EXT',
                'HUB', 'EHV', 'TIE', 'RESIDUAL_METERED_EDC'.

        """
        if date == "latest":
            return self._latest_lmp_from_today(
                market=market,
                locations=locations,
                location_type=location_type,
                verbose=verbose,
            )

        if locations == "hubs":
            locations = self.hub_node_ids

        params = {}

        if market == Markets.REAL_TIME_5_MIN:
            market_endpoint = "rt_fivemin_hrl_lmps"
            market_type = "rt"
            interval_duration_min = 5
        elif market == Markets.REAL_TIME_HOURLY:
            # TODO implement location type filter
            market_endpoint = "rt_hrl_lmps"
            market_type = "rt"
            interval_duration_min = 60
        elif market == Markets.DAY_AHEAD_HOURLY:
            # TODO implement location type filter
            market_endpoint = "da_hrl_lmps"
            market_type = "da"
            interval_duration_min = 60
        else:
            raise ValueError(
                (
                    "market must be one of REAL_TIME_5_MIN, REAL_TIME_HOURLY,"
                    " DAY_AHEAD_HOURLY"
                ),
            )

        if location_type:
            location_type = location_type.upper()
            if location_type not in self.location_types:
                raise ValueError(
                    f"location_type must be one of {self.location_types}",
                )

            if market == Markets.REAL_TIME_5_MIN:
                warnings.warn(
                    (
                        "When using Real Time 5 Minute market, location_type filter"
                        " will happen after all data is downloaded"
                    ),
                )
            else:
                params["type"] = f"*{location_type}*"

            if locations is not None:
                locations = None

        if date >= _get_pjm_archive_date(market):
            # after archive date, filtering allowed
            params["fields"] = (
                f"congestion_price_{market_type},datetime_beginning_ept,datetime_beginning_utc,equipment,marginal_loss_price_{market_type},pnode_id,pnode_name,row_is_current,system_energy_price_{market_type},total_lmp_{market_type},type,version_nbr,voltage,zone",
            )

            if locations and locations != "ALL":
                params["pnode_id"] = ";".join(map(str, locations))

        elif locations is not None and locations != "ALL":
            warnings.warn(
                (
                    "Querying before archive date, so filtering by location will happen"
                    " after all data is downloaded"
                ),
            )

        # returns on the latest version of the data
        params["row_is_current"] = "TRUE"

        try:
            data = self._get_pjm_json(
                market_endpoint,
                start=date,
                end=end,
                params=params,
                verbose=verbose,
                interval_duration_min=interval_duration_min,
            )
        except NoDataFoundException as e:
            if "No data found" not in str(e):
                raise e

            if market_endpoint == "rt_fivemin_hrl_lmps":
                market_endpoint = "rt_unverified_fivemin_lmps"
                params["fields"] = (
                    "congestion_price_rt,datetime_beginning_ept,datetime_beginning_utc,marginal_loss_price_rt,occ_check,pnode_id,pnode_name,ref_caseid_used_multi_interval,total_lmp_rt,type"  # noqa: E501
                )
                # remove this field because it's not supported in this endpoint
                del params["row_is_current"]

            data = self._get_pjm_json(
                market_endpoint,
                start=date,
                end=end,
                params=params,
                verbose=verbose,
                interval_duration_min=interval_duration_min,
            )

            data["system_energy_price_rt"] = (
                data["total_lmp_rt"]
                - data["congestion_price_rt"]
                - data["marginal_loss_price_rt"]
            )

        # API cannot filter location type for rt 5 min
        data = data.rename(columns={"type": "Location Type"})
        if location_type and market == Markets.REAL_TIME_5_MIN:
            data = data[data["Location Type"] == location_type]

        if locations is not None and locations != "ALL":
            # make sure Location is defined
            data["Location"] = data["pnode_id"]
            data = utils.filter_lmp_locations(
                data,
                map(int, locations),
            )

        data = self._add_pnode_info_to_lmp_data(data)

        data = data.rename(
            columns={
                "pnode_id": "Location Id",
                "pnode_name": "Location Name",
                "pnode_short_name": "Location Short Name",
                f"total_lmp_{market_type}": "LMP",
                f"system_energy_price_{market_type}": "Energy",
                f"congestion_price_{market_type}": "Congestion",
                f"marginal_loss_price_{market_type}": "Loss",
            },
        )
        data["Market"] = market.value

        data = data[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Market",
                "Location Id",
                "Location Name",
                "Location Short Name",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
        ]

        data = data.sort_values("Interval Start")

        return data

    def _add_pnode_info_to_lmp_data(self, data: pd.DataFrame) -> pd.DataFrame:
        # the pnode_name in the lmp data isn't always full name
        # so, let drop it for now
        # will get full name by merge with pnode data later
        data = data.drop(columns=["pnode_name"])

        p_nodes = self.get_pnode_ids()[
            ["pnode_id", "pnode_name", "voltage_level", "pnode_short_name"]
        ]

        data = data.merge(p_nodes, on="pnode_id")

        return data

    @support_date_range(frequency="365D")
    def get_lmp_real_time_unverified_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        locations: str | None = None,
        location_type: str | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get real-time unverified hourly LMPs"""

        if date == "latest":
            date = "today"

        params = {
            "fields": "datetime_beginning_utc, datetime_beginning_ept, pnode_name, type, total_lmp_rt, congestion_price_rt, marginal_loss_price_rt",  # noqa: E501
        }
        if location_type:
            location_type = location_type.upper()
            if location_type not in self.location_types:
                raise ValueError(
                    f"location_type must be one of {self.location_types}",
                )
            params["type"] = f"{location_type}"

        data = self._get_pjm_json(
            "rt_unverified_hrl_lmps",
            start=date,
            end=end,
            params=params,
            interval_duration_min=60,
            verbose=verbose,
        )
        if locations == "hubs":
            locations = self.hub_node_ids
        elif locations == "zones":
            locations = self.zone_node_ids
        if locations is not None and locations != "ALL":
            data["Location"] = data["pnode_name"]
            data = utils.filter_lmp_locations(
                data,
                map(int, locations),
            )

        data["system_energy_price_rt"] = (
            data["total_lmp_rt"]
            - data["congestion_price_rt"]
            - data["marginal_loss_price_rt"]
        )

        df = data.rename(
            columns={
                "pnode_name": "Location",
                "type": "Location Type",
                "total_lmp_rt": "LMP",
                "system_energy_price_rt": "Energy",
                "congestion_price_rt": "Congestion",
                "marginal_loss_price_rt": "Loss",
            },
        )
        df = df.sort_values("Interval Start").reset_index(drop=True)

        return df[
            [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
        ]

    @support_date_range(frequency=None)
    def get_it_sced_lmp_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get 5 minute LMPs from the Integrated Forward Market (IFM)"""

        if date == "latest":
            return self.get_it_sced_lmp_5_min("today", verbose=verbose)

        params = {
            "fields": (
                "case_approval_datetime_utc,datetime_beginning_utc,itsced_lmp,marginal_congestion,marginal_loss,pnode_id,pnode_name"  # noqa: E501
            ),
        }

        df = self._get_pjm_json(
            "five_min_itsced_lmps",
            start=date,
            end=end,
            params=params,
            verbose=verbose,
            interval_duration_min=5,
        )

        df = self._add_pnode_info_to_lmp_data(df)

        df.columns = df.columns.map(lambda x: x.replace("_", " ").title())

        df = df.rename(
            columns={
                "Case Approval Datetime Utc": "Case Approval Time",
                "Itsced Lmp": "LMP",
                "Pnode Id": "Location Id",
                "Pnode Name": "Location Name",
                "Pnode Short Name": "Location Short Name",
                "Marginal Congestion": "Congestion",
                "Marginal Loss": "Loss",
            },
        )

        # LMP = Energy + Congestion + Loss so Energy = LMP - Congestion - Loss
        df["Energy"] = df["LMP"] - df["Congestion"] - df["Loss"]

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Case Approval Time",
                "Location Id",
                "Location Name",
                "Location Short Name",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
        ]

        df["Case Approval Time"] = pd.to_datetime(
            df["Case Approval Time"],
            utc=True,
        ).dt.tz_convert(self.default_timezone)

        return df

    @support_date_range(frequency=None)
    def get_settlements_verified_lmp_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = self._get_pjm_json(
            "rt_fivemin_mnt_lmps",
            start=date,
            params={
                "fields": "congestion_price_rt,datetime_beginning_utc,equipment,marginal_loss_price_rt,pnode_id,pnode_name,system_energy_price_rt,total_lmp_rt,type,voltage,zone",  # noqa: E501
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=5,
            verbose=verbose,
        )

        return self._handle_settlements_verified_lmp_5_min(df)

    def _handle_settlements_verified_lmp_5_min(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        rename = {
            "Interval Start": "Interval Start",
            "Interval End": "Interval End",
            "pnode_id": "Location Id",
            "pnode_name": "Location Name",
            "type": "Location Type",
            "voltage": "Voltage",
            "equipment": "Equipment",
            "zone": "Zone",
            "total_lmp_rt": "LMP",
            "system_energy_price_rt": "Energy",
            "congestion_price_rt": "Congestion",
            "marginal_loss_price_rt": "Loss",
        }

        data = data.rename(columns=rename)[rename.values()]

        for col in ["Location Type", "Zone"]:
            data[col] = data[col].astype("category")

        return data.sort_values(["Interval Start", "Location Name"])

    @support_date_range(frequency=None)
    def get_settlements_verified_lmp_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = self._get_pjm_json(
            "rt_da_monthly_lmps",
            start=date,
            params={
                "fields": "congestion_price_da,congestion_price_rt,datetime_beginning_utc,equipment,marginal_loss_price_da,marginal_loss_price_rt,pnode_id,pnode_name,system_energy_price_da,system_energy_price_rt,total_lmp_da,total_lmp_rt,type,voltage,zone",  # noqa: E501
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=60,
            verbose=verbose,
        )

        return self._handle_settlements_verified_lmp_hourly(df)

    def _handle_settlements_verified_lmp_hourly(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        rename = {
            "Interval Start": "Interval Start",
            "Interval End": "Interval End",
            "pnode_id": "Location Id",
            "pnode_name": "Location Name",
            "type": "Location Type",
            "voltage": "Voltage",
            "equipment": "Equipment",
            "zone": "Zone",
            "total_lmp_rt": "LMP RT",
            "system_energy_price_rt": "Energy RT",
            "congestion_price_rt": "Congestion RT",
            "marginal_loss_price_rt": "Loss RT",
            "total_lmp_da": "LMP DA",
            "system_energy_price_da": "Energy DA",
            "congestion_price_da": "Congestion DA",
            "marginal_loss_price_da": "Loss DA",
        }

        data = data.rename(columns=rename)[rename.values()]

        for col in ["Location Type", "Zone"]:
            data[col] = data[col].astype("category")

        return data.sort_values(["Interval Start", "Location Name"])

    def _get_pjm_json(
        self,
        endpoint: str,
        start: str | pd.Timestamp,
        params: dict,
        end: str | pd.Timestamp | None = None,
        start_row: int = 1,
        row_count: int = 50000,
        interval_duration_min: int | float | None = None,
        filter_timestamp_name: str = "datetime_beginning",
        verbose: bool = False,
    ):
        default_params = {
            "startRow": start_row,
            "rowCount": row_count,
        }

        # update final params with default params
        final_params = params.copy()
        final_params.update(default_params)

        if start is not None:
            start = utils._handle_date(start)

            if end:
                end = utils._handle_date(end)
            else:
                end = start + pd.DateOffset(days=1)
                # the end is inclusive, pulls records
                # for next day without below adjustment
                end = end - pd.DateOffset(seconds=1)

            final_params[f"{filter_timestamp_name}_ept"] = (
                start.strftime("%m/%d/%Y %H:%M") + "to" + end.strftime("%m/%d/%Y %H:%M")
            )

        # Exclude API key from logs
        params_to_log = final_params.copy()

        if "Ocp-Apim-Subscription-Key" in params_to_log:
            params_to_log["Ocp-Apim-Subscription-Key"] = "API_KEY_HIDDEN"

        logger.info(f"Retrieving data from {endpoint} with params {params_to_log}")
        r = self._get_json(
            "https://api.pjm.com/api/v1/" + endpoint,
            verbose=verbose,
            retries=self.retries,
            params=final_params,
            headers={"Ocp-Apim-Subscription-Key": self.api_key},
        )

        if "errors" in r:
            raise RuntimeError(r["errors"])

        # # todo should this be a warning?
        if r["totalRows"] == 0:
            raise NoDataFoundException(f"No data found for {endpoint}")

        df = pd.DataFrame(r["items"])

        num_pages = math.ceil(r["totalRows"] / row_count)
        if num_pages > 1:
            to_add = [df]
            for page in tqdm.tqdm(range(1, num_pages), initial=1, total=num_pages):
                next_url = [x for x in r["links"] if x["rel"] == "next"][0]["href"]
                r = self._get_json(
                    next_url,
                    verbose=verbose,
                    retries=self.retries,
                    headers={
                        "Ocp-Apim-Subscription-Key": self.api_key,
                    },
                )
                to_add.append(pd.DataFrame(r["items"]))

            df = pd.concat(to_add)

        if "datetime_beginning_utc" in df.columns:
            df["Interval Start"] = (
                # Some datetimes from the source have milliseconds. Parsing these
                # requires specifying the format.
                pd.to_datetime(df["datetime_beginning_utc"], format="ISO8601")
                .dt.tz_localize(
                    "UTC",
                )
                .dt.tz_convert(self.default_timezone)
            )

            # drop datetime_beginning_utc
            df = df.drop(columns=["datetime_beginning_utc"])

            # PJM API is inclusive of end,
            # so we need to drop where end timestamp is included
            df = df[
                df["Interval Start"].dt.strftime(
                    "%Y-%m-%d %H:%M",
                )
                != end.strftime("%Y-%m-%d %H:%M")
            ]

            if "datetime_ending_utc" in df.columns:
                df["Interval End"] = (
                    pd.to_datetime(df["datetime_ending_utc"], format="ISO8601")
                    .dt.tz_localize(
                        "UTC",
                    )
                    .dt.tz_convert(self.default_timezone)
                )

                # drop datetime_ending_utc
                df = df.drop(columns=["datetime_ending_utc"])
            elif interval_duration_min:
                df["Interval End"] = df["Interval Start"] + pd.Timedelta(
                    minutes=interval_duration_min,
                )

        if "Interval Start" in df.columns:
            df["Time"] = df["Interval Start"]

        return df

    def get_raw_interconnection_queue(self, verbose: bool = False) -> BinaryIO:
        url = "https://services.pjm.com/PJMPlanningApi/api/Queue/ExportToXls"
        response = requests.post(
            url,
            headers={
                # unclear if this key changes. obtained from https://www.pjm.com/dist/interconnectionqueues.71b76ed30033b3ff06bd.js
                "api-subscription-key": "E29477D0-70E0-4825-89B0-43F460BF9AB4",
                "Host": "services.pjm.com",
                "Origin": "https://www.pjm.com",
                "Referer": "https://www.pjm.com/",
            },
        )
        return utils.get_response_blob(response)

    def get_interconnection_queue(self, verbose: bool = False) -> pd.DataFrame:
        raw_data = self.get_raw_interconnection_queue(verbose)
        queue = pd.read_excel(raw_data)

        queue["Capacity (MW)"] = queue[["MFO", "MW In Service"]].min(axis=1)

        rename = {
            "Project ID": "Queue ID",
            "Name": "Project Name",
            "County": "County",
            "State": "State",
            "Transmission Owner": "Transmission Owner",
            "Submitted Date": "Queue Date",
            "Withdrawal Date": "Withdrawn Date",
            "Withdrawn Remarks": "Withdrawal Comment",
            "Status": "Status",
            "Revised In Service Date": "Proposed Completion Date",
            "Actual In Service Date": "Actual Completion Date",
            "Fuel": "Generation Type",
            "MW Capacity": "Summer Capacity (MW)",
            "MW Energy": "Winter Capacity (MW)",
        }

        extra = [
            "MW In Service",
            "Commercial Name",
            "Initial Study",
            "Feasibility Study",
            "Feasibility Study Status",
            "System Impact Study",
            "System Impact Study Status",
            "Facilities Study",
            "Facilities Study Status",
            "Interim/Interconnection Service/Generation Interconnection Agreement",
            "Interim/Interconnection Service/Generation Interconnection Agreement Status",  # noqa: E501
            "Wholesale Market Participation Agreement",
            "Construction Service Agreement",
            "Construction Service Agreement Status",
            "Upgrade Construction Service Agreement",
            "Upgrade Construction Service Agreement Status",
            "Backfeed Date",
            "Long-Term Firm Service Start Date",
            "Long-Term Firm Service End Date",
            "Test Energy Date",
        ]

        missing = ["Interconnecting Entity", "Interconnection Location"]

        queue = utils.format_interconnection_df(
            queue,
            rename,
            extra=extra,
            missing=missing,
        )

        return queue

    @support_date_range(frequency=None)
    def get_solar_forecast_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the hourly solar forecast including behind the meter solar forecast.
        From: https://dataminer2.pjm.com/feed/hourly_solar_power_forecast/definition
        Only available in past 30 days

        Args:
            date (str | pd.Timestamp): Start datetime for data
            end (str | pd.Timestamp | None, optional): End datetime for data. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame with the solar forecast data.
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "hourly_solar_power_forecast",
            start=date,
            params={
                "fields": "datetime_beginning_ept,datetime_beginning_utc,"
                "datetime_ending_ept,datetime_ending_utc,evaluated_at_ept,"
                "evaluated_at_utc,solar_forecast_btm_mwh,solar_forecast_mwh",
            },
            end=end,
            filter_timestamp_name="evaluated_at",
            interval_duration_min=60,
            verbose=verbose,
        )

        return self._parse_solar_forecast(df)

    @support_date_range(frequency=None)
    def get_solar_forecast_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the 5-min solar forecast including behind the meter solar forecast.
        From: https://dataminer2.pjm.com/feed/five_min_solar_power_forecast/definition
        Only available in past 30 days

        Args:
            date (str | pd.Timestamp): Start datetime for data
            end (str | pd.Timestamp | None, optional): End datetime for data. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame with the solar forecast data.
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "five_min_solar_power_forecast",
            start=date,
            params={
                "fields": "datetime_beginning_ept,datetime_beginning_utc,"
                "datetime_ending_ept,datetime_ending_utc,evaluated_at_ept,"
                "evaluated_at_utc,solar_forecast_btm_mwh,solar_forecast_mwh",
            },
            end=end,
            filter_timestamp_name="evaluated_at",
            interval_duration_min=5,
            verbose=verbose,
        )

        return self._parse_solar_forecast(df)

    def _parse_solar_forecast(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "evaluated_at_utc": "Publish Time",
                "solar_forecast_btm_mwh": "Solar Forecast BTM",
                "solar_forecast_mwh": "Solar Forecast",
            },
        )

        df["Publish Time"] = pd.to_datetime(
            df["Publish Time"],
            utc=True,
        ).dt.tz_convert(self.default_timezone)

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Solar Forecast BTM",
                "Solar Forecast",
            ]
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_wind_forecast_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the hourly wind forecast
        From: https://dataminer2.pjm.com/feed/hourly_wind_power_forecast/definition
        Only available in past 30 days

        Args:
            date (str | pd.Timestamp): Start datetime for data
            end (Optional[str  |  pd.Timestamp], optional): End datetime for data. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame with the wind forecast data.
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "hourly_wind_power_forecast",
            start=date,
            params={
                "fields": "datetime_beginning_ept,datetime_beginning_utc,"
                "datetime_ending_ept,datetime_ending_utc,evaluated_at_ept,"
                "evaluated_at_utc,wind_forecast_mwh",
            },
            end=end,
            filter_timestamp_name="evaluated_at",
            interval_duration_min=60,
            verbose=verbose,
        )

        return self._parse_wind_forecast(df)

    @support_date_range(frequency=None)
    def get_wind_forecast_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the 5-min wind forecast
        From: https://dataminer2.pjm.com/feed/five_min_wind_power_forecast/definition
        Only available in past 30 days

        Args:
            date (str | pd.Timestamp): Start datetime for data
            end (Optional[str  |  pd.Timestamp], optional): End datetime for data. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame with the wind forecast data.
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "five_min_wind_power_forecast",
            start=date,
            params={
                "fields": "datetime_beginning_ept,datetime_beginning_utc,"
                "datetime_ending_ept,datetime_ending_utc,evaluated_at_ept,"
                "evaluated_at_utc,wind_forecast_mwh",
            },
            end=end,
            filter_timestamp_name="evaluated_at",
            interval_duration_min=5,
            verbose=verbose,
        )

        return self._parse_wind_forecast(df)

    def _parse_wind_forecast(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "evaluated_at_utc": "Publish Time",
                "wind_forecast_mwh": "Wind Forecast",
            },
        )

        df["Publish Time"] = pd.to_datetime(
            df["Publish Time"],
            utc=True,
        ).dt.tz_convert(self.default_timezone)

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Wind Forecast",
            ]
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_gen_outages_by_type(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the generation outage data
        From: https://dataminer2.pjm.com/feed/gen_outages_by_type/definition
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "gen_outages_by_type",
            start=date,
            params={
                "fields": "forecast_execution_date_ept,forecast_date,"
                "region,planned_outages_mw,maintenance_outages_mw,"
                "forced_outages_mw,total_outages_mw",
            },
            end=end,
            filter_timestamp_name="forecast_execution_date",
            verbose=verbose,
        )

        return self._parse_gen_outages_by_type(df)

    def _parse_gen_outages_by_type(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "forecast_execution_date_ept": "Publish Time",
                "forecast_date": "Interval Start",
                "region": "Region",
                "planned_outages_mw": "Planned Outages MW",
                "maintenance_outages_mw": "Maintenance Outages MW",
                "forced_outages_mw": "Forced Outages MW",
                "total_outages_mw": "Total Outages MW",
            },
        )

        df["Interval Start"] = self.to_local_datetime(df, "Interval Start")
        df["Interval End"] = df["Interval Start"] + pd.DateOffset(days=1)
        df["Publish Time"] = self.to_local_datetime(df, "Publish Time")

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Region",
                "Planned Outages MW",
                "Maintenance Outages MW",
                "Forced Outages MW",
                "Total Outages MW",
            ]
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    # Can retrieve a max of 365 days at a time.
    @support_date_range(frequency="365D")
    def get_projected_rto_statistics_at_peak(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """RTO-wide projected data for the peak of the day

        https://dataminer2.pjm.com/feed/ops_sum_frcst_peak_rto/definition
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            endpoint="ops_sum_frcst_peak_rto",
            start=date,
            params={
                "fields": "area,capacity_adjustments,generated_at_ept,"
                "internal_scheduled_capacity,load_forecast,operating_reserve,"
                "projected_peak_datetime_ept,projected_peak_datetime_utc,"
                "scheduled_tie_flow_total,total_scheduled_capacity,"
                "unscheduled_steam_capacity",
            },
            end=end,
            filter_timestamp_name="generated_at",
            verbose=verbose,
        )

        return self._handle_projected_rto_statistics_at_peak(df)

    def _handle_projected_rto_statistics_at_peak(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        df["projected_peak_datetime_ept"] = pd.to_datetime(
            df["projected_peak_datetime_ept"],
        ).dt.tz_localize(self.default_timezone)
        df["generated_at_ept"] = pd.to_datetime(df["generated_at_ept"]).dt.tz_localize(
            self.default_timezone,
        )

        df = df.rename(
            columns={
                "projected_peak_datetime_ept": "Projected Peak Time",
                "generated_at_ept": "Publish Time",
                "area": "Area",
                "internal_scheduled_capacity": "Internal Scheduled Capacity",
                "scheduled_tie_flow_total": "Scheduled Tie Flow Total",
                "capacity_adjustments": "Capacity Adjustments",
                "total_scheduled_capacity": "Total Scheduled Capacity",
                "load_forecast": "Load Forecast",
                "operating_reserve": "Operating Reserve",
                "unscheduled_steam_capacity": "Unscheduled Steam Capacity",
            },
        ).drop(columns=["projected_peak_datetime_utc"])

        df["Interval Start"] = df["Projected Peak Time"].dt.floor("D")
        df["Interval End"] = df["Interval Start"] + pd.DateOffset(days=1)

        df = utils.move_cols_to_front(
            df,
            ["Interval Start", "Interval End", "Publish Time"],
        )

        return df.sort_values("Publish Time").reset_index(drop=True)

    @support_date_range(frequency="365D")
    def get_projected_area_statistics_at_peak(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Area projected data for the peak of the day

        https://dataminer2.pjm.com/feed/ops_sum_frcst_peak_area/definition
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            endpoint="ops_sum_frcst_peak_area",
            start=date,
            params={
                "fields": "area,generated_at_ept,internal_scheduled_capacity,"
                "pjm_load_forecast,projected_peak_datetime_ept,"
                "projected_peak_datetime_utc,unscheduled_steam_capacity",
            },
            end=end,
            filter_timestamp_name="generated_at",
            verbose=verbose,
        )

        return self._handle_projected_area_statistics_at_peak(df)

    def _handle_projected_area_statistics_at_peak(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        df["projected_peak_datetime_ept"] = pd.to_datetime(
            df["projected_peak_datetime_ept"],
        ).dt.tz_localize(self.default_timezone)

        df["generated_at_ept"] = pd.to_datetime(df["generated_at_ept"]).dt.tz_localize(
            self.default_timezone,
        )

        df = df.rename(
            columns={
                "projected_peak_datetime_ept": "Projected Peak Time",
                "generated_at_ept": "Publish Time",
                "area": "Area",
                "internal_scheduled_capacity": "Internal Scheduled Capacity",
                "pjm_load_forecast": "PJM Load Forecast",
                "unscheduled_steam_capacity": "Unscheduled Steam Capacity",
            },
        ).drop(columns=["projected_peak_datetime_utc"])

        df["Interval Start"] = df["Projected Peak Time"].dt.floor("D")
        df["Interval End"] = df["Interval Start"] + pd.DateOffset(days=1)

        df = utils.move_cols_to_front(
            df,
            ["Interval Start", "Interval End", "Publish Time"],
        )

        return df.sort_values("Publish Time").reset_index(drop=True)

    def to_local_datetime(self, df: pd.DataFrame, column_name: str) -> pd.Series:
        return pd.to_datetime(df[column_name]).dt.tz_localize(
            self.default_timezone,
        )

    @support_date_range(frequency=None)
    def get_solar_generation_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the 5 min solar generation data from:
        https://dataminer2.pjm.com/feed/five_min_solar_generation/definition
        Only available in past 30 days.

        Arguments:
            date (str or pandas.Timestamp): Start datetime for data
            end: (str or pandas.Timestamp, optional): End datetime for data.
                Defaults to one day past `date` if not specified.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with 5 minute solar generation data.
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "five_min_solar_generation",
            start=date,
            params={
                "fields": "datetime_beginning_ept,datetime_beginning_utc,"
                "solar_generation_mw",
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=5,
            verbose=verbose,
        )

        return self._parse_solar_generation_5_min(df)

    def _parse_solar_generation_5_min(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "solar_generation_mw": "Solar Generation",
            },
        )

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Solar Generation",
            ]
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_wind_generation_instantaneous(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the instantaneous wind generation data from:
        https://dataminer2.pjm.com/feed/instantaneous_wind_gen/definition
        Only available in past 30 days.

        Arguments:
            date (str or pandas.Timestamp): Start datetime for data
            end: (str or pandas.Timestamp, optional): End datetime for data.
                Defaults to one day past `date` if not specified.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with instantaneous wind generation data
                in 15 second intervals.
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "instantaneous_wind_gen",
            start=date,
            params={
                "fields": "datetime_beginning_ept,datetime_beginning_utc,"
                "wind_generation_mw",
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=0.25,
            verbose=verbose,
        )

        return self._parse_wind_generation_instantaneous(df)

    def _parse_wind_generation_instantaneous(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "wind_generation_mw": "Wind Generation",
            },
        )

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Wind Generation",
            ]
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_operational_reserves(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the reserve market quantities in Megawatts from:
        https://dataminer2.pjm.com/feed/operational_reserves/definition
        Only available in past 15 days.

        Arguments:
            date (str or pandas.Timestamp): Start datetime for data
            end: (str or pandas.Timestamp, optional): End datetime for data.
                Defaults to one day past `date` if not specified.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with reserve market quantities
                in 15 second intervals.
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "operational_reserves",
            start=date,
            params={
                "fields": "datetime_beginning_ept,datetime_beginning_utc,"
                "reserve_name,reserve_mw",
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=0.25,
            verbose=verbose,
        )

        return self._parse_operational_reserves(df)

    def _parse_operational_reserves(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "reserve_name": "Reserve Name",
                "reserve_mw": "Reserve",
            },
        )

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Reserve Name",
                "Reserve",
            ]
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_transfer_interface_information_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the transfer interface information from:
        https://dataminer2.pjm.com/feed/transfer_interface_infor/definition
        Only available in past 30 days.

        Arguments:
            date (str or pandas.Timestamp): Start datetime for data
            end: (str or pandas.Timestamp, optional): End datetime for data.
                Defaults to one day past `date` if not specified.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with transfer interface information
            in 5 minute intervals.
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "transfer_interface_infor",
            start=date,
            params={
                "fields": "datetime_beginning_ept,datetime_beginning_utc,name,"
                "actual_flow,warning_level,transfer_limit",
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=5,
            verbose=verbose,
        )

        return self._parse_transfer_interface_information_5_min(df)

    def _parse_transfer_interface_information_5_min(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        df = df.rename(
            columns={
                "name": "Interface Name",
                "actual_flow": "Actual Flow",
                "warning_level": "Warning Level",
                "transfer_limit": "Transfer Limit",
            },
        )

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Interface Name",
                "Actual Flow",
                "Warning Level",
                "Transfer Limit",
            ]
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_transmission_limits(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the current transmission limit information from:
        https://dataminer2.pjm.com/feed/transfer_interface_infor/definition
        Only available in past 30 days. Data is published only when constraints
        exist for that five minute interval.

        Arguments:
            date (str or pandas.Timestamp): Start datetime for data
            end: (str or pandas.Timestamp, optional): End datetime for data.
                Defaults to one day past `date` if not specified.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with transmission limit information
            in 5 minute intervals, when data is available.
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "transmission_limits",
            start=date,
            params={
                "fields": "datetime_beginning_ept,datetime_beginning_utc,"
                "constraint_name,constraint_type,contingency,shadow_price",
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=5,
            verbose=verbose,
        )

        return self._parse_transmission_limits(df)

    def _parse_transmission_limits(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "constraint_name": "Constraint Name",
                "constraint_type": "Constraint Type",
                "contingency": "Contingency",
                "shadow_price": "Shadow Price",
            },
        )

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Constraint Type",
                "Contingency",
                "Shadow Price",
            ]
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_solar_generation_by_area(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the current solar generation information from:
        https://dataminer2.pjm.com/feed/solar_gen/definition
        Data is published daily around 7am market time.

        Arguments:
            date (str or pandas.Timestamp): Start datetime for data
            end: (str or pandas.Timestamp, optional): End datetime for data.
                Defaults to one day past `date` if not specified.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with solar generation information.
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "solar_gen",
            start=date,
            params={
                "fields": "datetime_beginning_ept,datetime_beginning_utc,"
                "area,solar_generation_mw",
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=60,
            verbose=verbose,
        )

        return self._parse_solar_generation_by_area(df)

    def _parse_solar_generation_by_area(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.pivot_table(
            index=["Time", "Interval Start", "Interval End"],
            columns="area",
            values="solar_generation_mw",
            aggfunc="first",
        ).reset_index()

        df = df[
            [
                "Interval Start",
                "Interval End",
                "MIDATL",
                "OTHER",
                "RFC",
                "RTO",
                "SOUTH",
                "WEST",
            ]
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_wind_generation_by_area(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        """
        Retrieves the current wind generation information from:
        https://dataminer2.pjm.com/feed/wind_gen/definition
        Data is published daily around 7am market time.

        Arguments:
            date (str or pandas.Timestamp): Start datetime for data
            end: (str or pandas.Timestamp, optional): End datetime for data.
                Defaults to one day past `date` if not specified.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with wind generation information.
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "wind_gen",
            start=date,
            params={
                "fields": "datetime_beginning_ept,datetime_beginning_utc,"
                "area,wind_generation_mw",
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=60,
            verbose=verbose,
        )

        return self._parse_wind_generation_by_area(df)

    def _parse_wind_generation_by_area(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.pivot_table(
            index=["Time", "Interval Start", "Interval End"],
            columns="area",
            values="wind_generation_mw",
            aggfunc="first",
        ).reset_index()

        # SOUTH and OTHER columns did not exist at the start of the
        # data, but we want them to be present, so make sure they exist.
        cols = ["SOUTH", "OTHER"]
        for col in cols:
            if col not in df.columns:
                df[col] = pd.NA

        df = df[
            [
                "Interval Start",
                "Interval End",
                "MIDATL",
                "OTHER",
                "RFC",
                "RTO",
                "SOUTH",
                "WEST",
            ]
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_dam_as_market_results(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        """
        Retrieves the day-ahead ancillary service market results from :
        https://dataminer2.pjm.com/feed/da_reserve_market_results/definition
        Data is published daily.

        Arguments:
            date (str or pandas.Timestamp): Start datetime for data
            end: (str or pandas.Timestamp, optional): End datetime for data.
                Defaults to one day past `date` if not specified.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with day-ahead ancillary service
            market results.
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "da_reserve_market_results",
            start=date,
            params={
                "fields": "datetime_beginning_ept,datetime_beginning_utc,locale,"
                "service,mcp,mcp_capped,as_req_mw,total_mw,as_mw,ss_mw,ircmwt2,"
                "dsr_as_mw,nsr_mw",
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=60,
            verbose=verbose,
        )

        return self._parse_dam_as_market_results(df)

    def _parse_dam_as_market_results(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "locale": "Locale",
                "service": "Service Type",
                "mcp": "Market Clearing Price",
                "mcp_capped": "Market Clearing Price Capped",
                "as_req_mw": "Ancillary Service Required",
                "total_mw": "Total MW",
                "as_mw": "Assigned MW",
                "ss_mw": "Self-Scheduled MW",
                "ircmwt2": "Interface Reserve Capability MW",
                "dsr_as_mw": "Demand Response MW Assigned",
                "nsr_mw": "Non-Synchronized Reserve MW Assigned",
            },
        )

        # Add new Ancillary Service column
        locale_full_name_to_abbreviation = {
            v: k for k, v in self.locale_abbreviated_to_full.items()
        }
        df["Ancillary Service"] = (
            df["Locale"].replace(locale_full_name_to_abbreviation)
            + "-"
            + df["Service Type"]
        )

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Ancillary Service",
                "Locale",
                "Service Type",
                "Market Clearing Price",
                "Market Clearing Price Capped",
                "Ancillary Service Required",
                "Total MW",
                "Assigned MW",
                "Self-Scheduled MW",
                "Interface Reserve Capability MW",
                "Demand Response MW Assigned",
                "Non-Synchronized Reserve MW Assigned",
            ]
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_real_time_as_market_results(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        """
        Retrieves the real-time ancillary service market results from :
        https://dataminer2.pjm.com/feed/reserve_market_results/definition
        Data for the previous day is published daily on business days,
        typically between 11am and 12pm market time.

        Data granularity changed on Sep 1, 2022 so when querying data,
        start and end dates must both be before or both after that date.

        Arguments:
            date (str or pandas.Timestamp): Start datetime for data
            end: (str or pandas.Timestamp, optional): End datetime for data.
                Defaults to one day past `date` if not specified.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with real-time ancillary service
            market results.
        """
        if date == "latest":
            date = "today"

        # Make sure start and end are both before or both after Sep 1, 2022
        # when data granularity changes
        cutoff_date = datetime(
            2022,
            9,
            1,
            0,
            0,
            0,
            0,
            pytz.timezone(self.default_timezone),
        )
        if date < cutoff_date:
            if end and end > cutoff_date:
                raise ValueError(
                    f"Both start and end dates must be before {cutoff_date}.",
                )
            interval_duration = 60
        else:
            interval_duration = 5

        df = self._get_pjm_json(
            "reserve_market_results",
            start=date,
            params={
                "fields": "datetime_beginning_ept,datetime_beginning_utc,locale,"
                "service,mcp,mcp_capped,reg_ccp,reg_pcp,as_req_mw,total_mw,as_mw,"
                "ss_mw,tier1_mw,ircmwt2,dsr_as_mw,nsr_mw,regd_mw",
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=interval_duration,
            verbose=verbose,
        )

        return self._parse_real_time_as_market_results(df)

    def _parse_real_time_as_market_results(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "locale": "Locale",
                "service": "Service Type",
                "mcp": "Market Clearing Price",
                "mcp_capped": "Market Clearing Price Capped",
                "reg_ccp": "Regulation Capability Clearing Price",
                "reg_pcp": "Regulation Performance Clearing Price",
                "as_req_mw": "Ancillary Service Required",
                "total_mw": "Total MW",
                "as_mw": "Assigned MW",
                "ss_mw": "Self-Scheduled MW",
                "tier1_mw": "Tier 1 MW",
                "ircmwt2": "Interface Reserve Capability MW",
                "dsr_as_mw": "Demand Response MW Assigned",
                "nsr_mw": "Non-Synchronized Reserve MW Assigned",
                "regd_mw": "REGD MW",
            },
        )
        # Replace abbreviated locale values will full values
        df = df.replace({"Locale": self.locale_abbreviated_to_full})

        # Replace abbreviated service type values with full values
        df = df.replace({"Service Type": self.service_type_abbreviated_to_full})

        # Add new Ancillary Service column
        locale_full_name_to_abbreviation = {
            v: k for k, v in self.locale_abbreviated_to_full.items()
        }
        df["Ancillary Service"] = (
            df["Locale"].replace(locale_full_name_to_abbreviation)
            + "-"
            + df["Service Type"]
        )

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Ancillary Service",
                "Locale",
                "Service Type",
                "Market Clearing Price",
                "Market Clearing Price Capped",
                "Regulation Capability Clearing Price",
                "Regulation Performance Clearing Price",
                "Ancillary Service Required",
                "Total MW",
                "Assigned MW",
                "Self-Scheduled MW",
                "Tier 1 MW",
                "Interface Reserve Capability MW",
                "Demand Response MW Assigned",
                "Non-Synchronized Reserve MW Assigned",
                "REGD MW",
            ]
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_load_metered_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        """
        Retrieves the hourly metered load data from:

        https://dataminer2.pjm.com/feed/hrl_load_metered/definition
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "hrl_load_metered",
            start=date,
            params={
                "fields": "datetime_beginning_ept,datetime_beginning_utc,is_verified,load_area,mkt_region,mw,nerc_region,zone",  # noqa: E501
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=60,
            verbose=verbose,
        )

        return self._parse_load_metered_hourly(df)

    def _parse_load_metered_hourly(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "load_area": "Load Area",
                "mkt_region": "Mkt Region",
                "mw": "MW",
                "nerc_region": "NERC Region",
                "zone": "Zone",
                "is_verified": "Is Verified",
            },
        )

        df = df[
            [
                "Interval Start",
                "Interval End",
                "NERC Region",
                "Mkt Region",
                "Zone",
                "Load Area",
                "MW",
                "Is Verified",
            ]
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_forecasted_generation_outages(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ):
        """
        Retrieves the forecasted generation outages for the next 90 days from:

        https://dataminer2.pjm.com/feed/frcstd_gen_outages/definition
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "frcstd_gen_outages",
            start=date,
            params={
                "fields": "forecast_execution_date_ept,forecast_date,forecast_gen_outage_mw_rto,forecast_gen_outage_mw_west,forecast_gen_outage_mw_other",  # noqa: E501
            },
            end=end,
            filter_timestamp_name="forecast_execution_date",
            interval_duration_min=1440,
            verbose=verbose,
        )

        return self._parse_forecasted_generation_outages(df)

    def _parse_forecasted_generation_outages(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "forecast_execution_date_ept": "Publish Time",
                "forecast_date": "Interval Start",
                "forecast_gen_outage_mw_rto": "RTO MW",
                "forecast_gen_outage_mw_west": "West MW",
                "forecast_gen_outage_mw_other": "Other MW",
            },
        )

        df["Interval Start"] = self.to_local_datetime(df, "Interval Start")
        df["Interval End"] = df["Interval Start"] + pd.DateOffset(days=1)
        df["Publish Time"] = self.to_local_datetime(df, "Publish Time")

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "RTO MW",
                "West MW",
                "Other MW",
            ]
        ]

        return df.sort_values("Interval Start").reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_marginal_value_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the marginal value data from:
        https://dataminer2.pjm.com/feed/rt_marginal_value/definition
        """
        df = self._get_pjm_json(
            "rt_marginal_value",
            start=date,
            params={
                "fields": "datetime_beginning_utc, datetime_ending_utc, monitored_facility, contingency_facility, transmission_constraint_penalty_factor, limit_control_percentage, shadow_price",
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=5,
            verbose=verbose,
        )

        df = df.rename(
            columns={
                "monitored_facility": "Monitored Facility",
                "contingency_facility": "Contingency Facility",
                "transmission_constraint_penalty_factor": "Transmission Constraint Penalty Factor",
                "limit_control_percentage": "Limit Control Percentage",
                "shadow_price": "Shadow Price",
            },
        )

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Monitored Facility",
                "Contingency Facility",
                "Transmission Constraint Penalty Factor",
                "Limit Control Percentage",
                "Shadow Price",
            ]
        ]
        return df

    @support_date_range(frequency=None)
    def get_marginal_value_day_ahead_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the marginal value data from:
        https://dataminer2.pjm.com/feed/da_marginal_value/definition
        """
        df = self._get_pjm_json(
            "da_marginal_value",
            start=date,
            params={
                "fields": "datetime_beginning_utc, datetime_ending_utc, monitored_facility, contingency_facility, shadow_price",
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=60,
            verbose=verbose,
        )
        df = df.rename(
            columns={
                "monitored_facility": "Monitored Facility",
                "contingency_facility": "Contingency Facility",
                "shadow_price": "Shadow Price",
            },
        )
        df = df[
            [
                "Interval Start",
                "Interval End",
                "Monitored Facility",
                "Contingency Facility",
                "Shadow Price",
            ]
        ]
        return df

    @support_date_range(frequency=None)
    def get_transmission_constraints_day_ahead_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the transmission constraints data from:
        https://dataminer2.pjm.com/feed/da_transconstraints/definition
        """
        df = self._get_pjm_json(
            "da_transconstraints",
            start=date,
            params={
                "fields": "datetime_beginning_utc,datetime_ending_utc,duration, day_ahead_congestion_event, monitored_facility, contingency_facility",
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=60,
            verbose=verbose,
        )
        df = df.rename(
            columns={
                "duration": "Duration",
                "day_ahead_congestion_event": "Day Ahead Congestion Event",
                "monitored_facility": "Monitored Facility",
                "contingency_facility": "Contingency Facility",
            },
        )
        df = df[
            [
                "Interval Start",
                "Interval End",
                "Duration",
                "Day Ahead Congestion Event",
                "Monitored Facility",
                "Contingency Facility",
            ]
        ]
        return df

    @support_date_range(frequency=None)
    def get_day_ahead_demand_bids(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the day ahead demand bids data from:
        https://dataminer2.pjm.com/feed/hrl_dmd_bids/definition
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "hrl_dmd_bids",
            start=date,
            params={
                "fields": "area,datetime_beginning_utc,hrly_da_demand_bid",
            },
            end=end,
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=60,
            verbose=verbose,
        ).rename(
            columns={
                "hrly_da_demand_bid": "Demand Bid",
                "area": "Area",
            },
        )

        df = df[["Interval Start", "Interval End", "Area", "Demand Bid"]].sort_values(
            ["Interval Start", "Area"],
        )

        return df

    @support_date_range(frequency=None)
    def get_area_control_error(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the area control error data from:
        https://dataminer2.pjm.com/feed/area_control_error/definition
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "area_control_error",
            start=date,
            end=end,
            params={
                "fields": "datetime_beginning_utc,ace_mw",
            },
            verbose=verbose,
        )
        df = df.rename(
            columns={
                "ace_mw": "Area Control Error",
            },
        )
        return df[["Time", "Area Control Error"]]

    @support_date_range(frequency=None)
    def get_dispatched_reserves_prelim(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the dispatched reserves preliminary data from:
        https://dataminer2.pjm.com/feed/dispatched_reserves/definition
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "dispatched_reserves",
            start=date,
            end=end,
            params={
                "fields": "datetime_beginning_utc,area,reserve_type,reserve_quantity,reserve_requirement,reliability_requirement,extended_requirement,mw_adjustment,market_clearing_price,shortage_indicator",
            },
            interval_duration_min=5,
            verbose=verbose,
        )

        df = df.rename(
            columns={
                "area": "Area",
                "reserve_type": "Reserve Type",
                "reserve_quantity": "Reserve Quantity",
                "reserve_requirement": "Reserve Requirement",
                "reliability_requirement": "Reliability Requirement",
                "extended_requirement": "Extended Requirement",
                "mw_adjustment": "MW Adjustment",
                "market_clearing_price": "Market Clearing Price",
                "shortage_indicator": "Shortage Indicator",
            },
        )

        df["Area"] = df["Area"].str.replace("Mid-Atlantic/Dominion", "MAD")
        df["Ancillary Service"] = df["Area"] + "-" + df["Reserve Type"]

        df = df.replace({"Area": self.locale_abbreviated_to_full})

        return df[
            [
                "Interval Start",
                "Interval End",
                "Ancillary Service",
                "Area",
                "Reserve Type",
                "Reserve Quantity",
                "Reserve Requirement",
                "Reliability Requirement",
                "Extended Requirement",
                "MW Adjustment",
                "Market Clearing Price",
                "Shortage Indicator",
            ]
        ]

    @support_date_range(frequency=None)
    def get_dispatched_reserves_verified(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the dispatched reserves verified data from:
        https://dataminer2.pjm.com/feed/rt_dispatch_reserves/definition
        """
        if date == "latest":
            # TODO: This is a hack to get the data for the previous day
            # because the data is not available for the current day. Thinking about
            # adding a "yesterday" to @support_date_range
            date = (
                pd.Timestamp.now(self.default_timezone) - pd.Timedelta(days=1)
            ).strftime("%Y-%m-%d")

        df = self._get_pjm_json(
            "rt_dispatch_reserves",
            start=date,
            end=end,
            params={
                "fields": "datetime_beginning_utc,area,reserve_type,total_reserve_mw,reserve_reqmt_mw,reliability_reqmt_mw,extended_reqmt_mw,additional_extended_reqmt_mw,deficit_mw",
            },
            interval_duration_min=5,
            verbose=verbose,
        )

        df = df.rename(
            columns={
                "area": "Area",
                "reserve_type": "Reserve Type",
                "total_reserve_mw": "Total Reserve",
                "reserve_reqmt_mw": "Reserve Requirement",
                "reliability_reqmt_mw": "Reliability Requirement",
                "extended_reqmt_mw": "Extended Requirement",
                "additional_extended_reqmt_mw": "Additional Extended Requirement",
                "deficit_mw": "Deficit",
            },
        )
        full_name_to_abbreviation = {
            v: k for k, v in self.locale_abbreviated_to_full.items()
        }
        df["Ancillary Service"] = (
            df["Area"].replace(full_name_to_abbreviation) + "-" + df["Reserve Type"]
        )

        return df[
            [
                "Interval Start",
                "Interval End",
                "Ancillary Service",
                "Area",
                "Reserve Type",
                "Total Reserve",
                "Reserve Requirement",
                "Reliability Requirement",
                "Extended Requirement",
                "Additional Extended Requirement",
                "Deficit",
            ]
        ]

    @support_date_range(frequency=None)
    def get_regulation_market_monthly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the PJM Regulation Market Monthly data from:
        https://dataminer2.pjm.com/feed/reg_market_results/definition
        """
        if date == "latest":
            current_date = pd.Timestamp.now(self.default_timezone).replace(
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            ) + pd.tseries.offsets.MonthEnd(0)

            while current_date > pd.Timestamp("2024-01-01").tz_localize(
                self.default_timezone,
            ):
                try:
                    return self.get_regulation_market_monthly(
                        date=current_date.strftime("%Y-%m-%d"),
                        end=end,
                        verbose=verbose,
                    )
                except NoDataFoundException:
                    logger.warning(
                        f"No regulation market monthly data found for {current_date.strftime('%Y-%m-%d')}, trying previous month",
                    )
                    current_date = current_date - pd.DateOffset(months=1)

        df = self._get_pjm_json(
            "reg_market_results",
            start=date,
            end=end,
            params={
                "fields": "datetime_beginning_utc,rega_procure,regd_procure,rega_ssmw,regd_ssmw,requirement,total_mw,deficiency,rto_perfscore,rega_mileage,regd_mileage,rega_hourly,regd_hourly,is_approved,modified_datetime_utc",
            },
            interval_duration_min=60,
            verbose=verbose,
        )
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(minutes=60)
        df = df.rename(
            columns={
                "datetime_beginning_utc": "Interval Start",
                "rega_procure": "RegA Procure",
                "regd_procure": "RegD Procure",
                "rega_ssmw": "RegA SSMW",
                "regd_ssmw": "RegD SSMW",
                "requirement": "Requirement",
                "total_mw": "Total MW",
                "deficiency": "Deficiency",
                "rto_perfscore": "RTO Perfscore",
                "rega_mileage": "RegA Mileage",
                "regd_mileage": "RegD Mileage",
                "rega_hourly": "RegA Hourly",
                "regd_hourly": "RegD Hourly",
                "is_approved": "Is Approved",
                "modified_datetime_utc": "Modified Datetime UTC",
            },
        )

        df = df.astype(
            {
                "RegD SSMW": float,
                "RegA SSMW": float,
                "RegD Procure": float,
                "RegA Procure": float,
                "Total MW": float,
                "Deficiency": float,
                "RTO Perfscore": float,
                "RegA Mileage": float,
                "RegD Mileage": float,
                "RegA Hourly": float,
                "RegD Hourly": float,
                "Is Approved": int,
            },
        )

        df["Modified Datetime UTC"] = pd.to_datetime(
            df["Modified Datetime UTC"],
            utc=True,
        )

        return df[
            [
                "Interval Start",
                "Interval End",
                "Requirement",
                "RegD SSMW",
                "RegA SSMW",
                "RegD Procure",
                "RegA Procure",
                "Total MW",
                "Deficiency",
                "RTO Perfscore",
                "RegA Mileage",
                "RegD Mileage",
                "RegA Hourly",
                "RegD Hourly",
                "Is Approved",
                "Modified Datetime UTC",
            ]
        ]

    @support_date_range(frequency=None)
    def get_tie_flows_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the PJM Tie Flows 5 Minute data from:
        https://dataminer2.pjm.com/feed/tie_flows/definition
        """
        if date == "latest":
            date = "today"

        df = self._get_pjm_json(
            "five_min_tie_flows",
            start=date,
            end=end,
            params={
                "fields": "datetime_beginning_utc,tie_flow_name, actual_mw, scheduled_mw",
            },
            interval_duration_min=5,
            verbose=verbose,
        )
        df = df.rename(
            columns={
                "tie_flow_name": "Tie Flow Name",
                "actual_mw": "Actual",
                "scheduled_mw": "Scheduled",
            },
        )
        # NB: The data has an extra second on each timestamp like 2025-05-20 12:00:01, so we need to floor to the minute
        df["Interval Start"] = pd.to_datetime(df["Interval Start"]).dt.floor("min")
        df["Interval End"] = pd.to_datetime(df["Interval End"]).dt.floor("min")

        return df[
            ["Interval Start", "Interval End", "Tie Flow Name", "Actual", "Scheduled"]
        ]

    @support_date_range(frequency=None)
    def get_instantaneous_dispatch_rates(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the instantaneous dispatch rate data from:
        https://dataminer2.pjm.com/feed/inst_dispatch_rate/definition
        """
        if date == "latest":
            # Get latest 5 minutes
            date = pd.Timestamp.now(tz=self.default_timezone) - pd.Timedelta(minutes=5)
            end = pd.Timestamp.now(tz=self.default_timezone)

        df = self._get_pjm_json(
            "inst_dispatch_rates",
            start=date,
            end=end,
            params={
                "fields": "datetime_beginning_utc,dispatch_rate,zone",
            },
            interval_duration_min=0.25,
            verbose=verbose,
        )

        df = df.rename(
            columns={"zone": "Zone", "dispatch_rate": "Instantaneous Dispatch Rate"},
        )

        return (
            df[
                [
                    "Interval Start",
                    "Interval End",
                    "Zone",
                    "Instantaneous Dispatch Rate",
                ]
            ]
            .sort_values(
                "Interval Start",
            )
            .reset_index(drop=True)
        )

    @support_date_range(frequency=None)
    def get_hourly_net_exports_by_state(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the hourly net exports by state data from:
        https://dataminer2.pjm.com/feed/state_net_interchange/definition
        """
        if date == "latest":
            return self.get_hourly_net_exports_by_state("today")

        df = self._get_pjm_json(
            "state_net_interchange",
            start=date,
            end=end,
            params={
                "fields": "datetime_beginning_utc,state,net_interchange",
            },
            interval_duration_min=60,
            verbose=verbose,
        )
        df = df.rename(
            columns={
                "state": "State",
                "net_interchange": "Net Interchange",
            },
        )
        return df[["Interval Start", "Interval End", "State", "Net Interchange"]]

    @support_date_range(frequency=None)
    def get_hourly_transfer_limits_and_flows(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the hourly transfer limits and flows data from:
        https://dataminer2.pjm.com/feed/transfer_limits_and_flows/definition
        """
        if date == "latest":
            # NB: Most recent complete month
            today = pd.Timestamp.now(tz=self.default_timezone)
            first_of_month = today.replace(day=1) - pd.DateOffset(months=1)
            end_of_month = (
                first_of_month + pd.DateOffset(months=1) - pd.DateOffset(days=1)
            )
            return self.get_hourly_transfer_limits_and_flows(
                first_of_month,
                end=end_of_month,
            )

        df = self._get_pjm_json(
            "transfer_limits_and_flows",
            start=date,
            end=end,
            params={
                "fields": "datetime_beginning_utc,datetime_ending_utc,transfer_limit_area,transfers,transfer_limit",
            },
            interval_duration_min=60,
            verbose=verbose,
        )
        df = df.rename(
            columns={
                "transfer_limit_area": "Transfer Limit Area",
                "transfers": "Average Transfers",
                "transfer_limit": "Average Transfer Limit",
            },
        )
        return df[
            [
                "Interval Start",
                "Interval End",
                "Transfer Limit Area",
                "Average Transfers",
                "Average Transfer Limit",
            ]
        ]

    @support_date_range(frequency=None)
    def get_actual_and_scheduled_interchange_summary(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the actual and scheduled interchange summary data from:
        https://dataminer2.pjm.com/feed/actual_and_scheduled_interchange_summary/definition
        """
        if date == "latest":
            # NB: Most recent full week, back to the previous Sunday at 8am default timezone
            today = pd.Timestamp.now(tz=self.default_timezone)
            start = today - pd.Timedelta(days=today.dayofweek)
            start = start - pd.Timedelta(days=7)
            start = start.replace(hour=8, minute=0, second=0, microsecond=0)
            if today.hour < 8:
                start = start - pd.Timedelta(days=7)
            end = start + pd.Timedelta(days=7)
            return self.get_actual_and_scheduled_interchange_summary(start, end=end)

        df = self._get_pjm_json(
            "act_sch_interchange",
            start=date,
            end=end,
            params={
                "fields": "datetime_beginning_utc,datetime_ending_utc,tie_line,actual_flow,sched_flow,inadv_flow",
            },
            interval_duration_min=60,
            verbose=verbose,
        )
        df = df.rename(
            columns={
                "tie_line": "Tie Line",
                "actual_flow": "Actual Flow",
                "sched_flow": "Scheduled Flow",
                "inadv_flow": "Inadvertent Flow",
            },
        )
        return df[
            [
                "Interval Start",
                "Interval End",
                "Tie Line",
                "Actual Flow",
                "Scheduled Flow",
                "Inadvertent Flow",
            ]
        ]

    @support_date_range(frequency=None)
    def get_scheduled_interchange_real_time(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the scheduled interchange real time data from:
        https://dataminer2.pjm.com/feed/rt_scheduled_interchange/definition
        """
        if date == "latest":
            try:
                return self.get_scheduled_interchange_real_time("today")
            except NoDataFoundException:
                logger.warning(
                    "No scheduled interchange real time data found for today, trying yesterday...",
                )
                yesterday = pd.Timestamp.now(
                    tz=self.default_timezone,
                ).date() - pd.Timedelta(days=1)
                return self.get_scheduled_interchange_real_time(yesterday)

        df = self._get_pjm_json(
            "rt_scheduled_interchange",
            start=date,
            end=end,
            params={
                "fields": "datetime_beginning_utc,datetime_ending_utc,tie_line, hrly_net_tie_sched",
            },
            interval_duration_min=60,
            verbose=verbose,
        )
        df = df.rename(
            columns={
                "tie_line": "Tie Line",
                "hrly_net_tie_sched": "Hourly Net Tie Schedule",
            },
        ).sort_values("Interval Start")
        return df[
            ["Interval Start", "Interval End", "Tie Line", "Hourly Net Tie Schedule"]
        ]

    @support_date_range(frequency=None)
    def get_interface_flows_and_limits_day_ahead(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the interface flows and limit day ahead data from:
        https://dataminer2.pjm.com/feed/da_interface_flows_and_limits/definition
        """
        if date == "latest":
            return self.get_interface_flows_and_limits_day_ahead("today")

        df = self._get_pjm_json(
            "da_interface_flows_and_limits",
            start=date,
            end=end,
            params={
                "fields": "datetime_beginning_utc,interface_limit_name,flow_mw,limit_mw",
            },
            interval_duration_min=60,
            verbose=verbose,
        )
        df = df.rename(
            columns={
                "interface_limit_name": "Interface Limit Name",
                "flow_mw": "Flow",
                "limit_mw": "Limit",
            },
        )
        return df[
            ["Interval Start", "Interval End", "Interface Limit Name", "Flow", "Limit"]
        ]

    @support_date_range(frequency=None)
    def get_projected_peak_tie_flow(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the projected peak tie flow data from:
        https://dataminer2.pjm.com/feed/ops_sum_prjctd_tie_flow/definition
        """
        if date == "latest":
            now = pd.Timestamp.now(tz=self.default_timezone)
            if now.hour >= 5:
                return self.get_projected_peak_tie_flow("today")
            else:
                yesterday = pd.Timestamp.now(
                    tz=self.default_timezone,
                ).date() - pd.Timedelta(days=1)
                return self.get_projected_peak_tie_flow(yesterday)

        df = self._get_pjm_json(
            "ops_sum_prjctd_tie_flow",
            start=date,
            end=end,
            params={
                "fields": "projected_peak_datetime_utc,generated_at_ept,interface,scheduled_tie_flow",
            },
            interval_duration_min=60,
            verbose=verbose,
            filter_timestamp_name="generated_at",
        )

        df["Publish Time"] = pd.to_datetime(df["generated_at_ept"]).dt.tz_localize(
            self.default_timezone,
        )

        df["Projected Peak Time"] = (
            pd.to_datetime(
                df["projected_peak_datetime_utc"],
                format="ISO8601",
            )
            .dt.tz_localize("UTC")
            .dt.tz_convert(self.default_timezone)
        )

        df["Interval Start"] = df["Projected Peak Time"].dt.floor("D")
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(days=1)
        df = df.rename(
            columns={
                "interface": "Interface",
                "scheduled_tie_flow": "Scheduled Tie Flow",
            },
        )
        return df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Projected Peak Time",
                "Interface",
                "Scheduled Tie Flow",
            ]
        ]

    @support_date_range(frequency=None)
    def get_actual_operational_statistics(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the actual operational statistics data from:
        https://dataminer2.pjm.com/feed/ops_sum_prev_period/definition
        """
        if date == "latest":
            now = pd.Timestamp.now(tz=self.default_timezone)
            if now.hour >= 5:
                return self.get_actual_operational_statistics("today")
            else:
                yesterday = pd.Timestamp.now(
                    tz=self.default_timezone,
                ).date() - pd.Timedelta(days=1)
                return self.get_actual_operational_statistics(yesterday)

        df = self._get_pjm_json(
            "ops_sum_prev_period",
            start=date,
            end=end,
            params={
                "fields": "datetime_beginning_utc,datetime_ending_utc,generated_at_ept,area,area_load_forecast,actual_load,dispatch_rate",
            },
            filter_timestamp_name="generated_at",
            verbose=verbose,
        )

        df = df.rename(
            columns={
                "generated_at_ept": "Publish Time",
                "area": "Area",
                "area_load_forecast": "Area Load Forecast",
                "actual_load": "Actual Load",
                "dispatch_rate": "Dispatch Rate",
            },
        )
        df["Publish Time"] = pd.to_datetime(df["Publish Time"]).dt.tz_localize(
            self.default_timezone,
        )

        return (
            df[
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "Area",
                    "Area Load Forecast",
                    "Actual Load",
                    "Dispatch Rate",
                ]
            ]
            .sort_values(["Interval Start", "Area"])
            .reset_index(drop=True)
        )

    def _filter_active_records(
        self,
        df: pd.DataFrame,
        as_of: pd.Timestamp | None,
    ) -> pd.DataFrame:
        """Filter out records that are terminated before the given date"""
        df["Effective Date"] = pd.to_datetime(
            df["Effective Date"],
            errors="coerce",
        ).dt.tz_localize(
            self.default_timezone,
        )

        df["Termination Date"] = df["Termination Date"].replace(
            "9999-12-31T00:00:00",
            None,
        )
        df["Termination Date"] = pd.to_datetime(
            df["Termination Date"],
            errors="coerce",
        ).dt.tz_localize(
            self.default_timezone,
        )

        if as_of is None:
            return df

        return df[(df["Termination Date"].isna()) | (df["Termination Date"] > as_of)]

    def get_pricing_nodes(
        self,
        as_of: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the pricing nodes data from:
        https://dataminer2.pjm.com/feed/pnode/definition
        """
        as_of = utils._handle_date(as_of, tz=self.default_timezone)
        if as_of == "now":
            return self.get_pricing_nodes(
                as_of=pd.Timestamp.now(tz=self.default_timezone),
            )

        df = self._get_pjm_json(
            "pnode",
            start=None,
            end=None,
            params={
                "fields": "pnode_id,pnode_name,pnode_type,pnode_subtype,zone,voltage_level,effective_date,termination_date",
            },
            verbose=verbose,
        )

        df = df.rename(
            columns={
                "pnode_id": "Pricing Node ID",
                "pnode_name": "Pricing Node Name",
                "pnode_type": "Pricing Node Type",
                "pnode_subtype": "Pricing Node SubType",
                "zone": "Zone",
                "voltage_level": "Voltage Level",
                "effective_date": "Effective Date",
                "termination_date": "Termination Date",
            },
        )

        df = self._filter_active_records(df, as_of)

        return (
            df[
                [
                    "Pricing Node ID",
                    "Pricing Node Name",
                    "Pricing Node Type",
                    "Pricing Node SubType",
                    "Zone",
                    "Voltage Level",
                    "Effective Date",
                    "Termination Date",
                ]
            ]
            .sort_values(["Effective Date", "Pricing Node Name"])
            .reset_index(drop=True)
        )

    def get_reserve_subzone_resources(
        self,
        as_of: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the reserve subzone resources data from:
        https://dataminer2.pjm.com/feed/sync_pri_reserves_resources_list/definition
        """
        as_of = utils._handle_date(as_of, tz=self.default_timezone)
        if as_of == "now":
            return self.get_reserve_subzone_resources(
                as_of=pd.Timestamp.now(tz=self.default_timezone),
                verbose=verbose,
            )

        df = self._get_pjm_json(
            "sync_pri_reserves_resources_list",
            start=None,
            params={
                "fields": "effective_date,terminate_date,subzone,resource_id,resource_name,resource_type,zone",
            },
            verbose=verbose,
        )

        df = df.rename(
            columns={
                "effective_date": "Effective Date",
                "terminate_date": "Termination Date",
                "subzone": "Subzone",
                "resource_id": "Resource ID",
                "resource_name": "Resource Name",
                "resource_type": "Resource Type",
                "zone": "Zone",
            },
        )

        df = self._filter_active_records(df, as_of)

        return (
            df[
                [
                    "Resource ID",
                    "Resource Name",
                    "Resource Type",
                    "Zone",
                    "Subzone",
                    "Effective Date",
                    "Termination Date",
                ]
            ]
            .sort_values("Effective Date")
            .reset_index(drop=True)
        )

    def get_reserve_subzone_buses(
        self,
        as_of: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the reserve subzone buses data from:
        https://dataminer2.pjm.com/feed/sync_pri_reserves_buses_list/definition
        """
        as_of = utils._handle_date(as_of, tz=self.default_timezone)
        if as_of == "now":
            return self.get_reserve_subzone_buses(
                as_of=pd.Timestamp.now(tz=self.default_timezone),
                verbose=verbose,
            )

        df = self._get_pjm_json(
            "sync_pri_reserves_buses_list",
            start=None,
            params={
                "fields": "effective_date,terminate_date,subzone,pnode_id,pnode_name,pnode_type",
            },
            verbose=verbose,
        )

        df = df.rename(
            columns={
                "effective_date": "Effective Date",
                "terminate_date": "Termination Date",
                "subzone": "Subzone",
                "pnode_id": "Pricing Node ID",
                "pnode_name": "Pricing Node Name",
                "pnode_type": "Pricing Node Type",
            },
        )

        df = self._filter_active_records(df, as_of)
        return (
            df[
                [
                    "Pricing Node ID",
                    "Pricing Node Name",
                    "Pricing Node Type",
                    "Subzone",
                    "Effective Date",
                    "Termination Date",
                ]
            ]
            .sort_values(["Effective Date", "Pricing Node Name"])
            .reset_index(drop=True)
        )

    def get_weight_average_aggregation_definition(
        self,
        as_of: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the weight average aggregation definition data from:
        https://dataminer2.pjm.com/feed/agg_definitions/definition
        """
        as_of = utils._handle_date(as_of, tz=self.default_timezone)
        if as_of == "now":
            return self.get_weight_average_aggregation_definition(
                as_of=pd.Timestamp.now(tz=self.default_timezone),
                verbose=verbose,
            )

        df = self._get_pjm_json(
            "agg_definitions",
            start=None,
            end=None,
            params={
                "fields": "effective_date_ept,terminate_date_ept,agg_pnode_id,agg_pnode_name,bus_pnode_id,bus_pnode_name,bus_pnode_factor",
            },
            verbose=verbose,
        )

        df = df.rename(
            columns={
                "effective_date_ept": "Effective Date",
                "terminate_date_ept": "Termination Date",
                "agg_pnode_id": "Aggregate Node ID",
                "agg_pnode_name": "Aggregate Node Name",
                "bus_pnode_id": "Bus Node ID",
                "bus_pnode_name": "Bus Node Name",
                "bus_pnode_factor": "Bus Node Factor",
            },
        )

        df = self._filter_active_records(df, as_of)

        return (
            df[
                [
                    "Aggregate Node ID",
                    "Aggregate Node Name",
                    "Bus Node ID",
                    "Bus Node Name",
                    "Bus Node Factor",
                    "Effective Date",
                    "Termination Date",
                ]
            ]
            .sort_values(["Effective Date", "Aggregate Node Name"])
            .reset_index(drop=True)
        )
