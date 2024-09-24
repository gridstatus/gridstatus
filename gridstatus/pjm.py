import math
import os
import warnings
from datetime import datetime
from typing import BinaryIO, Optional

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
from gridstatus.gs_logging import log
from gridstatus.lmp_config import lmp_config

# PJM requires retries because the API is flaky
DEFAULT_RETRIES = 3


class PJM(ISOBase):
    """PJM"""

    name = "PJM"
    iso_id = "pjm"
    default_timezone = "US/Eastern"

    interconnection_homepage = (
        "https://www.pjm.com/planning/service-requests/services-request-status"
    )

    location_types = [
        "ZONE",
        "LOAD",
        "GEN",
        "AGGREGATE",
        "INTERFACE",
        "EXT",
        "HUB",
        "EHV",
        "TIE",
        "RESIDUAL_METERED_EDC",
    ]

    hub_node_ids = [
        "51217",
        "116013751",
        "35010337",
        "34497151",
        "34497127",
        "34497125",
        "33092315",
        "33092313",
        "33092311",
        "4669664",
        "51288",
        "51287",
    ]

    zone_node_ids = [
        "1",
        "3",
        "51291",
        "51292",
        "51293",
        "51295",
        "51296",
        "51297",
        "51298",
        "51299",
        "51300",
        "51301",
        "7633629",
        "8394954",
        "8445784",
        "33092371",
        "34508503",
        "34964545",
        "37737283",
        "116013753",
        "124076095",
        "970242670",
        "1709725933",
    ]

    price_node_ids = [
        "5021703",
        "5021704",
        "5021723",
        "5021724",
        "93354015",
        "93354017",
        "93354019",
        "34887765",
        "34887767",
        "34887769",
        "34887771",
        "34887773",
        "34887775",
        "34887777",
        "2156111970",
        "34887779",
        "34887781",
        "34887783",
        "34887787",
        "34887789",
        "34887791",
        "34887793",
        "74008711",
        "34887819",
        "34887821",
        "34887823",
        "2156112027",
        "34887845",
        "1439658151",
        "34887847",
        "34887849",
        "74008743",
        "34887851",
        "34887853",
        "1123180720",
        "34887857",
        "1123180722",
        "34887859",
        "1123180723",
        "34887861",
        "1123180721",
        "34887871",
        "34887873",
        "34887887",
        "34887889",
        "34887891",
        "34887893",
        "34887895",
        "1207075032",
        "34887897",
        "34887899",
        "34887901",
        "34887911",
        "34887913",
        "34887915",
        "34887917",
        "34887923",
        "1097732340",
        "34887925",
        "34887927",
        "34887929",
        "34887935",
        "34887937",
        "34887939",
        "34887941",
        "34887949",
        "34887951",
        "34887953",
        "34887955",
        "1552845076",
        "34887957",
        "1552845077",
        "34887959",
        "1552845078",
        "34887961",
        "34887963",
        "34887965",
        "34887967",
        "34887969",
        "34887971",
        "1305131304",
        "34887977",
        "1305131306",
        "34887993",
        "34887997",
        "34887999",
        "34888001",
        "119118151",
        "2156114262",
        "1379266905",
        "1379266906",
        "1097732449",
        "1292915048",
        "1132294512",
        "1132294513",
        "1132294514",
        "1132294515",
        "1552845186",
        "106856851",
        "2156112284",
        "1305131444",
        "119118263",
        "119118265",
        "119118267",
        "119118269",
        "119118271",
        "106856905",
        "2156110343",
        "40243747",
        "71856675",
        "40243749",
        "71856677",
        "40243751",
        "40243753",
        "40243755",
        "40243757",
        "40243759",
        "40243761",
        "40243763",
        "40243765",
        "40243767",
        "40243769",
        "40243771",
        "40243773",
        "40243775",
        "40243777",
        "40243779",
        "1248991825",
        "1248991826",
        "1248991827",
        "40243801",
        "40243803",
        "40243805",
        "40243807",
        "135389793",
        "135389819",
        "40243837",
        "1666116222",
        "1666116223",
        "1666116224",
        "1666116225",
        "40243839",
        "1356163765",
        "38367965",
        "38367967",
        "38367969",
        "1218915048",
        "1218915049",
        "1218915050",
        "1218915051",
        "1388614399",
        "2156110624",
        "32418611",
        "32418613",
        "32418615",
        "32418617",
        "1388614460",
        "1084390238",
        "1218915186",
        "1218915187",
        "1369011076",
        "1369011077",
        "1369011078",
        "1268571042",
        "98370477",
        "1084390354",
        "93140",
        "93141",
        "93142",
        "93143",
        "93144",
        "93145",
        "98370523",
        "98370525",
        "98370527",
        "98370529",
        "98370531",
        "98370533",
        "98370535",
        "1552843818",
        "57967665",
        "1552843913",
        "1552843915",
        "1552843916",
        "1356162213",
        "1356162214",
        "50401",
        "48934161",
        "48934163",
        "48934165",
        "48934167",
        "48934169",
        "36181299",
        "50488",
        "50489",
        "50490",
        "36181325",
        "2156113262",
        "50542",
        "50543",
        "50557",
        "50558",
        "2156113284",
        "50578",
        "50579",
        "50581",
        "50621",
        "50622",
        "87901631",
        "50628",
        "50629",
        "50654",
        "50655",
        "50659",
        "50660",
        "50661",
        "50662",
        "2156111333",
        "1048047",
        "1048049",
        "1048050",
        "1048051",
        "1048052",
        "21601782",
        "21601783",
        "21601784",
        "21601785",
        "21601786",
        "50695",
        "50696",
        "50697",
        "50698",
        "50699",
        "2041990671",
        "123901459",
        "123901461",
        "123901463",
        "123901465",
        "123901467",
        "50715",
        "50716",
        "50717",
        "50727",
        "50728",
        "50729",
        "50730",
        "2156113457",
        "2156113469",
        "50764",
        "2156113488",
        "50769",
        "50770",
        "50771",
        "50777",
        "50778",
        "50779",
        "123901537",
        "123901539",
        "123901543",
        "31020649",
        "123901545",
        "31020651",
        "31020653",
        "50809",
        "50810",
        "50811",
        "50812",
        "50813",
        "50814",
        "50817",
        "50818",
        "1165479564",
        "2156109456",
        "50887",
        "50888",
        "50893",
        "50894",
        "50911",
        "50915",
        "32417525",
        "32417527",
        "2156111608",
        "1218914041",
        "1218914042",
        "1218914043",
        "32417545",
        "32417547",
        "1183231801",
        "32417599",
        "32417601",
        "32417603",
        "32417605",
        "51019",
        "51020",
        "51021",
        "1348263767",
        "32417625",
        "32417627",
        "32417629",
        "32417631",
        "32417633",
        "32417635",
        "1379268471",
        "1379268472",
        "1379268473",
        "1379268474",
        "1379268475",
        "1379268476",
        "63381383",
        "63381385",
        "2156111770",
        "2156109760",
        "2156109763",
        "2156109765",
        "2156109768",
        "2156109772",
        "2156109777",
        "5021665",
        "5021666",
        "5021667",
        "2156111847",
        "93353961",
        "93353963",
        "93353965",
    ]

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

    def __init__(self, api_key=None, retries=DEFAULT_RETRIES) -> None:
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
    def get_fuel_mix(self, date, end=None, verbose=False):
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
    def get_load(self, date, end=None, verbose=False):
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

        # pivot on area
        load = load.pivot_table(
            index=["Time", "Interval Start"],
            columns="area",
            values="instantaneous_load",
            aggfunc="first",
        ).reset_index()

        # round to nearest minute
        # need to round in utc time
        load["Interval Start"] = (
            load["Interval Start"]
            .dt.tz_convert("UTC")
            .dt.round("1min")
            .dt.tz_convert(self.default_timezone)
        )
        load["Time"] = load["Interval Start"]

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
    def get_load_forecast(self, date, end=None, verbose=False):
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
    def get_load_forecast_historical(self, date, end=None, verbose=False):
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

    def _handle_load_forecast(self, data):
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

    def get_pnode_ids(self):
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
        date,
        market: str,
        end=None,
        locations="hubs",
        location_type=None,
        verbose=False,
    ):
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
            # todo implemlement location type filter
            market_endpoint = "rt_hrl_lmps"
            market_type = "rt"
            interval_duration_min = 60
        elif market == Markets.DAY_AHEAD_HOURLY:
            # todo implemlement location type filter
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
                params[
                    "fields"
                ] = "congestion_price_rt,datetime_beginning_ept,datetime_beginning_utc,marginal_loss_price_rt,occ_check,pnode_id,pnode_name,ref_caseid_used_multi_interval,total_lmp_rt,type"  # noqa: E501
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

        # the pnode_name in the lmp data isn't always full name
        # so, let drop it for now
        # will get full name by merge with pnode data later
        data = data.drop(columns=["pnode_name"])

        p_nodes = self.get_pnode_ids()[["pnode_id", "pnode_name", "voltage_level"]]

        # this is needed because rt_unverified_fivemin_lmps
        # doesn't have short name
        # so we need to extract it from full name
        # other LMP datasets have but do it this way
        # for consistent logic
        def extract_short_name(row):
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

        p_nodes["pnode_short_name"] = p_nodes.apply(extract_short_name, axis=1)

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

        data = data.merge(p_nodes)

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

    def _get_pjm_json(
        self,
        endpoint,
        start,
        params,
        end=None,
        start_row=1,
        row_count=50000,
        interval_duration_min=None,
        filter_timestamp_name="datetime_beginning",
        verbose=False,
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

        msg = f"Retrieving data from {endpoint} with params {params_to_log}"
        log(msg, verbose)

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
                pd.to_datetime(df["datetime_beginning_utc"])
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
                    pd.to_datetime(df["datetime_ending_utc"])
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

    def get_raw_interconnection_queue(self, verbose=False) -> BinaryIO:
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

    def get_interconnection_queue(self, verbose=False):
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
            "Interim Interconnection Service Agreement",
            "Interim/Interconnection Service Agreement Status",
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
    def get_solar_forecast(self, date, end=None, verbose=False):
        """
        Retrieves the hourly solar forecast including behind the meter solar forecast.
        From:  https://dataminer2.pjm.com/feed/hourly_solar_power_forecast/definition
        Only available in past 30 days
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

    def _parse_solar_forecast(self, df):
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
    def get_wind_forecast(self, date, end=None, verbose=False):
        """
        Retrieves the hourly wind forecast
        From: https://dataminer2.pjm.com/feed/hourly_wind_power_forecast/definition
        Only available in past 30 days
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

    def _parse_wind_forecast(self, df):
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
    def get_gen_outages_by_type(self, date, end=None, verbose=False):
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

    def _parse_gen_outages_by_type(self, df):
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
    def get_projected_rto_statistics_at_peak(self, date, end=None, verbose=False):
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
            filter_timestamp_name="projected_peak_datetime",
            verbose=verbose,
        )

        return self._handle_projected_rto_statistics_at_peak(df)

    def _handle_projected_rto_statistics_at_peak(self, df):
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
    def get_projected_area_statistics_at_peak(self, date, end=None, verbose=False):
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
            filter_timestamp_name="projected_peak_datetime",
            verbose=verbose,
        )

        return self._handle_projected_area_statistics_at_peak(df)

    def _handle_projected_area_statistics_at_peak(self, df):
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

    def to_local_datetime(self, df, column_name):
        return pd.to_datetime(df[column_name]).dt.tz_localize(
            self.default_timezone,
        )

    @support_date_range(frequency=None)
    def get_solar_generation_5_min(
        self,
        date: str | pd.Timestamp,
        end: Optional[str | pd.Timestamp] = None,
        verbose: Optional[bool] = False,
    ):
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

    def _parse_solar_generation_5_min(self, df: pd.DataFrame):
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
        end: Optional[str | pd.Timestamp] = None,
        verbose: Optional[bool] = False,
    ):
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

    def _parse_wind_generation_instantaneous(self, df: pd.DataFrame):
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
        end: Optional[str | pd.Timestamp] = None,
        verbose: Optional[bool] = False,
    ):
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

    def _parse_operational_reserves(self, df: pd.DataFrame):
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
        end: Optional[str | pd.Timestamp] = None,
        verbose: Optional[bool] = False,
    ):
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

    def _parse_transfer_interface_information_5_min(self, df: pd.DataFrame):
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
        end: Optional[str | pd.Timestamp] = None,
        verbose: Optional[bool] = False,
    ):
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

    def _parse_transmission_limits(self, df: pd.DataFrame):
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
        end: Optional[str | pd.Timestamp] = None,
        verbose: Optional[bool] = False,
    ):
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

    def _parse_solar_generation_by_area(self, df: pd.DataFrame):
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
        end: Optional[str | pd.Timestamp] = None,
        verbose: Optional[bool] = False,
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

    def _parse_wind_generation_by_area(self, df: pd.DataFrame):
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
        end: Optional[str | pd.Timestamp] = None,
        verbose: Optional[bool] = False,
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

    def _parse_dam_as_market_results(self, df: pd.DataFrame):
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
        end: Optional[str | pd.Timestamp] = None,
        verbose: Optional[bool] = False,
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

    def _parse_real_time_as_market_results(self, df: pd.DataFrame):
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
    def get_load_metered_hourly(self, date, end=None, verbose=False):
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

    def _parse_load_metered_hourly(self, df):
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
    def get_forecasted_generation_outages(self, date, end=None, verbose=False):
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

    def _parse_forecasted_generation_outages(self, df):
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
