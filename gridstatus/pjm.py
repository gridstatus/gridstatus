import io
import json
import math
import re
import sys
import warnings

import bs4
import pandas as pd
import requests
import tqdm

from gridstatus import utils
from gridstatus.base import FuelMix, ISOBase, Markets, NotSupported
from gridstatus.decorators import (
    _get_pjm_archive_date,
    pjm_update_dates,
    support_date_range,
)
from gridstatus.lmp_config import lmp_config

DATAVIEWER_LMP_URL = "https://dataviewer.pjm.com/dataviewer/pages/public/lmp.jsf"

LMP_PARTIAL_RENDER_ID = "formLeftPanel:topLeftGrid"

DV_LMP_RECENT_NUM_DAYS = 3


class PJM(ISOBase):
    """PJM"""

    name = "PJM"
    iso_id = "pjm"
    default_timezone = "US/Eastern"

    interconnection_queue_homepage = (
        "https://www.pjm.com/planning/services-requests/interconnection-queues.aspx"
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

    markets = [
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_HOURLY,
        Markets.DAY_AHEAD_HOURLY,
    ]

    @support_date_range(frequency="365D")
    def get_fuel_mix(self, date, end=None, verbose=False):
        """Get fuel mix for a date or date range  in hourly intervals"""

        if date == "latest":
            mix = self.get_fuel_mix("today")
            latest = mix.iloc[-1]
            time = latest.pop("Time")
            mix_dict = latest.to_dict()
            return FuelMix(time=time, mix=mix_dict, iso=self.name)

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
        )

        mix_df = mix_df.pivot_table(
            index="Time",
            columns="fuel_type",
            values="mw",
            aggfunc="first",
        ).reset_index()

        return mix_df

    @support_date_range(frequency="30D")
    def get_load(self, date, end=None, verbose=False):
        """Returns load at a previous date at 5 minute intervals

        Arguments:
            date (datetime.date, str): date to get load for. must be in last 30 days
        """

        if date == "latest":
            return self._latest_from_today(self.get_load, verbose=verbose)

        # more hourly historical load here: https://dataminer2.pjm.com/feed/hrl_load_metered/definition

        # todo can support a load area
        data = {
            "order": "Asc",
            "sort": "datetime_beginning_utc",
            "isActiveMetadata": "true",
            "fields": "area,datetime_beginning_utc,instantaneous_load",
            "area": "PJM RTO",
        }
        load = self._get_pjm_json(
            "inst_load",
            start=date,
            end=end,
            params=data,
            verbose=verbose,
        )

        load = load.drop("area", axis=1)

        load = load.rename(
            columns={
                "instantaneous_load": "Load",
            },
        )

        load = load[["Time", "Load"]]

        return load

    def get_load_forecast(self, date, verbose=False):
        """Get forecast for today in hourly intervals.

        Updates every Every half hour on the quarter E.g. 1:15 and 1:45

        """

        if date != "today":
            raise NotSupported()

        # todo: should we use the UTC field instead of EPT?
        params = {
            "fields": (
                "evaluated_at_datetime_ept,forecast_area,                   "
                " forecast_datetime_beginning_ept,forecast_load_mw"
            ),
            "forecast_area": "RTO_COMBINED",
        }
        data = self._get_pjm_json(
            "load_frcstd_7_day",
            start=None,
            params=params,
            verbose=verbose,
        )
        data = data.rename(
            columns={
                "evaluated_at_datetime_ept": "Forecast Time",
                "forecast_datetime_beginning_ept": "Time",
                "forecast_load_mw": "Load Forecast",
            },
        )

        data.drop("forecast_area", axis=1, inplace=True)

        data["Forecast Time"] = pd.to_datetime(data["Forecast Time"]).dt.tz_localize(
            self.default_timezone,
        )

        data["Time"] = pd.to_datetime(data["Time"]).dt.tz_localize(
            self.default_timezone,
        )

        return data

    # todo https://dataminer2.pjm.com/feed/load_frcstd_hist/definition
    # def get_historical_forecast(self, date):
    # pass

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
            Markets.REAL_TIME_5_MIN: ["today", "historical"],
            Markets.REAL_TIME_HOURLY: ["today", "historical"],
            Markets.DAY_AHEAD_HOURLY: ["latest", "today", "historical"],
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
        if locations == "hubs":
            locations = self.hub_node_ids

        if location_type:
            location_type = location_type.upper()
            if location_type not in self.location_types:
                raise ValueError(
                    f"location_type must be one of {self.location_types}",
                )

        if date == "latest":
            """Supports DAY_AHEAD_HOURlY, REAL_TIME_5_MIN"""
            return self._latest_lmp_from_today(
                market=market,
                locations=locations,
                location_type=location_type,
                verbose=verbose,
            )
        if utils.is_today(date, tz=self.default_timezone):
            if market not in (
                Markets.REAL_TIME_5_MIN,
                Markets.DAY_AHEAD_HOURLY,
            ):
                raise NotSupported(
                    f"{market.value} is not supported for latest/today",
                )

        recent_threshold = pd.Timestamp.now(tz=self.default_timezone) - pd.Timedelta(
            days=DV_LMP_RECENT_NUM_DAYS,
        )
        dv_df = None
        if utils._handle_date(date, tz=self.default_timezone) >= recent_threshold:
            dv_df = self._get_lmp_via_dv(
                date,
                market,
                end=end,
                locations=locations,
                location_type=location_type,
                verbose=verbose,
            )

        # Figure out whether we need to call the JSON API
        if utils.is_today(date, tz=self.default_timezone):
            fetch_json = market in (Markets.DAY_AHEAD_HOURLY,)
        else:
            fetch_json = True

        json_df = None
        if fetch_json:
            json_df = self._get_lmp_via_pjm_json(
                date,
                market,
                end=end,
                locations=locations,
                location_type=location_type,
                verbose=verbose,
            )

        dfs = [dv_df, json_df]
        dfs = [df for df in dfs if df is not None]

        # deduplicate data, choosing the _src=json when there are duplicates
        # in (time, market, location name)
        df = self._df_deduplicate(
            dfs,
            unique_cols=["Time", "Market", "Location Name"],
            keep_field="_src",
            keep_value="json",
            verbose=verbose,
        )
        df.sort_values("Time", inplace=True)
        df.reset_index(drop=True, inplace=True)
        df = df[
            [
                "Time",
                "Market",
                "Location",
                "Location Name",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
                # "_src",  # uncomment to debug
            ]
        ]
        return df

    def _get_lmp_via_pjm_json(
        self,
        date,
        market: str,
        end=None,
        locations=None,
        location_type=None,
        verbose=False,
    ):
        """Fetches data using the pjm_json api"""
        params = {}

        if market == Markets.REAL_TIME_5_MIN:
            market_endpoint = "rt_fivemin_hrl_lmps"
            market_type = "rt"
        elif market == Markets.REAL_TIME_HOURLY:
            # todo implement location type filter
            market_endpoint = "rt_hrl_lmps"
            market_type = "rt"
        elif market == Markets.DAY_AHEAD_HOURLY:
            # todo implement location type filter
            market_endpoint = "da_hrl_lmps"
            market_type = "da"
        else:
            raise ValueError(
                (
                    "market must be one of REAL_TIME_5_MIN, REAL_TIME_HOURLY,"
                    " DAY_AHEAD_HOURLY"
                ),
            )

        if location_type:
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
        elif locations is not None:
            warnings.warn(
                (
                    "Querying before archive date, so filtering by location will happen"
                    " after all data is downloaded"
                ),
            )

        data = self._get_pjm_json(
            market_endpoint,
            start=date,
            end=end,
            params=params,
            verbose=verbose,
        )

        # finalize data
        data = data.rename(
            columns={
                "pnode_id": "Location",
                "pnode_name": "Location Name",
                "type": "Location Type",
                f"total_lmp_{market_type}": "LMP",
                f"system_energy_price_{market_type}": "Energy",
                f"congestion_price_{market_type}": "Congestion",
                f"marginal_loss_price_{market_type}": "Loss",
            },
        )
        data["Market"] = market.value
        data["_src"] = "json"
        # API cannot filter location type for rt 5 min
        if location_type and market == Markets.REAL_TIME_5_MIN:
            data = data[data["Location Type"] == location_type]
        if locations is not None and locations != "ALL":
            data = utils.filter_lmp_locations(
                data,
                map(int, locations),
            )
        return data

    def _get_pjm_json(
        self,
        endpoint,
        start,
        params,
        end=None,
        start_row=1,
        row_count=100000,
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

            final_params["datetime_beginning_ept"] = (
                start.strftime("%m/%d/%Y %H:%M") + "to" + end.strftime("%m/%d/%Y %H:%M")
            )

        if verbose:
            print(
                f"Retrieving data from {endpoint} with params {final_params}",
            )

        api_key = self._get_key()
        r = self._get_json(
            "https://api.pjm.com/api/v1/" + endpoint,
            params=final_params,
            headers={"Ocp-Apim-Subscription-Key": api_key},
        )

        if "errors" in r:
            raise RuntimeError(r["errors"])

        # todo should this be a warning?
        if r["totalRows"] == 0:
            raise RuntimeError("No data found for query")

        df = pd.DataFrame(r["items"])

        num_pages = math.ceil(r["totalRows"] / row_count)
        if num_pages > 1:
            to_add = [df]
            for page in tqdm.tqdm(range(1, num_pages), initial=1, total=num_pages):
                next_url = [x for x in r["links"] if x["rel"] == "next"][0]["href"]
                r = self._get_json(
                    next_url,
                    headers={
                        "Ocp-Apim-Subscription-Key": api_key,
                    },
                )
                to_add.append(pd.DataFrame(r["items"]))

            df = pd.concat(to_add)

        if "datetime_beginning_utc" in df.columns:
            df["Time"] = (
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
                df["Time"].dt.strftime(
                    "%Y-%m-%d %H:%M",
                )
                != end.strftime("%Y-%m-%d %H:%M")
            ]

        return df

    def get_interconnection_queue(self):
        r = requests.post(
            "https://services.pjm.com/PJMPlanningApi/api/Queue/ExportToXls",
            headers={
                # unclear if this key changes. obtained from https://www.pjm.com/dist/interconnectionqueues.71b76ed30033b3ff06bd.js
                "api-subscription-key": "E29477D0-70E0-4825-89B0-43F460BF9AB4",
                "Host": "services.pjm.com",
                "Origin": "https://www.pjm.com",
                "Referer": "https://www.pjm.com/",
            },
        )
        queue = pd.read_excel(io.BytesIO(r.content))

        queue["Capacity (MW)"] = queue[["MFO", "MW In Service"]].min(axis=1)

        rename = {
            "Queue Number": "Queue ID",
            "Name": "Project Name",
            "County": "County",
            "State": "State",
            "Transmission Owner": "Transmission Owner",
            "Queue Date": "Queue Date",
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

    def _get_key(self):
        settings = self._get_json(
            "https://dataminer2.pjm.com/config/settings.json",
        )

        return settings["subscriptionKey"]

    @staticmethod
    def _new_dv_session(session, verbose=False):
        if verbose:
            print(f"GET {DATAVIEWER_LMP_URL}")
        response = session.get(DATAVIEWER_LMP_URL)

        html = response.content
        doc = bs4.BeautifulSoup(html, "html.parser")

        scripts = doc.find_all("script", {"nonce": True})

        nonce = None
        for script in scripts:
            nonce = script["nonce"]
            if nonce is not None:
                break

        view_state = None
        view_states = doc.find_all(
            "input",
            {"type": "hidden", "name": "javax.faces.ViewState"},
        )
        for view_state in view_states:
            view_state = view_state["value"]
            if view_state is not None:
                break

        if nonce is None or view_state is None:
            return None
        else:
            return {
                "nonce": nonce,
                "view_state": view_state,
                "session": session,
            }

    def _dv_lmp_fetch_data(self, dv_session, verbose=False):
        data = self._dv_lmp_init_fetch(dv_session, verbose=verbose)
        chart_series_source_id = self._dv_lmp_get_chart_series_source_id(
            data,
            verbose=verbose,
        )

        # fetch current included_locations and source id
        (
            included_locations,
            included_locations_source_id,
        ) = self._dv_lmp_fetch_included_locations_context(
            dv_session,
            verbose=verbose,
        )

        # enable remaining included_locations
        self._dv_lmp_include_all_locations(
            dv_session,
            included_locations_source_id,
            included_locations,
            verbose=verbose,
        )

        # fetch chart data
        return self._dv_lmp_fetch_chart_df(
            dv_session,
            chart_series_source_id,
            verbose=verbose,
        )

    def _dv_lmp_fetch_chart_df(self, dv_session, chart_source_id, verbose=False):
        response = self._dv_lmp_fetch(
            dv_session,
            {
                "chart1": "chart1",
                "chart1:chart1valueDataTable_scrollState": "0,0",
                chart_source_id: chart_source_id,
                "chart1:typeSelection": "BOTH",
                "javax.faces.partial.ajax": "true",
                "javax.faces.partial.execute": "@all",
                "javax.faces.partial.render": "chart1:selectAggregates chart1:valuesPanel",  # noqa: E501
                "javax.faces.source": chart_source_id,
            },
            verbose=verbose,
        )
        extensions = self._parse_xml_find_all(response.content, "extension")
        if len(extensions) > 0:
            data = self._json_loads_nested_jsonstrings(extensions[0].text)
            df = self._parse_lmp_series(data["allLmpValues"]["lmpSeries"])
        df["_src"] = "dv"
        return df

    def _dv_lmp_include_all_locations(
        self,
        dv_session,
        filters_source_id,
        filters,
        verbose=False,
    ):
        # max_requests is a fallback in case
        # the while loop goes haywire
        max_requests = len(filters)
        request_count = 0
        while (
            any(not value for value in filters.values())
            and request_count < max_requests
        ):
            to_check_idx = self._get_next_checkbox_idx(filters)
            if to_check_idx is not None:
                self._dv_lmp_select_checkbox(
                    dv_session,
                    filters_source_id,
                    filters,
                    to_check_idx,
                    verbose=verbose,
                )
            request_count += 1

    def _dv_lmp_init_fetch(self, dv_session, verbose=False):
        """Initial fetch for LMP data in Data Viewer"""
        chart_source_id = dv_session["chart_source_id"]
        chart_parent_source_id = dv_session["chart_parent_source_id"]
        return self._dv_lmp_fetch(
            dv_session,
            {
                chart_parent_source_id: chart_parent_source_id,
                chart_source_id: chart_source_id,
                "javax.faces.partial.ajax": "true",
                "javax.faces.partial.execute": "@all",
                "javax.faces.partial.render": "tabPanel",
                "javax.faces.source": chart_source_id,
            },
            verbose=verbose,
        )

    def _dv_lmp_get_chart_series_source_id(self, response, verbose):
        """Retrieves source ID from update response,
        to be used later for fetching series data"""
        html_update_docs = self._parse_xml_find_all(response.content, "update")
        html_update_doc = html_update_docs[0]
        chart_series_source_id = None
        scripts = html_update_doc.find_all("script")
        for script in scripts:
            script_id = script.get("id", None)
            if script_id is not None:
                chart_series_source_id = script_id
                break
        if verbose:
            print(f"chart_series_source_id = {chart_series_source_id}", file=sys.stderr)

        return chart_series_source_id

    def _dv_lmp_fetch_included_locations_context(self, dv_session, verbose=False):
        """
        Returns a tuple:

        * a dictionary checkboxes[int] = bool where
          the key is a numeric index, and the value is the
           checkbox status, i.e. whether the location is included
        * the form source_id to be used in later requests
        """
        params = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "chart1FrmLmpSelection:dlgLmpSelection",
            "javax.faces.partial.execute": "chart1FrmLmpSelection:dlgLmpSelection",
            "javax.faces.partial.render": "chart1FrmLmpSelection:dlgLmpSelection",
            "chart1FrmLmpSelection:dlgLmpSelection": "chart1FrmLmpSelection:dlgLmpSelection",  # noqa: E501
            "chart1FrmLmpSelection:dlgLmpSelection_contentLoad": "true",
            "chart1FrmLmpSelection": "chart1FrmLmpSelection",
        }
        response = self._dv_lmp_fetch(
            dv_session,
            params,
            verbose=verbose,
        )
        update_docs = self._parse_xml_find_all(response.content, "update")
        source_id = None
        checkboxes = {}
        for update_doc in update_docs:
            checkbox_elems = update_doc.find_all("input", {"type": "checkbox"})
            for checkbox_elem in checkbox_elems:
                checked = checkbox_elem.get("checked", None) is not None
                checkbox_name = checkbox_elem.get("name")
                matches = re.search(r":([0-9]+):(j_.+)$", checkbox_name)
                if matches is not None:
                    to_check_idx = int(matches.group(1))
                    if source_id is None:
                        source_id = re.sub(r"_input$", "", matches.group(2))
                checkboxes[to_check_idx] = checked

        return (
            checkboxes,
            source_id,
        )

    def _dv_lmp_select_checkbox(
        self,
        dv_session,
        form_source_id,
        checkboxes,
        to_check_idx,
        verbose=False,
    ):
        # toggle checkbox
        params = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": f"chart1FrmLmpSelection:tblBusAggregates:{to_check_idx}:{form_source_id}",  # noqa: E501
            "javax.faces.partial.execute": f"chart1FrmLmpSelection:tblBusAggregates:{to_check_idx}:{form_source_id}",  # noqa: E501
            "javax.faces.partial.render": "globalMessages",
            "javax.faces.behavior.event": "change",
            "javax.faces.partial.event": "change",
            "chart1FrmLmpSelection": "chart1FrmLmpSelection",
            "chart1FrmLmpSelection:tblBusAggregates:name:filter": "",
            "chart1FrmLmpSelection:tblBusAggregates:voltage:filter": "",
            "chart1FrmLmpSelection:tblBusAggregates:station:filter": "",
            "chart1FrmLmpSelection:tblBusAggregates_scrollState": "0,0",
        }
        checkboxes[to_check_idx] = True
        for idx, is_checked in checkboxes.items():
            if is_checked:
                params[
                    f"chart1FrmLmpSelection:tblBusAggregates"
                    f":{idx}:{form_source_id}_input"  # noqa: E501
                ] = "on"
        response = self._dv_lmp_fetch(
            dv_session,
            params,
            verbose=verbose,
        )
        if verbose:
            print(f"response (truncated) = {response.content[0:256]}", file=sys.stderr)

    def _get_lmp_via_dv(
        self,
        date,
        market: str,
        end=None,
        locations="hubs",
        location_type=None,
        verbose=False,
    ):
        """Get latest LMP data from Data Viewer, which includes RT & DA"""
        with requests.session() as session:
            initial_fetch = session.get(DATAVIEWER_LMP_URL)

            dv_session = self._new_dv_session(session, verbose=verbose)
            if dv_session is None:
                raise ValueError("Could not create new DV Session")

            chart_ids = self._dv_lmp_extract_chart_ids(initial_fetch, verbose=verbose)
            if chart_ids is None:
                raise ValueError("Could not get LMP Chart IDs")
            dv_session.update(chart_ids)

            df = self._dv_lmp_fetch_data(dv_session, verbose=verbose)
            if market in (Markets.DAY_AHEAD_HOURLY, Markets.REAL_TIME_5_MIN):
                df = df[df["Market"] == market.value]

            if end is None:
                df = df[df["Time"].dt.date == date.date()]
            else:
                df = df[
                    df["Time"].dt.date.between(
                        date.date(),
                        end.date(),
                        inclusive="both",
                    )
                ]
            return df

    def _dv_lmp_fetch(self, dv_session, params, verbose=False):
        """Fetch with dv_session view state and nonce"""
        params.update(
            {
                "javax.faces.ViewState": dv_session["view_state"],
                "primefaces.nonce": dv_session["nonce"],
            },
        )
        if verbose:
            print(f"POST {DATAVIEWER_LMP_URL} with {params}", file=sys.stderr)
        return dv_session["session"].post(
            DATAVIEWER_LMP_URL,
            data=params,
        )

    @staticmethod
    def _parse_xml_find_all(xml_string: str, *args, **kwargs):
        """From a string, extract <update> CDATA content
        and return a list of BeautifulSoup-parsed HTML."""
        xml_doc = bs4.BeautifulSoup(xml_string, "xml")
        elems = xml_doc.find_all(*args, **kwargs)
        return [bs4.BeautifulSoup(elem.text, "html.parser") for elem in elems]

    @staticmethod
    def _get_next_checkbox_idx(checkboxes):
        to_check_idx = None
        for idx, is_checked in checkboxes.items():
            if not is_checked:
                to_check_idx = idx
                break
        return to_check_idx

    def _json_loads_nested_jsonstrings(self, text):
        """Load JSON where values are JSON-strings"""
        doc = dict((k, json.loads(v)) for k, v in json.loads(text).items())
        return doc

    def _parse_lmp_series(self, series):
        dfs = []
        for item in series or []:
            dfs.append(self._parse_lmp_item(item))
        if len(dfs) > 0:
            df = pd.concat(dfs)
        else:
            df = pd.DataFrame()
        df = df.rename(
            columns={
                "lmp": "LMP",
                "mlcValue": "Loss",
                "mccValue": "Congestion",
            },
        )
        df["Energy"] = df["LMP"] - df["Loss"] - df["Congestion"]

        # Pricing Node data provides mappings for Location IDs and Location Types
        pnode_ids = self.get_pnode_ids()

        # Location IDs
        location_ids = dict(
            zip(pnode_ids["pnode_name"], pnode_ids["pnode_id"].astype(int)),
        )
        df["Location"] = df["Location Name"].apply(
            lambda location_name: location_ids.get(location_name, pd.NA),
        )

        # Location Types
        location_types = dict(zip(pnode_ids["pnode_name"], pnode_ids["pnode_subtype"]))
        df["Location Type"] = df["Location Name"].apply(
            lambda location_name: location_types.get(location_name, pd.NA),
        )

        return df

    def _parse_lmp_item(self, item):
        item_id = item["id"]
        if item_id.endswith(" (DA)"):
            market = Markets.DAY_AHEAD_HOURLY
            item_id = item_id.replace(" (DA)", "")
        else:
            market = Markets.REAL_TIME_5_MIN
        item_data = item["data"]
        df = pd.DataFrame(item_data)
        df["Market"] = market.value
        df["Time"] = (
            pd.to_datetime(df["timestamp"], unit="ms")
            .dt.tz_localize(tz="UTC")
            .dt.tz_convert(tz=self.default_timezone)
        )
        df["Location Name"] = item_id
        return df

    def __debug_nested_data(self, data):
        pd.options.display.width = 0  # DEBUG
        pairs = []
        for k1, v1 in data.items():
            if isinstance(v1, dict):
                for k2, v2 in v1.items():
                    if isinstance(v2, list) and len(v2) > 0:
                        pairs.append(
                            (
                                k1,
                                k2,
                            ),
                        )

        for k1, k2 in pairs:
            df = self._parse_lmp_series(data[k1][k2])
            print(f"self._parse_lmp_series(data['{k1}']['{k2}'])")
            print(df)

    @staticmethod
    def _dv_lmp_extract_chart_ids(response, verbose=False):
        html = response.content
        doc = bs4.BeautifulSoup(html, "html.parser")
        scripts = doc.find_all("script", {"nonce": True})

        chart_source_id = None
        menu_form = doc.find("form", {"id": "menuForm"})
        grandparent = menu_form.parent.parent
        hidden_elements = grandparent.find_all("input", {"type": "hidden"})
        j_hidden_elements = [
            elem for elem in hidden_elements if elem["name"].startswith("j_")
        ]
        seed = j_hidden_elements[0]["name"]
        for script in scripts:
            lines = script.text.split("\n")
            for line in lines:
                match = re.search(rf's:"({seed}[^"]+)"', line)
                if match is not None:
                    chart_source_id = match.group(1)
                    chart_parent_source_id = chart_source_id.split(":")[0]

        if verbose:
            print(f"chart_source_id = {chart_source_id}", file=sys.stderr)
            print(f"chart_parent_source_id = {chart_parent_source_id}", file=sys.stderr)

        if chart_source_id is not None and chart_parent_source_id is not None:
            return {
                "chart_source_id": chart_source_id,
                "chart_parent_source_id": chart_parent_source_id,
            }

    @staticmethod
    def _df_deduplicate(dfs, unique_cols, keep_field, keep_value, verbose=False):
        """Concatenate dataframes and deduplicate based on a list of columns,
        keeping keep_field=keep_value.
        """
        df = pd.concat(dfs)
        if verbose:
            print(f"Starting with {len(df)} rows", file=sys.stderr)

        keep_fields = sorted(df[keep_field].unique().tolist())
        if keep_value in keep_fields:
            if keep_fields[0] == keep_value:
                dedupe_keep = "first"
            elif keep_fields[-1] == keep_value:
                dedupe_keep = "last"
            else:
                raise ValueError(
                    f"keep_value {repr(keep_value)} must be "
                    f"first or last for de-duplication to work: {keep_fields}",
                )

            # Extract subset without duplicates
            df.sort_values(keep_field, ascending=True, inplace=True)
            df.drop_duplicates(subset=unique_cols, keep=dedupe_keep, inplace=True)

        df.reset_index(inplace=True, drop=True)

        if verbose:
            print(f"Ending with {len(df)} rows", file=sys.stderr)

        return df


"""
import gridstatus
iso = gridstatus.PJM()
nodes = iso.get_pnode_ids()
zones = nodes[nodes["pnode_subtype"] == "ZONE"]
zone_ids = zones["pnode_id"].tolist()
iso.get_historical_lmp("Oct 1, 2022", "DAY_AHEAD_HOURLY", locations=zone_ids)
pnode_id
"""


if __name__ == "__main__":
    import gridstatus

    pd.options.display.width = 0
    iso = gridstatus.PJM()
    verbose = False

    two_days_ago = pd.Timestamp.now(tz=iso.default_timezone) - pd.Timedelta(days=2)
    two_weeks_ago = pd.Timestamp.now(tz=iso.default_timezone) - pd.Timedelta(weeks=2)

    combos = (
        (Markets.DAY_AHEAD_HOURLY, "latest"),
        (Markets.DAY_AHEAD_HOURLY, "today"),
        (Markets.DAY_AHEAD_HOURLY, two_days_ago),
        (Markets.DAY_AHEAD_HOURLY, two_weeks_ago),
        (Markets.REAL_TIME_5_MIN, "latest"),
        (Markets.REAL_TIME_5_MIN, "today"),
        (Markets.REAL_TIME_5_MIN, two_days_ago),
        (Markets.REAL_TIME_5_MIN, two_weeks_ago),
        (Markets.REAL_TIME_HOURLY, "latest"),  # not supported
        (Markets.REAL_TIME_HOURLY, "today"),  # not supported
        (Markets.REAL_TIME_HOURLY, two_days_ago),
        (Markets.REAL_TIME_HOURLY, two_weeks_ago),
    )

    for market, date in combos:
        try:
            print(f"date:{date}, market: {market}")
            df = iso.get_lmp(date=date, market=market, verbose=verbose)
            print(f"{len(df)} rows:")
            if date == "latest":
                print(df.to_string())
            else:
                print(df.head())
        except Exception as e:
            print("Error:", e, file=sys.stderr)
