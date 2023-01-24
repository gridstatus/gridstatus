import json
import re
import sys

import bs4
import pandas as pd
import requests

from gridstatus.base import Markets

DATAVIEWER_LMP_URL = "https://dataviewer.pjm.com/dataviewer/pages/public/lmp.jsf"

LMP_PARTIAL_RENDER_ID = "formLeftPanel:topLeftGrid"

DV_LMP_RECENT_NUM_DAYS = 3


class PJMDataViewer:
    def __init__(self, pjm):
        self.pjm = pjm

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
            raise ValueError("Could not create new DV Session")
        else:
            return {
                "nonce": nonce,
                "view_state": view_state,
                "session": session,
            }

    def _dv_lmp_fetch_data(self, session, verbose=False):
        initial_fetch = session.get(DATAVIEWER_LMP_URL)

        dv_session = self._new_dv_session(session, verbose=verbose)
        chart_ids = self._dv_lmp_extract_chart_ids(initial_fetch, verbose=verbose)
        dv_session.update(chart_ids)

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
            df = self._dv_lmp_fetch_data(session, verbose=verbose)
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
        pnode_ids = self.pjm.get_pnode_ids()

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
            .dt.tz_convert(tz=self.pjm.default_timezone)
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
        else:
            raise ValueError(
                "Could not get LMP Chart IDs (chart_source_id or chart_parent_source_id)",  # noqa E501
            )

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
