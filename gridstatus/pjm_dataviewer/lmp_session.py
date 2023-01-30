import re
import sys

import bs4
import pandas as pd

from gridstatus.base import Markets
from gridstatus.pjm_dataviewer.session import Session


class LMPSession(Session):

    URL = "https://dataviewer.pjm.com/dataviewer/pages/public/lmp.jsf"

    def fetch_chart_df(self, tz, verbose=False):
        # enable remaining location_checkboxes
        self._enable_all_locations(
            verbose=verbose,
        )

        # fetch chart data
        return self._fetch_chart_df(
            tz=tz,
            verbose=verbose,
        )

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
            print(
                f"chart_parent_source_id = {chart_parent_source_id}",
                file=sys.stderr,
            )

        if chart_source_id is not None and chart_parent_source_id is not None:
            return {
                "chart_source_id": chart_source_id,
                "chart_parent_source_id": chart_parent_source_id,
            }
        else:
            raise ValueError(
                "Could not get LMP Chart IDs (chart_source_id or chart_parent_source_id)",  # noqa E501
            )

    def _enable_all_locations(
        self,
        verbose=False,
    ):
        # fetch current location_checkboxes and source id
        response = self.fetch(
            {
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": "chart1FrmLmpSelection:dlgLmpSelection",
                "javax.faces.partial.execute": "chart1FrmLmpSelection:dlgLmpSelection",
                "javax.faces.partial.render": "chart1FrmLmpSelection:dlgLmpSelection",
                "chart1FrmLmpSelection:dlgLmpSelection": "chart1FrmLmpSelection:dlgLmpSelection",  # noqa: E501
                "chart1FrmLmpSelection:dlgLmpSelection_contentLoad": "true",
                "chart1FrmLmpSelection": "chart1FrmLmpSelection",
            },
            verbose=verbose,
        )
        update_docs = self._parse_xml_find_all(response.content, "update")

        source_id = None
        location_checkboxes = {}
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
                location_checkboxes[to_check_idx] = checked

        # max_requests is a fallback in case
        # the while loop goes haywire
        max_requests = len(location_checkboxes)
        request_count = 0
        while (
            any(not value for value in location_checkboxes.values())
            and request_count < max_requests
        ):
            to_check_idx = self._get_next_checkbox_idx(location_checkboxes)
            if to_check_idx is not None:
                self._dv_lmp_select_checkbox(
                    source_id,
                    location_checkboxes,
                    to_check_idx,
                    verbose=verbose,
                )
            request_count += 1

    def _fetch_chart_df(self, tz, verbose=False):
        data = self.fetch_chart(verbose=verbose)
        chart_source_id = self._dv_lmp_get_chart_series_source_id(data, verbose=verbose)
        response = self.fetch(
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
            data = self._load_jsonstrings(extensions[0].text)
            df = self._parse_lmp_series(data["allLmpValues"]["lmpSeries"], tz=tz)
        df["_src"] = "dv"
        return df

    def fetch_chart(self, verbose=False):
        """Initial fetch for LMP data in Data Viewer"""
        chart_ids = self._dv_lmp_extract_chart_ids(
            self.initial_response,
            verbose=verbose,
        )
        chart_source_id = chart_ids["chart_source_id"]
        chart_parent_source_id = chart_ids["chart_parent_source_id"]
        return self.fetch(
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
            print(
                f"chart_series_source_id = {chart_series_source_id}",
                file=sys.stderr,
            )

        return chart_series_source_id

    def _dv_lmp_select_checkbox(
        self,
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
                    f":{idx}:{form_source_id}_input"
                ] = "on"
        response = self.fetch(
            params,
            verbose=verbose,
        )
        if verbose:
            print(
                f"response (truncated) = {response.content[0:256]}",
                file=sys.stderr,
            )

    @staticmethod
    def _parse_lmp_series(series, tz):
        dfs = []
        for item in series or []:
            dfs.append(LMPSession._parse_lmp_item(item, tz=tz))
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

        return df

    @staticmethod
    def _parse_lmp_item(item, tz):
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
            .dt.tz_convert(tz=tz)
        )
        df["Location Name"] = item_id
        return df
