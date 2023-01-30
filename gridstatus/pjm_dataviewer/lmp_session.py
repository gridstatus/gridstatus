import json
import re
import sys

import bs4
import pandas as pd

from gridstatus.base import Markets
from gridstatus.pjm_dataviewer.session import Session


class LMPSession(Session):

    URL = "https://dataviewer.pjm.com/dataviewer/pages/public/lmp.jsf"

    def __init__(self):
        super().__init__()
        self.chart_source_id = None
        self.chart_parent_source_id = None

    def enable_all_locations_and_fetch_chart_df(self, verbose=False):
        # enable remaining location_checkboxes
        self._enable_all_locations(
            verbose=verbose,
        )
        # fetch chart data
        return self._fetch_chart_df(
            verbose=verbose,
        )

    def start(self, verbose=False):
        """Starts the LMP session, and extracts some information
        from the initial response for future LMP DV calls.
        """
        response = super().start(verbose=verbose)

        """Extract two element IDs which are important for future API calls"""
        html = response.content
        doc = bs4.BeautifulSoup(html, "html.parser")

        """
        1) Find menuForm
        2) Find all hidden elements who share a grandparent element with menuForm
        3) Set ${seed_prefix} to the name of first hidden elements with "j_" prefix
        4) Find seed in javascript searching for /s:"(${seed_prefix}.*)"/
        5) chart_source_id is first matching capture group, e.g. "j_idt231:j_idt232"
        6) chart_parent_source_id is the first token with colons as the delimiter,
           e.g. "j_idt231"
        """
        menu_form = doc.find("form", {"id": "menuForm"})
        if menu_form is not None:
            grandparent = menu_form.parent.parent
            hidden_elements = grandparent.find_all("input", {"type": "hidden"})
            j_hidden_elements = [
                elem for elem in hidden_elements if elem["name"].startswith("j_")
            ]
            if len(j_hidden_elements) > 0:
                seed_prefix = j_hidden_elements[0]["name"]
                all_lines = [
                    script.text.split("\n")
                    for script in doc.find_all("script", {"nonce": True})
                ]
                for lines in all_lines:
                    for line in lines:
                        match = re.search(rf's:"({seed_prefix}.+?)"', line)
                        if match is not None:
                            self.chart_source_id = match.group(1)
                            self.chart_parent_source_id = match.group(1).split(":")[0]
                            break

        if verbose:
            print(f"self.chart_source_id = {self.chart_source_id}", file=sys.stderr)
            print(
                f"self.chart_parent_source_id = {self.chart_parent_source_id}",
                file=sys.stderr,
            )

        if self.chart_source_id is None or self.chart_parent_source_id is None:
            raise ValueError(
                "Could not get LMP Chart IDs (chart_source_id or chart_parent_source_id)",  # noqa E501
            )

    def _enable_all_locations(
        self,
        verbose=False,
    ):
        """Fetches the current location checkboxes, enables all of them.
        This makes N calls for N remaining unchecked locations."""

        # Fetch current selected locations
        response = self.post_api(
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

        # Extract location checkbox statuses, and form_source_id to be used to update
        # the checkboxes later on
        location_checkboxes = {}
        form_source_id = None
        for update_doc in update_docs:
            checkbox_elems = update_doc.find_all("input", {"type": "checkbox"})
            for checkbox_elem in checkbox_elems:
                checked = checkbox_elem.get("checked", None) is not None
                checkbox_name = checkbox_elem.get("name")
                matches = re.search(r":([0-9]+):(j_.+)$", checkbox_name)
                if matches is not None:
                    next_checkbox_index = int(matches.group(1))
                    if form_source_id is None:
                        form_source_id = re.sub(r"_input$", "", matches.group(2))
                location_checkboxes[next_checkbox_index] = checked

        # Check all the checkboxes, setting max_requests to the total number
        # of location checkboxes to prevent infinite loop
        max_requests = len(location_checkboxes)
        request_count = 0
        while (
            any(not value for value in location_checkboxes.values())
            and request_count < max_requests
        ):
            # find the next unchecked checkbox
            next_checkbox_index = next(
                iter(
                    (
                        idx
                        for idx, is_checked in location_checkboxes.items()
                        if not is_checked
                    ),
                    None,
                ),
            )
            if next_checkbox_index is None:
                # Done!
                break
            else:
                # Check the next checkbox
                self._check_location_checkbox(
                    form_source_id,
                    location_checkboxes,
                    next_checkbox_index,
                    verbose=verbose,
                )
            request_count += 1

    def _fetch_chart_df(self, verbose=False):
        data = self.post_api(
            {
                self.chart_parent_source_id: self.chart_parent_source_id,
                self.chart_source_id: self.chart_source_id,
                "javax.faces.partial.ajax": "true",
                "javax.faces.partial.execute": "@all",
                "javax.faces.partial.render": "tabPanel",
                "javax.faces.source": self.chart_source_id,
            },
            verbose=verbose,
        )
        chart_source_id = self._get_chart_series_source_id(data, verbose=verbose)
        response = self.post_api(
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
            extension = extensions[0]
            """parse json where values are embedded JSON strings

            {'foo': '{"bar":"baz"}'} -> {'foo': {'bar': 'baz'}}
            """
            data = dict(
                (k, json.loads(v)) for k, v in json.loads(extension.text).items()
            )
            df = self._parse_lmp_series(data["allLmpValues"]["lmpSeries"])
        df["_src"] = "dv"
        return df

    def _get_chart_series_source_id(self, response, verbose):
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

    def _check_location_checkbox(
        self,
        form_source_id,
        location_checkboxes,
        checkbox_index,
        verbose=False,
    ):
        # enable location
        location_checkboxes[checkbox_index] = True

        # generate params reflecting location_checkboxes
        params = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": f"chart1FrmLmpSelection:tblBusAggregates:{checkbox_index}:{form_source_id}",  # noqa: E501
            "javax.faces.partial.execute": f"chart1FrmLmpSelection:tblBusAggregates:{checkbox_index}:{form_source_id}",  # noqa: E501
            "javax.faces.partial.render": "globalMessages",
            "javax.faces.behavior.event": "change",
            "javax.faces.partial.event": "change",
            "chart1FrmLmpSelection": "chart1FrmLmpSelection",
            "chart1FrmLmpSelection:tblBusAggregates:name:filter": "",
            "chart1FrmLmpSelection:tblBusAggregates:voltage:filter": "",
            "chart1FrmLmpSelection:tblBusAggregates:station:filter": "",
            "chart1FrmLmpSelection:tblBusAggregates_scrollState": "0,0",
        }
        for idx, is_checked in location_checkboxes.items():
            if is_checked:
                params[
                    f"chart1FrmLmpSelection:tblBusAggregates"
                    f":{idx}:{form_source_id}_input"
                ] = "on"

        # call the API, ignoring the response unless verbose mode is on
        response = self.post_api(
            params,
            verbose=verbose,
        )
        if verbose:
            print(
                f"response (truncated) = {response.content[0:256]}",
                file=sys.stderr,
            )

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

        return df

    def _parse_lmp_item(self, item):
        # TODO tz_convert to local timezone
        item_id = item["id"]
        if item_id.endswith(" (DA)"):
            market = Markets.DAY_AHEAD_HOURLY
            item_id = item_id.replace(" (DA)", "")
        else:
            market = Markets.REAL_TIME_5_MIN
        item_data = item["data"]
        df = pd.DataFrame(item_data)
        df["Market"] = market.value
        df["Time"] = pd.to_datetime(df["timestamp"], unit="ms").dt.tz_localize(tz="UTC")
        df["Location Name"] = item_id
        return df
