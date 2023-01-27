import json

import bs4
import requests


class Session:
    def __init__(self, verbose=False):
        self.requests_session = requests.Session()

        self.data = {}
        self.initial_fetch = None
        self.nonce = None
        self.view_state = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.requests_session.close()

    def update(self, data):
        self.data.update(data)

    def __getitem__(self, item):
        return self.data[item]

    def __setitem__(self, key, value):
        self.data[key] = value

    def start(self, verbose=False):
        """Initial fetch: creating a new requests.Session,
        extracting the session and view state
        """
        if verbose:
            print(f"GET {self.URL}")
        self.initial_fetch = self.requests_session.get(self.URL)

        html = self.initial_fetch.content
        doc = bs4.BeautifulSoup(html, "html.parser")

        scripts = doc.find_all("script", {"nonce": True})

        for script in scripts:
            self.nonce = script["nonce"]
            if self.nonce is not None:
                break

        view_states = doc.find_all(
            "input",
            {"type": "hidden", "name": "javax.faces.ViewState"},
        )
        for view_state in view_states:
            self.view_state = view_state["value"]
            if self.view_state is not None:
                break

        if self.nonce is None or self.view_state is None:
            raise ValueError("Could not start DV Session")

    def post(self, **kwargs):
        return self.requests_session.post(self.URL, **kwargs)

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

    @staticmethod
    def _json_loads_nested_jsonstrings(text):
        """Load JSON where values are JSON-strings"""
        doc = dict((k, json.loads(v)) for k, v in json.loads(text).items())
        return doc
