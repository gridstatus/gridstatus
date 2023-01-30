import sys

import bs4
import requests


class Session:
    """Holds a requests.Session, keeping view_state, nonce, and initial response.

    Subclasses should set a URL attribute."""

    def __init__(self):
        self.requests_session = requests.Session()
        self.initial_response = None
        self.nonce = None
        self.view_state = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.requests_session.close()

    def start(self, verbose=False):
        """Initial fetch: creating a new requests.Session,
        extracting the session and view state
        """
        if verbose:
            print(f"GET {self.URL}")
        self.initial_response = self.requests_session.get(self.URL)

        html = self.initial_response.content
        doc = bs4.BeautifulSoup(html, "html.parser")

        scripts = doc.find_all("script", {"nonce": True})
        # find first nonce attribute
        self.nonce = next(
            iter(script["nonce"] for script in scripts if script["nonce"]),
            None,
        )

        view_states = doc.find_all(
            "input",
            {"type": "hidden", "name": "javax.faces.ViewState"},
        )
        # find first value of <input type="hidden" name="javax.faces.ViewState">
        self.view_state = next(
            iter(
                view_state["value"] for view_state in view_states if view_state["value"]
            ),
            None,
        )

        if self.nonce is None or self.view_state is None:
            raise ValueError("Could not find nonce or ViewState")

    def post_api(self, args, verbose=False):
        """POSTs to URL endpoint, adding ViewState and nonce"""
        args.update(
            {
                "javax.faces.ViewState": self.view_state,
                "primefaces.nonce": self.nonce,
            },
        )
        if verbose:
            print(f"POST with {args}", file=sys.stderr)
        return self.requests_session.post(self.URL, data=args)

    @staticmethod
    def _parse_xml_find_all(xml_string: str, *args, **kwargs):
        """From a string, extract <update> CDATA content
        and return a list of BeautifulSoup-parsed HTML."""
        xml_doc = bs4.BeautifulSoup(xml_string, "xml")
        elems = xml_doc.find_all(*args, **kwargs)
        return [bs4.BeautifulSoup(elem.text, "html.parser") for elem in elems]
