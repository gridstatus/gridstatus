import os
import sys
import traceback

import pandas as pd
import requests


class Httpio(object):
    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(Httpio, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.verbose = "HTTPIO_VERBOSE" in os.environ

    def pd_read_csv(self, *args, **kwargs):
        self._log_verbose("read_csv", args, kwargs)
        return pd.read_csv(*args, **kwargs)

    def pd_read_excel(self, *args, **kwargs):
        self._log_verbose("read_excel", args, kwargs)
        return pd.read_excel(*args, **kwargs)

    def pd_read_html(self, *args, **kwargs):
        self._log_verbose("read_html", args, kwargs)
        return pd.read_html(*args, **kwargs)

    def requests_get(self, *args, **kwargs):
        self._log_verbose("get", args, kwargs)
        return requests.get(*args, **kwargs)

    def requests_post(self, *args, **kwargs):
        self._log_verbose("post", args, kwargs)
        return requests.post(*args, **kwargs)

    @staticmethod
    def _last_external_filename_lineno():
        """Return the first frame outside of this file in the traceback."""
        for frame in reversed(traceback.extract_stack()):
            if frame.filename != __file__:
                return f"{frame.filename}:{frame.lineno}"

    def _log_verbose(self, method, args, kwargs):
        if self.verbose:
            file_line = self._last_external_filename_lineno()
            method_args = []
            method_args += [repr(arg) for arg in args]
            method_args += [f"{k}={repr(v)}" for k, v in kwargs.items()]
            method_args = ", ".join(method_args)
            print(
                f"{file_line} httpio.{method}({method_args})",
                file=sys.stderr,
            )


class Session:
    def __init__(self):
        self.session = requests.Session()
        self.verbose = Httpio().verbose

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.session.close()

    def get(self, *args, **kwargs):
        if self.verbose:
            print(f"session.get: args={args}, kwargs={kwargs}", file=sys.stderr)
        return self.session.get(*args, **kwargs)

    def post(self, *args, **kwargs):
        if self.verbose:
            print(f"session.post: args={args}, kwargs={kwargs}", file=sys.stderr)
        return self.session.post(*args, **kwargs)

    def __getattr__(self, item):
        if self.verbose:
            print(f"session.{item}", file=sys.stderr)
        return getattr(self.session, item)


def read_csv(*args, **kwargs):
    return Httpio().pd_read_csv(*args, **kwargs)


def read_excel(*args, **kwargs):
    return Httpio().pd_read_excel(*args, **kwargs)


def read_html(*args, **kwargs):
    return Httpio().pd_read_html(*args, **kwargs)


def get(*args, **kwargs):
    return Httpio().requests_get(*args, **kwargs)


def post(*args, **kwargs):
    return Httpio().requests_post(*args, **kwargs)
