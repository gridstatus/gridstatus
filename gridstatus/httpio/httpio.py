import os
import sys
import traceback

import pandas as pd
import requests

INTERNAL_FILES = (
    "httpio/__init__.py",
    "httpio/httpio.py",
)


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
            if not any(frame.filename.endswith(f) for f in INTERNAL_FILES):
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
