import os
import sys
import traceback

import pandas as pd
import requests

INTERNAL_FILES = (
    "httpio/__init__.py",
    "httpio/httpio_pandas.py",
    "httpio/httpio_requests.py",
    "httpio/httpio_requests_session.py",
)


class HttpioRequests(object):
    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(HttpioRequests, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.verbose = "HTTPIO_VERBOSE" in os.environ

    def get(self, *args, **kwargs):
        self._log_verbose("get", args, kwargs)
        return requests.get(*args, **kwargs)

    def post(self, *args, **kwargs):
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
