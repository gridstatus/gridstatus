import sys
import traceback

import requests

from gridstatus.httpio import HttpioPandas

INTERNAL_FILES = (
    "httpio/__init__.py",
    "httpio/httpio_pandas.py",
    "httpio/httpio_requests.py",
    "httpio/httpio_requests_session.py",
)


class HttpioRequestsSession:
    def __init__(self):
        self.session = requests.Session()
        self.verbose = HttpioPandas().verbose

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.session.close()

    def get(self, *args, **kwargs):
        self._log_verbose("session.get", args, kwargs)
        return self.session.get(*args, **kwargs)

    def post(self, *args, **kwargs):
        self._log_verbose("session.post", args, kwargs)
        return self.session.post(*args, **kwargs)

    def __getattr__(self, item):
        return getattr(self.session, item)

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
