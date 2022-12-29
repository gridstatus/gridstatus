import sys
import traceback

from gridstatus.httpio.adapters.base import BaseAdapter

INTERNAL_FILES = (
    "httpio/__init__.py",
    "httpio/httpio_pandas.py",
    "httpio/httpio_requests.py",
    "httpio/logger.py",
)


class LoggerAdapter(BaseAdapter):
    @staticmethod
    def _last_external_filename_lineno():
        """Return the first frame outside of this file in the traceback."""
        for frame in reversed(traceback.extract_stack()):
            if not any(frame.filename.endswith(f) for f in INTERNAL_FILES):
                return f"{frame.filename}:{frame.lineno}"

    def before_hook(self, method, args, kwargs):
        file_line = self._last_external_filename_lineno()
        method_args = []
        method_args += [repr(arg) for arg in args]
        method_args += [f"{k}={repr(v)}" for k, v in kwargs.items()]
        method_args = ", ".join(method_args)
        print(
            f"{file_line} httpio.{method}({method_args})",
            file=sys.stderr,
        )
