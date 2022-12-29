import hashlib
import os
import pickle
import sys

from gridstatus.httpio.adapters.base import BaseAdapter


class PickleCacheAdapter(BaseAdapter):
    """This cache adapter will pickle values in tmp_dir, using method,
    args and kwargs to generate a SHA256 hash for the filename. There
    is no time-to-live support currently.
    """

    def __init__(self, tmp_dir="/tmp"):
        super().__init__("pickle_cache")
        self.tmp_dir = tmp_dir

    ALLOWED_METHODS = (
        "get",
        "post",
        "read_csv",
        "read_excel",
        "read_html",
        "session.get",
        "session.post",
    )

    def before_filter(self, method, args, kwargs):
        if method in self.ALLOWED_METHODS:
            path = self._get_path(method, args, kwargs)
            if os.path.exists(path):
                return pickle.load(open(path, "rb"))
        else:
            print(
                f"WARN: PickleCacheAdapter before_hook: method {method} not allowed",
                file=sys.stderr,
            )

    def after_hook(self, method, args, kwargs, value, is_new_value=False):
        if method in self.ALLOWED_METHODS:
            if is_new_value:
                path = self._get_path(method, args, kwargs)
                with open(path, "wb") as f:
                    pickle.dump(value, f)
        else:
            print(
                f"WARN: PickleCacheAdapter after_hook: method {method} not allowed",
                file=sys.stderr,
            )

    def _get_path(self, method, args, kwargs):
        hash = PickleCacheAdapter._get_hash(method, args, kwargs)
        return f"{self.tmp_dir}/{hash}.dat"

    @staticmethod
    def _get_hash(method, args, kwargs):
        contents = repr(
            sorted(
                {
                    "method": method,
                    "args": args,
                    "kwargs": sorted(kwargs),
                }.items(),
            ),
        )
        return hashlib.sha256(contents.encode("utf-8")).hexdigest()
