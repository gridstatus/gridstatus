import os
import sys
import traceback

import pandas as pd

from gridstatus.httpio.adapters.logger import LoggerAdapter
from gridstatus.httpio.hook_dispatch import HookDispatch


class HttpioPandas(HookDispatch):
    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(HttpioPandas, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        super().__init__()
        self.register_hook(LoggerAdapter("HTTPIO_VERBOSE" in os.environ))

    def read_csv(self, *args, **kwargs):
        self._before_hook("read_csv", args, kwargs)
        return pd.read_csv(*args, **kwargs)

    def read_excel(self, *args, **kwargs):
        self._before_hook("read_excel", args, kwargs)
        return pd.read_excel(*args, **kwargs)

    def read_html(self, *args, **kwargs):
        self._before_hook("read_html", args, kwargs)
        return pd.read_html(*args, **kwargs)
