import pandas as pd

from gridstatus.httpio.auto_hook_dispatch import AutoHookDispatch


class HttpioPandas(AutoHookDispatch):
    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(HttpioPandas, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        super().__init__()

    def read_csv(self, *args, **kwargs):
        value = self._before_hook("read_csv", args, kwargs)
        return value or pd.read_csv(*args, **kwargs)

    def read_excel(self, *args, **kwargs):
        value = self._before_hook("read_excel", args, kwargs)
        return value or pd.read_excel(*args, **kwargs)

    def read_html(self, *args, **kwargs):
        value = self._before_hook("read_html", args, kwargs)
        return value or pd.read_html(*args, **kwargs)
