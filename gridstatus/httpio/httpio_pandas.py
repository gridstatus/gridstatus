import pandas as pd

from gridstatus.httpio import AdapterDispatcher


class HttpioPandas(AdapterDispatcher):
    """These are drop-in replacements for pandas.read_*"""

    def __init__(self):
        super().__init__()

    def read_csv(self, *args, **kwargs):
        return self._exec_method("read_csv", pd.read_csv, *args, **kwargs)

    def read_excel(self, *args, **kwargs):
        return self._exec_method("read_excel", pd.read_excel, *args, **kwargs)

    def read_html(self, *args, **kwargs):
        return self._exec_method("read_html", pd.read_html, *args, **kwargs)
