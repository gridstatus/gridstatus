import os

from gridstatus.httpio.adapter_dispatcher import AdapterDispatcher
from gridstatus.httpio.adapters.base import BaseAdapter
from gridstatus.httpio.adapters.logger import LoggerAdapter
from gridstatus.httpio.adapters.pickle_cache import PickleCacheAdapter
from gridstatus.httpio.httpio_pandas import HttpioPandas
from gridstatus.httpio.httpio_requests import HttpioRequests

""""For easy access to underlying Adapter Dispatchers"""

_httpio_pandas = HttpioPandas()
_httpio_requests = HttpioRequests()

adapter_dispatchers = [
    _httpio_requests,
    _httpio_pandas,
]

Session = HttpioRequests.Session


def read_csv(*args, **kwargs):
    return _httpio_pandas.read_csv(*args, **kwargs)


def read_excel(*args, **kwargs):
    return _httpio_pandas.read_excel(*args, **kwargs)


def read_html(*args, **kwargs):
    return _httpio_pandas.read_html(*args, **kwargs)


def get(*args, **kwargs):
    return _httpio_requests.get(*args, **kwargs)


def post(*args, **kwargs):
    return _httpio_requests.post(*args, **kwargs)


def register_adapter(adapter: BaseAdapter):
    for dispatcher in adapter_dispatchers:
        dispatcher.register_adapter(adapter)


if "HTTPIO_LOGGING" in os.environ:
    register_adapter(LoggerAdapter())

if "HTTPIO_PICKLE_CACHE" in os.environ:
    register_adapter(PickleCacheAdapter())
