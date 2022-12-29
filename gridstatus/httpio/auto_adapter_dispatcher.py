import os

from gridstatus.httpio.adapter_dispatcher import AdapterDispatcher
from gridstatus.httpio.adapters.logger import LoggerAdapter
from gridstatus.httpio.adapters.pickle_cache import PickleCacheAdapter


class AutoAdapterDispatcher(AdapterDispatcher):
    def __init__(self):
        super().__init__()
        if "HTTPIO_LOGGING" in os.environ:
            self.register_adapter(LoggerAdapter())
        if "HTTPIO_PICKLE_CACHE" in os.environ:
            self.register_adapter(PickleCacheAdapter())
