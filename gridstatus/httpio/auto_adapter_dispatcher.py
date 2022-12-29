import os

from gridstatus.httpio.adapter_dispatcher import AdapterDispatcher
from gridstatus.httpio.adapters.logger import LoggerAdapter


class AutoAdapterDispatcher(AdapterDispatcher):
    def __init__(self):
        super().__init__()
        if "HTTPIO_LOGGING" in os.environ:
            self.register_adapter(LoggerAdapter())
