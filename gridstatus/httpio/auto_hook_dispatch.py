import os

from gridstatus.httpio.adapters.logger import LoggerAdapter
from gridstatus.httpio.hook_dispatch import HookDispatch


class AutoHookDispatch(HookDispatch):
    def __init__(self):
        super().__init__()
        if "HTTPIO_LOGGING" in os.environ:
            self.register_hook(LoggerAdapter())
