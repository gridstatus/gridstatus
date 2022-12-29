import os

from gridstatus.httpio.adapters.logger import LoggerAdapter
from gridstatus.httpio.hook_dispatch import HookDispatch


class AutoHookDispatch(HookDispatch):
    def __init__(self):
        super().__init__()
        self.register_hook(LoggerAdapter("HTTPIO_VERBOSE" in os.environ))
