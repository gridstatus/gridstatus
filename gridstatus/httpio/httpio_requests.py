import os

import requests

from gridstatus.httpio.adapters.logger import LoggerAdapter
from gridstatus.httpio.hook_dispatch import HookDispatch


class HttpioRequests(HookDispatch):
    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(HttpioRequests, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        super().__init__()
        self.register_hook(LoggerAdapter("HTTPIO_VERBOSE" in os.environ))

    def get(self, *args, **kwargs):
        self._before_hook("get", args, kwargs)
        return requests.get(*args, **kwargs)

    def post(self, *args, **kwargs):
        self._before_hook("post", args, kwargs)
        return requests.post(*args, **kwargs)
