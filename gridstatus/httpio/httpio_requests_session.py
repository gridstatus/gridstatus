import os

import requests

from gridstatus.httpio.adapters.logger import LoggerAdapter
from gridstatus.httpio.hook_dispatch import HookDispatch


class HttpioRequestsSession(HookDispatch):
    def __init__(self):
        super().__init__()
        self.session = requests.Session()
        self.register_hook(LoggerAdapter("HTTPIO_VERBOSE" in os.environ))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.session.close()

    def get(self, *args, **kwargs):
        self._before_hook("session.get", args, kwargs)
        return self.session.get(*args, **kwargs)

    def post(self, *args, **kwargs):
        self._before_hook("session.post", args, kwargs)
        return self.session.post(*args, **kwargs)

    def __getattr__(self, item):
        return getattr(self.session, item)
