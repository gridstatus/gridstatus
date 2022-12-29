import requests

from gridstatus.httpio.auto_hook_dispatch import AutoHookDispatch


class HttpioRequests(AutoHookDispatch):
    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(HttpioRequests, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        super().__init__()

    def get(self, *args, **kwargs):
        self._before_hook("get", args, kwargs)
        return requests.get(*args, **kwargs)

    def post(self, *args, **kwargs):
        self._before_hook("post", args, kwargs)
        return requests.post(*args, **kwargs)
