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

    class Session(AutoHookDispatch):
        def __init__(self):
            super().__init__()
            self.session = requests.Session()

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
