import requests

from gridstatus.httpio.auto_hook_dispatch import AutoHookDispatch


class HttpioRequestsSession(AutoHookDispatch):
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
