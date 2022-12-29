import requests

from gridstatus.httpio import AdapterDispatcher


class HttpioRequests(AdapterDispatcher):
    """These are drop-in replacements for requests.{get,post} and requests.Session"""

    def __init__(self):
        super().__init__()

    def get(self, *args, **kwargs):
        return self._exec_method("get", requests.get, *args, **kwargs)

    def post(self, *args, **kwargs):
        return self._exec_method("post", requests.post, *args, **kwargs)

    class Session(AdapterDispatcher):
        def __init__(self):
            super().__init__()
            self.session = requests.Session()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.session.close()

        def get(self, *args, **kwargs):
            return self._exec_method("session.get", self.session.get, *args, **kwargs)

        def post(self, *args, **kwargs):
            return self._exec_method("session.post", self.session.post, *args, **kwargs)

        def __getattr__(self, item):
            return getattr(self.session, item)
