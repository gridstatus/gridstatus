import requests

from gridstatus.httpio import Httpio


class Session:
    def __init__(self):
        self.session = requests.Session()
        self.verbose = Httpio().verbose

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.session.close()

    def get(self, *args, **kwargs):
        if self.verbose:
            print(f"session.get: args={args}, kwargs={kwargs}", file=sys.stderr)
        return self.session.get(*args, **kwargs)

    def post(self, *args, **kwargs):
        if self.verbose:
            print(f"session.post: args={args}, kwargs={kwargs}", file=sys.stderr)
        return self.session.post(*args, **kwargs)

    def __getattr__(self, item):
        if self.verbose:
            print(f"session.{item}", file=sys.stderr)
        return getattr(self.session, item)
