import os
import sys

import pandas as pd
import requests


class Httpio(object):
    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(Httpio, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.verbose = "HTTPIO_VERBOSE" in os.environ

    def pd_read_csv(self, *args, **kwargs):
        if self.verbose:
            print(f"read_csv: args={args}, kwargs={kwargs}", file=sys.stderr)
        return pd.read_csv(*args, **kwargs)

    def pd_read_excel(self, *args, **kwargs):
        if self.verbose:
            print(f"read_excel: args={args}, kwargs={kwargs}", file=sys.stderr)
        return pd.read_excel(*args, **kwargs)

    def pd_read_html(self, *args, **kwargs):
        if self.verbose:
            print(f"read_html: args={args}, kwargs={kwargs}", file=sys.stderr)
        return pd.read_html(*args, **kwargs)

    def requests_get(self, *args, **kwargs):
        if self.verbose:
            print(f"requests_get: args={args}, kwargs={kwargs}", file=sys.stderr)
        return requests.get(*args, **kwargs)

    def requests_post(self, *args, **kwargs):
        if self.verbose:
            print(f"requests_post: args={args}, kwargs={kwargs}", file=sys.stderr)
        return requests.post(*args, **kwargs)


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


def read_csv(*args, **kwargs):
    return Httpio().pd_read_csv(*args, **kwargs)


def read_excel(*args, **kwargs):
    return Httpio().pd_read_excel(*args, **kwargs)


def read_html(*args, **kwargs):
    return Httpio().pd_read_html(*args, **kwargs)


def get(*args, **kwargs):
    return Httpio().requests_get(*args, **kwargs)


def post(*args, **kwargs):
    return Httpio().requests_post(*args, **kwargs)
