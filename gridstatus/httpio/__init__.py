from gridstatus.httpio.httpio_pandas import HttpioPandas
from gridstatus.httpio.httpio_requests import HttpioRequests

""""For easy access to underlying Adapter Dispatchers"""


def read_csv(*args, **kwargs):
    return HttpioPandas().read_csv(*args, **kwargs)


def read_excel(*args, **kwargs):
    return HttpioPandas().read_excel(*args, **kwargs)


def read_html(*args, **kwargs):
    return HttpioPandas().read_html(*args, **kwargs)


def get(*args, **kwargs):
    return HttpioRequests().get(*args, **kwargs)


def post(*args, **kwargs):
    return HttpioRequests().post(*args, **kwargs)


Session = HttpioRequests.Session
