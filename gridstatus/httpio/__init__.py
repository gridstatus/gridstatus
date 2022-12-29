from gridstatus.httpio.httpio import Httpio


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
