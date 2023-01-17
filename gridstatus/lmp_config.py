import datetime
import functools
from collections import OrderedDict

import pandas as pd

from gridstatus.base import ISOBase, Markets, NotSupported


class lmp_config:

    configs = {}

    def __init__(self, supports, tz=None):
        self.supports = supports
        self.tz = tz

    def __call__(self, func):

        lmp_config.configs[func.__qualname__] = self.supports

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            fn_params = self._get_fn_params(func, args, kwargs)
            if len(args) > 0 and isinstance(args[0], ISOBase):
                fn_params = self._verify_fn_params(fn_params)
                return self._class_method_wrapper(func, fn_params)
            else:
                raise ValueError("Must be class method on ISOBase")

        return wrapper

    @staticmethod
    def _parse_date(date, tz):
        """Parse date to pd.Timestamp,
        to be used later on for validation"""
        from gridstatus.utils import _handle_date

        if date == "latest":
            return _handle_date("today", tz=tz)
        elif (
            isinstance(date, str)
            or isinstance(date, pd.Timestamp)
            or isinstance(date, datetime.date)
        ):
            final_date = _handle_date(date, tz=tz)
            if not isinstance(final_date, pd.Timestamp):
                raise ValueError(f"Cannot parse date {repr(date)}")
            return final_date
        else:
            raise ValueError(
                "date must be string or pd.Timestamp: "
                f"{repr(date)} of type ({type(date)})",
            )

    @staticmethod
    def _class_method_wrapper(func, fn_params):
        instance_args = tuple(fn_params["args"])
        instance_kwargs = fn_params["kwargs"]
        return func(*instance_args, **instance_kwargs)

    def _verify_fn_params(self, fn_params):
        """Verify date/start and market args/kwargs. Transform values and injects
        them back into the original signature if needed.

        Raises:
            ValueError: If date/start or market are missing or invalid
        """
        args = fn_params["args"]
        kwargs = fn_params["kwargs"]
        instance = args["self"]

        date = self._get_first([args, kwargs], ["date", "start"])
        market = self._get_first([args, kwargs], ["market"])
        tz = instance.default_timezone

        if date is None:
            raise ValueError("date/start is required")
        elif market is None:
            raise ValueError("market is required")

        date_value = self._parse_date(date, tz=tz)
        market_value = Markets(market)

        self._check_support(date, date_value, market_value, tz)

        if date != date_value:
            self._set_first([args, kwargs], ["date", "start"], date_value)
        if market != market_value:
            self._set_first([args, kwargs], ["market"], market_value)

        return fn_params

    def _check_support(self, orig_date, date, market, tz):
        if market not in self.supports:
            raise NotSupported(f"{market} not supported")

        from gridstatus.utils import is_today

        if orig_date in (
            "latest",
            "today",
        ):
            supported = orig_date in self.supports[market]
        elif is_today(date, tz):
            supported = "today" in self.supports[market]
        else:
            supported = "historical" in self.supports[market]

        if not supported:
            raise NotSupported(
                f"{market} does not support {repr(orig_date)}",
            )

    @classmethod
    def supports(cls, method, market, date):
        """Check if a method supports a market and date.

        Example:
            lmp_config.supports(iso.get_lmp, Markets.REAL_TIME_5_MIN, "latest")
        """
        qualname = method.__qualname__
        market = Markets(market)
        return (
            qualname in cls.configs
            and market in cls.configs[qualname]
            and date in cls.configs[qualname][market]
        )

    @staticmethod
    def _get_first(dicts, params):
        """Find first param in list of dictionaries"""
        for param in params:
            for d in dicts:
                if param in d:
                    return d[param]
        return None

    @staticmethod
    def _set_first(dicts, params, value):
        """Set first param in list of dictionaries"""
        for param in params:
            for d in dicts:
                if param in d:
                    d[param] = value
                    break

    @staticmethod
    def _get_fn_params(fn, args, kwargs):
        """Returns args as ordered dictionary and kwargs"""
        args_names = fn.__code__.co_varnames[: fn.__code__.co_argcount]
        args_odict = {**OrderedDict(zip(args_names, args))}
        return {"args": args_odict, "kwargs": kwargs}
