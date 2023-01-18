import datetime
import functools
import inspect

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
            bound_args = self._get_bound_args(func, args, kwargs)
            if len(args) > 0 and isinstance(args[0], ISOBase):
                bound_args = self._verify_bound_args(bound_args)
                return self._class_method_wrapper(func, bound_args)
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
    def _class_method_wrapper(func, bound_args):
        instance_args = bound_args.args
        instance_kwargs = bound_args.kwargs
        return func(*instance_args, **instance_kwargs)

    def _verify_bound_args(self, bound_args: inspect.BoundArguments):
        """Verify date/start and market args/kwargs. Transform values and injects
        them back into the original signature if needed.

        Raises:
            ValueError: If date/start or market are missing or invalid
        """
        arguments = bound_args.arguments
        instance = arguments["self"]

        date = self._get_first(arguments, ["date", "start"])
        market = self._get_first(arguments, ["market"])
        tz = instance.default_timezone

        if date is None:
            raise ValueError("date/start is required")
        elif market is None:
            raise ValueError("market is required")

        date_value = self._parse_date(date, tz=tz)
        market_value = Markets(market)

        self._check_support(date, date_value, market_value, tz)

        if date != "latest" and date_value != date:
            self._set_first(arguments, ["date", "start"], date_value)
        if market != market_value:
            self._set_first(arguments, ["market"], market_value)

        return bound_args

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
    def _get_first(arguments, params):
        """Find first param in list of dictionaries"""
        for param in params:
            if param in arguments:
                return arguments[param]
        return None

    @staticmethod
    def _set_first(arguments, params, value):
        """Set first param in list of dictionaries"""
        for param in params:
            if param in arguments:
                arguments[param] = value
                break

    @staticmethod
    def _get_bound_args(fn, args, kwargs) -> inspect.BoundArguments:
        """Returns args as ordered dictionary and kwargs"""
        sig = inspect.signature(fn)
        bound_args = sig.bind(*args, **kwargs)

        if "self" not in bound_args.arguments:
            raise ValueError("Must be class method on ISOBase")

        return bound_args
