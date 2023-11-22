import datetime
import functools
import inspect

import pandas as pd

from gridstatus.base import ISOBase, Markets, NotSupported


class lmp_config:
    configs = {}

    def __init__(self, supports):
        self.supports = supports

    def __call__(self, func):
        lmp_config.configs[func.__qualname__] = self.supports

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            bound_args = self._get_bound_args(func, args, kwargs)
            if len(args) > 0 and isinstance(args[0], ISOBase):
                bound_args = self._verify_bound_args(bound_args)
                return self._class_method_wrapper(func, bound_args)
            else:
                # This is a runtime check after method is called.
                # Possible improvement: move this to "compile"-time.
                raise ValueError("Must be class method on ISOBase")

        return wrapper

    @staticmethod
    def _parse_date(date, tz):
        """Parse date to pd.Timestamp,
        to be used later on for validation"""
        from gridstatus.utils import _handle_date

        # if date range tuple, just validate start
        if isinstance(date, tuple):
            date = date[0]

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

        if "date" in arguments:
            date = arguments["date"]
        elif "start" in arguments:
            date = arguments["start"]
        else:
            raise ValueError("date/start is required")

        if "market" in arguments:
            market = arguments["market"]
        else:
            raise ValueError("market is required")

        tz = instance.default_timezone
        date_value = self._parse_date(date, tz=tz)
        market_value = Markets(market)

        self._check_support(date, date_value, market_value, tz)

        arguments["market"] = market_value

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
    def supports(cls, method, market, date=None):
        """Check if a method supports a market
        and optionally, a date ("latest", "today", "historical")

        Example:
            lmp_config.supports(iso.get_lmp, Markets.REAL_TIME_5_MIN, "latest")
        """
        qualname = method.__qualname__
        market = Markets(market)

        is_supported = qualname in cls.configs and market in cls.configs[qualname]
        if date is not None:
            is_supported = is_supported and date in cls.configs[qualname][market]
        return is_supported

    @classmethod
    def get_support(cls, method):
        """Fetches support config dictionary"""
        return cls.configs.get(method.__qualname__, {}).copy()

    @staticmethod
    def _get_bound_args(fn, args, kwargs) -> inspect.BoundArguments:
        """Returns args as ordered dictionary and kwargs"""
        sig = inspect.signature(fn)
        if "start" in kwargs and "date" not in kwargs and len(args) < 2:
            # For @support_date_range which allows start/end kwargs
            kwargs["date"] = kwargs.pop("start")
        bound_args = sig.bind(*args, **kwargs)

        if "self" not in bound_args.arguments:
            raise ValueError("Must be class method on ISOBase")

        return bound_args
