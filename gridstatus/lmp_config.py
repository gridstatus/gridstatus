import datetime
import functools

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
            if self._is_class_method(args, kwargs):
                instance = args[0]
                func_args = args[1:]
                func_kwargs = kwargs
                func_sig = self._parse_signature(
                    instance,
                    func_args,
                    func_kwargs,
                )
                return self._class_method_wrapper(instance, func, func_sig)
            else:
                raise ValueError("Must be class method on ISOBase")

        return wrapper

    @staticmethod
    def _is_class_method(args, kwargs):
        # args[0] must be "self" and an instance of ISOBase
        return len(args) > 0 and isinstance(args[0], ISOBase)

    @staticmethod
    def _validate_market_arg_signature(market):
        return isinstance(market, str) or isinstance(market, Markets)

    @staticmethod
    def _parse_date(date, tz):
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
    def _class_method_wrapper(instance, func, func_sig):
        instance_args = tuple([instance] + list(func_sig["args"]))
        instance_kwargs = func_sig["kwargs"]
        return func(*instance_args, **instance_kwargs)

    @staticmethod
    def _parse_market(market):
        return Markets(market)

    def _parse_signature(self, instance, args, kwargs):
        date = None
        date_field = None
        market = None

        tz = instance.default_timezone
        args = list(args)

        if len(args) == 0:
            if (
                "date" not in kwargs and "start" not in kwargs
            ) or "market" not in kwargs:
                raise ValueError("date/start and market are required")
            if "date" in kwargs:
                date_field = "date"
            else:
                date_field = "start"
            date = kwargs[date_field]
            market = kwargs["market"]
        elif len(args) == 1:
            if ("date" in kwargs or "start" in kwargs) and "market" in kwargs:
                if "date" in kwargs:
                    date_field = "date"
                else:
                    date_field = "start"
                date = kwargs[date_field]
                market = kwargs["market"]
            elif "date" in kwargs:
                date_field = "start"
                date = kwargs[date_field]
                market = args[0]
            elif "market" in kwargs:
                date = args[0]
                market = kwargs["market"]
            else:
                raise ValueError("Missing date or market")
        else:
            date = args[0]
            market = args[1]

        original_date_arg = date
        date = self._parse_date(date, tz=tz)
        market = self._parse_market(market)

        self._check_support(original_date_arg, date, market, tz)

        modify_date_arg = original_date_arg != "latest"

        if len(args) == 0:
            if modify_date_arg:
                kwargs[date_field] = date
            kwargs["market"] = market
        elif len(args) == 1:
            if date_field in kwargs and "market" not in kwargs:
                if modify_date_arg:
                    kwargs[date_field] = date
                args[0] = market
            elif date_field not in kwargs and "market" in kwargs:
                if modify_date_arg:
                    args[0] = date
                kwargs["market"] = market
        else:
            if modify_date_arg:
                args[0] = date
            args[1] = market

        return {
            "args": tuple(args),
            "kwargs": kwargs,
        }

    def _check_support(self, original_date_arg, date_arg, market_arg, tz):
        if market_arg not in self.supports:
            raise NotSupported(f"{market_arg} not supported")

        from gridstatus.utils import is_today

        if original_date_arg in (
            "latest",
            "today",
        ):
            supported = original_date_arg in self.supports[market_arg]
        elif is_today(date_arg, tz):
            supported = "today" in self.supports[market_arg]
        else:
            supported = "historical" in self.supports[market_arg]

        if not supported:
            raise NotSupported(
                f"{market_arg} does not support {repr(original_date_arg)}",
            )

    @classmethod
    def supports(cls, method, market, date):
        qualname = method.__qualname__
        market = Markets(market)
        return (
            qualname in cls.configs
            and market in cls.configs[qualname]
            and date in cls.configs[qualname][market]
        )
