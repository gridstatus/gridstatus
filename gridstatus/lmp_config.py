import functools

import pandas as pd

from gridstatus.base import ISOBase, Markets, NotSupported


class lmp_config:
    def __init__(self, supports, tz=None):
        self.supports = supports
        self.tz = tz

    def __call__(self, func):
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
                    tz=self.tz,
                )
                return self._class_method_wrapper(instance, func, func_sig)
            else:
                func_sig = self._parse_signature(None, args, kwargs, tz=self.tz)
                return self._function_wrapper(func, func_sig)

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
        if date == "latest":
            return lmp_config.__handle_date("today", tz=tz)
        elif isinstance(date, str) or isinstance(date, pd.Timestamp):
            final_date = lmp_config.__handle_date(date, tz=tz)
            if not isinstance(final_date, pd.Timestamp):
                raise ValueError(f"Cannot parse date {repr(date)}")
            return final_date
        else:
            raise ValueError("date must be string or pd.Timestamp")

    @staticmethod
    def _class_method_wrapper(instance, func, func_sig):
        instance_args = tuple([instance] + list(func_sig["args"]))
        instance_kwargs = func_sig["kwargs"]
        return func(*instance_args, **instance_kwargs)

    @staticmethod
    def _function_wrapper(func, func_sig):
        return func(*func_sig["args"], **func_sig["kwargs"])

    @staticmethod
    def _parse_market(market):
        return Markets(market)

    def _parse_signature(self, instance, args, kwargs, tz):
        date = None
        market = None

        if tz is None:
            if instance is not None:
                if not hasattr(instance, "default_timezone"):
                    raise ValueError(
                        "ISO does not have default_timezone set; cannot determine tz",
                    )
                else:
                    tz = instance.default_timezone
        if tz is None:
            raise ValueError("Must set tz= arg or ISO default timezone")

        args = list(args)

        if len(args) == 0:
            if "date" not in kwargs or "market" not in kwargs:
                raise ValueError("date and market are required")
            date = kwargs["date"]
            market = kwargs["market"]
        elif len(args) == 1:
            if "date" in kwargs and "market" in kwargs:
                date = kwargs["date"]
                market = kwargs["market"]
            elif "date" in kwargs:
                date = kwargs["date"]
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
                kwargs["date"] = date
            kwargs["market"] = market
        elif len(args) == 1:
            if "date" in kwargs and "market" not in kwargs:
                if modify_date_arg:
                    kwargs["date"] = date
                args[0] = market
            elif "date" not in kwargs and "market" in kwargs:
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

        if original_date_arg in (
            "latest",
            "today",
        ):
            supported = original_date_arg in self.supports[market_arg]
        elif lmp_config.__is_today(date_arg, tz):
            supported = "today" in self.supports[market_arg]
        else:
            supported = "historical" in self.supports[market_arg]

        if not supported:
            raise NotSupported(
                f"{market_arg} does not support {repr(original_date_arg)}",
            )

    # copied from utils.py to avoid circular import
    @staticmethod
    def __handle_date(date, tz=None):
        if date == "today":
            date = pd.Timestamp.now(tz=tz)

        if not isinstance(date, pd.Timestamp):
            date = pd.to_datetime(date)

        if tz and date.tzinfo is None:
            date = date.tz_localize(tz)

        return date

    # copied from utils.py to avoid circular import
    @staticmethod
    def __is_today(date, tz):
        return (
            lmp_config.__handle_date(date, tz=tz).date()
            == pd.Timestamp.now(tz=tz).date()
        )
