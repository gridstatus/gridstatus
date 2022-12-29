from collections import OrderedDict

from gridstatus.httpio.adapters.base import BaseAdapter

"""AdapterDispatchers holds a list of adapters which may implement the following methods:

- before_hook: Runs before any method call
- before_filter: Runs before any method call, and may return a value to short-circuit the method call
- after_hook: Runs after any method call, with the return value, and whether the value is new or not
"""


class AdapterDispatcher:
    def __init__(self):
        self.adapters = OrderedDict()

    def register_adapter(self, adapter: BaseAdapter):
        if adapter.name not in self.adapters:
            self.adapters[adapter.name] = adapter

    def unregister_adapter(self, adapter: BaseAdapter):
        if adapter.name in self.adapters:
            del self.adapters[adapter.name]

    def get_adapter(self, key):
        return self.adapters[key]

    def _exec_method(self, method: str, fn, *args, **kwargs):
        """Run before hooks, before filter, call the method if needed, and run after hooks"""
        self._run_before_hooks(method, args, kwargs)
        new_value = False
        value = self._run_before_filters(method, args, kwargs)
        if value is None:
            value = fn(*args, **kwargs)
            new_value = True
        self._run_after_hooks(method, args, kwargs, value, new_value)
        return value

    def _run_before_hooks(self, method, args, kwargs):
        """Run before_hook for all adapters"""
        for adapter in self.adapters.values():
            adapter.before_hook(method, args, kwargs)

    def _run_before_filters(self, method, args, kwargs):
        """Return the first non-None value from a before_filter"""
        value = None
        for adapter in self.adapters.values():
            value = adapter.before_filter(method, args, kwargs)
            if value is not None:
                break
        return value

    def _run_after_hooks(self, method, args, kwargs, value, is_new_value):
        """Run before_hook for all adapters"""
        for adapter in self.adapters.values():
            adapter.after_hook(method, args, kwargs, value, is_new_value)
