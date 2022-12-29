class AdapterDispatcher:
    def __init__(self):
        self.adapters = []

    def register_adapter(self, adapter):
        if adapter not in self.adapters:
            self.adapters.append(adapter)

    def unregister_adapter(self, hook):
        if hook in self.adapters:
            self.adapters.remove(hook)

    def _before_hook(self, method, args, kwargs):
        for adapter in self.adapters:
            value = adapter.before_hook(method, args, kwargs)
            if value is not None:
                return value
