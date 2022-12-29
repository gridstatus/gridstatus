class HookDispatch:
    def __init__(self):
        self.hooks = []

    def register_hook(self, hook):
        if hook not in self.hooks:
            self.hooks.append(hook)

    def unregister_hook(self, hook):
        if hook in self.hooks:
            self.hooks.remove(hook)

    def _before_hook(self, method, args, kwargs):
        for hook in self.hooks:
            hook.before_hook(method, args, kwargs)
