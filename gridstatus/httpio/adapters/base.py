class BaseAdapter:
    def __init__(self, name):
        self.name = name

    def before_hook(self, method, args, kwargs):
        pass

    def before_filter(self, method, args, kwargs):
        pass

    def after_hook(self, method, args, kwargs, value, is_new_value=False):
        pass
