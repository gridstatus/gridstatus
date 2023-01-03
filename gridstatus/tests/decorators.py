import pytest


class with_markets:
    def __init__(self, *markets):
        self.markets = markets

    def __call__(self, *args, **kwargs):
        return pytest.mark.parametrize("market", self.markets)(*args, **kwargs)
