from gridstatus.base import ISOBase
from gridstatus.decorators import support_date_range

_BASE = "https://www.caiso.com/outlook/SP"
_HISTORY_BASE = "https://www.caiso.com/outlook/SP/History"


class AESO(ISOBase):
    """Alberta Electric System Operator (AESO)"""

    name = "Alberta Electric System Operator"
    iso_id = "aeso"
    default_timezone = "US/Mountain"

    status_homepage = ""
    interconnection_homepage = ""

    markets = []

    trading_hub_locations = []

    def get_status(self, date="latest", verbose=False) -> str:
        pass

    @support_date_range(frequency="1D")
    def get_fuel_mix(self, date, start=None, end=None, verbose=False):
        pass

    @support_date_range(frequency="1D")
    def get_load(self, date, end=None, verbose=False):
        pass

    @support_date_range(frequency="31D")
    def get_load_forecast(self, date, end=None, sleep=4, verbose=False):
        pass

    @support_date_range(frequency="1D")
    def get_storage(self, date, verbose=False):
        pass


if __name__ == "__main__":
    import gridstatus

    iso = gridstatus.AESO()
