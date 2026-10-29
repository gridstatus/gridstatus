import importlib
import importlib.util
from typing import TYPE_CHECKING, Any

from gridstatus.version import __version__

from gridstatus.utils import (
    list_isos,
    get_iso,
    make_availability_table,
    get_interconnection_queues,
)
import gridstatus.base
import gridstatus.decorators

from gridstatus.base import Markets, NotSupported, NoDataFoundException

import gridstatus.utils

from gridstatus.utils import load_folder

if TYPE_CHECKING:
    from gridstatus import tests, viz
    from gridstatus.aeso import AESO
    from gridstatus.nyiso import NYISO
    from gridstatus.caiso import CAISO
    from gridstatus.ercot import Ercot
    from gridstatus.isone import ISONE
    from gridstatus.miso import MISO
    from gridstatus.spp import SPP
    from gridstatus.pjm import PJM
    from gridstatus.eia import EIA
    from gridstatus.ieso import IESO

    all_isos: list[type[gridstatus.base.ISOBase]]

# The ISO clients, `all_isos` and the submodules (such as `gridstatus.viz`) are
# imported on first access, so `import gridstatus` doesn't load every ISO module
# and the libraries they use (BeautifulSoup, pdfplumber, plotly).
_ISO_CLASS_MODULES = {
    "AESO": "gridstatus.aeso",
    "NYISO": "gridstatus.nyiso",
    "CAISO": "gridstatus.caiso",
    "Ercot": "gridstatus.ercot",
    "ISONE": "gridstatus.isone",
    "MISO": "gridstatus.miso",
    "SPP": "gridstatus.spp",
    "PJM": "gridstatus.pjm",
    "EIA": "gridstatus.eia",
    "IESO": "gridstatus.ieso",
}

_ALL_ISOS = ["AESO", "NYISO", "CAISO", "Ercot", "ISONE", "MISO", "SPP", "PJM", "IESO"]


def __getattr__(name: str) -> Any:
    if name in _ISO_CLASS_MODULES:
        value = getattr(importlib.import_module(_ISO_CLASS_MODULES[name]), name)
    elif name == "all_isos":
        value = [__getattr__(iso) for iso in _ALL_ISOS]
    elif (
        not name.startswith("__")
        and importlib.util.find_spec(f"{__name__}.{name}") is not None
    ):
        value = importlib.import_module(f"{__name__}.{name}")
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *_ISO_CLASS_MODULES, "all_isos", "tests", "viz"})


__all__ = [
    "AESO",
    "CAISO",
    "IESO",
    "ISONE",
    "MISO",
    "NYISO",
    "PJM",
    "SPP",
    "Ercot",
    "Markets",
    "NotSupported",
    "get_iso",
    "list_isos",
    "load_folder",
]
