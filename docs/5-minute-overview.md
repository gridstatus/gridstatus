---
file_format: mystnb
kernelspec:
  name: python3
---

# 5 Minute Overview

```{code-cell}
import isodata
isodata.list_isos()
```

First, we can see all of the ISOs that are supported

```{code-cell}
import isodata
isodata.list_isos()
```

Next, we can select an ISO we want to use

```{code-cell}
caiso = isodata.CAISO()
```

All ISOs have the same API. Here is how we can get the fuel mix

```{code-cell}
import requests
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':RC4-SHA'
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings
disable_warnings(InsecureRequestWarning)
caiso.get_latest_fuel_mix()
```

or the energy demand throughout the current day as a Pandas DataFrame

```{code-cell}
caiso.get_load_today()
```

we can get today's supply in the same way

```{code-cell}
caiso.get_supply_today()
```

Another dataset we can query is the load forecast

```{code-cell}
nyiso = isodata.NYISO()
nyiso.get_forecast_today()
```

When supported, you can use the historical method calls to get data for a specific day in the past. For example,

```{code-cell}
caiso.get_historical_load("Jan 1, 2020")
```

Frequently, we want to get data across multiple days. We can do that by providing a `start` and `end` parameter to any `iso.get_historical_*` method

```{code-cell}
:tags: [remove-input,remove-stdout,remove-stderr]
caiso.get_historical_load(start="Jan 1, 2020", end="Feb 1, 2020")
```

The best part is these APIs work in the same way across all the supported ISOs!
