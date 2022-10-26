---
title: ISODATA
file_format: mystnb
kernelspec:
  name: python3
---

# gridstatus Homepage

<p align="center">
<img width=75% src="https://github.com/kmax12/gridstatus/raw/75f0161f50466d4a13e01b57a695ac2a22fc0ca9/gridstatus-header.png" alt="gridstatus logo" />
</p>

<p align="center">
    <a href="https://github.com/kmax12/gridstatus/actions?query=branch%3Amain+workflow%3ATests" target="_blank">
        <img src="https://github.com/kmax12/gridstatus/workflows/Tests/badge.svg?branch=main" alt="Tests" />
    </a>
    <a href="https://badge.fury.io/py/gridstatus" target="_blank">
        <img src="https://badge.fury.io/py/gridstatus.svg?maxAge=2592000" alt="PyPI Version" />
    </a>
</p>

## What is gridstatus?

`gridstatus` is standardized Python API to electricity supply, demand, and pricing data for the major Independent System Operators (ISOs) in the United States.

Currently `gridstatus` supports CAISO, SPP, ISONE, MISO, Ercot, NYISO, and PJM.

We'd love to answer any usage or data access questions! Please let us know by posting a GitHub issue.

## 5 Minute Overview

First, we can see all of the ISOs that are supported

```{code-cell}
import gridstatus
gridstatus.list_isos()
```

Next, we can select an ISO we want to use

```{code-cell}
caiso = gridstatus.CAISO()
```

### Fuel Mix

all ISOs have the same API. Here is how we can get the fuel mix

```{code-cell}
caiso.get_latest_fuel_mix()
```

### Load

or the energy demand throughout the current day as a Pandas DataFrame

```{code-cell}
caiso.get_load_today()
```

### Supply

we can get today's supply in the same way

```{code-cell}
caiso.get_supply_today()
```

### Load Forecast

Another dataset we can query is the load forecast

```{code-cell}
nyiso = gridstatus.NYISO()
nyiso.get_forecast_today()
```

### Historical Data

When supported, you can use the historical method calls to get data for a specific day in the past. For example,

```{code-cell}
caiso.get_historical_load("Jan 1, 2020")
```

Frequently, we want to get data across multiple days. We can do that by providing a `start` and `end` parameter to any `iso.get_historical_*` method

```{code-cell}
:tags: [remove-input,remove-stdout,remove-stderr]
caiso.get_historical_load(start="Jan 1, 2020", end="Feb 1, 2020")
```

### Next Steps

The best part is these APIs work in the same way across all the supported ISOs!

```{toctree}
:maxdepth: 2
:caption: Getting Started

installation
availability
lmp
Examples/index

```

```{toctree}
:caption: Reference
:maxdepth: 1

changelog
api-reference
```
