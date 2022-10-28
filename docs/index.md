---
title: ISODATA
file_format: mystnb
kernelspec:
  name: python3
---

# gridstatus Homepage

<p align="center">
<img width=75% src="https://github.com/kmax12/isodata/raw/c77f933e30bc24a33ef36496d5250da4605b214f/gridstatus-header.png" alt="gridstatus logo" />
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

`gridstatus` is a standardized Python API to electricity supply, demand, and pricing data for the major Independent System Operators (ISOs) in the United States.

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
caiso.get_fuel_mix("today")
```

### Load

or the energy demand throughout the current day as a Pandas DataFrame

```{code-cell}
caiso.get_load("today")
```

### Supply

we can get today's supply in the same way

```{code-cell}
caiso.get_supply("today")
```

### Load Forecast

Another dataset we can query is the load forecast

```{code-cell}
nyiso = gridstatus.NYISO()
nyiso.get_load_forecast("today")
```

### Historical Data

When supported, you can use the historical method calls to get data for a specific day in the past. For example,

```{code-cell}
caiso.get_load("Jan 1, 2020")
```

Frequently, we want to get data across multiple days. We can do that by providing a `start` and `end` parameter to any `iso.get_*` method

```{code-cell}
:tags: [remove-input,remove-stdout,remove-stderr]
caiso.get_load(start="Jan 1, 2020", end="Feb 1, 2020")
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

api-reference
changelog
```
