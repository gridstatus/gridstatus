<p align="center">
<img width=75% src="./gridstatus-header.png" alt="gridstatus logo" />
</p>

<p align="center">
    <!-- disable until tests more reliable -->
    <!-- <a href="https://github.com/gridstatus/gridstatus/actions?query=branch%3Amain+workflow%3ATests" target="_blank">
        <img src="https://github.com/gridstatus/gridstatus/workflows/Tests/badge.svg?branch=main" alt="Tests" />
    </a> -->
    <a href="https://codecov.io/gh/gridstatus/gridstatus" target="_blank">
        <img src="https://codecov.io/gh/gridstatus/gridstatus/branch/main/graph/badge.svg" alt="Code Coverage"/>
    </a>
    <a href="https://badge.fury.io/py/gridstatus" target="_blank">
        <img src="https://badge.fury.io/py/gridstatus.svg?maxAge=2592000" alt="PyPI version">
    </a>
</p>

`gridstatus` is an open-source Python library, maintained by [Grid Status](https://www.gridstatus.io/), that fetches electricity market data directly from North American independent system operators (ISOs), regional transmission organizations (RTOs), and the U.S. EIA.

Available data includes load, fuel mix, forecasts, locational marginal prices (LMPs), storage, ancillary services, interconnection queues, and more. Coverage and historical availability vary by source.

## Contents

- [Why use `gridstatus`?](#why-use-gridstatus)
- [Installation](#installation)
- [Getting started](#getting-started)
- [What's available](#whats-available)
- [Notes for programmatic use](#notes-for-programmatic-use)
- [Documentation and examples](#documentation--examples)

## Why use `gridstatus`?

Choose between the open-source library and the hosted Grid Status API based on how you want to access and work with the data:

| Open-source `gridstatus` | Hosted [Grid Status API](https://www.gridstatus.io/api) and [`gridstatusio`](https://github.com/gridstatus/gridstatusio) |
|--------------------------|-----------------------------------------------------------------------------------------------------------------------|
| Minimally processed data fetched directly from ISO and EIA sources | Hosted data with consistent column names, timestamp formats, and DST handling |
| Python library with source-specific integrations and fields | Single REST API with a Python client |
| No Grid Status account; most datasets require no API key | Grid Status API key with free and paid plans |
| Historical availability depends on each source's retention policy | Historical data queryable immediately |
| Filtering capabilities vary by source | Consistent server-side filtering by time, columns, and row values |

Use `gridstatus` when you want data directly from the source in Python, need source-specific fields, or do not want a Grid Status account.

> [!TIP]
> **For normalized, hosted data, we recommend the [Grid Status Hosted API](https://www.gridstatus.io/api).** It provides a consistent schema across markets, a maintained historical archive, and server-side filtering. Python users can query it with [`gridstatusio`](https://github.com/gridstatus/gridstatusio); other languages can use the REST API directly.

Compared with integrating with each ISO yourself, `gridstatus` maintains the CSV, XML, Excel, PDF, and API parsers and handles source-specific timestamp conventions for you.

## Installation

`gridstatus` supports Python 3.10+. Install with uv or pip:

```bash
uv pip install gridstatus
# or
pip install gridstatus
```

To upgrade an existing installation:

```bash
uv pip install --upgrade gridstatus
```

## Getting started

```python
from gridstatus import CAISO, Ercot, SPP, list_isos

# See the ISO classes exposed by the discovery helper
list_isos()

# Pick an ISO and pull a dataset for a fixed date
iso = Ercot()
fuel_mix = iso.get_fuel_mix("2024-06-01")

# Source-specific methods expose other datasets and market intervals
ercot_lmp = Ercot().get_lmp_by_settlement_point("2024-06-01")
spp_lmp = SPP().get_lmp_real_time_5_min_by_location("2024-06-01")
real_time_15_min_lmp = CAISO().get_lmp_real_time_15_min("2024-06-01")
day_ahead_hourly_lmp = CAISO().get_lmp_day_ahead_hourly("2024-06-01")

print(fuel_mix.head())
```

Most dataset methods return pandas DataFrames, with timezone-aware columns for time-indexed data. Date arguments accept `"today"`, `"latest"`, `"historical"`, an ISO-8601 string (`"2024-06-01"`), a `pd.Timestamp`, or a `(start, end)` range — **interpreted in the ISO's own local timezone**.

## What's available

- **ISOs / RTOs / grids:** CAISO, ERCOT, PJM, MISO, SPP, NYISO, ISO-NE, IESO, and AESO, plus the U.S. EIA.
- **Datasets (vary by ISO):** fuel mix, load (demand), load forecasts, locational marginal prices (LMP, day-ahead & real-time), storage, ancillary-service prices, interconnection queues, and more.

The exact dataset coverage per ISO is documented in the [API reference](https://opensource.gridstatus.io/).

## Notes for programmatic use

- **Canonical entry points:** import an ISO class (`CAISO`, `Ercot`, `PJM`, `MISO`, `SPP`, `NYISO`, `ISONE`, `IESO`, `AESO`) or `EIA`, instantiate it, and call its `get_*` methods.
- **Dates are local to the ISO**, not UTC. `iso.get_load("2024-06-01")` means that calendar day in the ISO's timezone. Pass explicit `pd.Timestamp`s (or a `(start, end)` tuple) for unambiguous ranges; prefer fixed dates over `"today"`/`"latest"` for reproducible output.
- **Dataset methods generally return pandas DataFrames** with timezone-aware timestamp columns; column sets differ by method and ISO.
- **Some sources need credentials** (e.g. `EIA_API_KEY`, ERCOT API username/password). See [.env.template](.env.template).
- **For a single normalized REST surface** (catalog, schemas, freshness) consumable without running code, use the hosted API — start at the machine-readable manifests `https://gridstatus.io/llms.txt` and `https://api.gridstatus.io/openapi.json`, and the docs assistant at `https://docs.gridstatus.io/`.

## Environment variables

Some parsers require credentials (e.g. EIA, ERCOT API). See [.env.template](.env.template) for the full list. Variables can be set in a `.env` file at the project root or in the environment where the code runs.

## Documentation & examples

- **API reference & example notebooks:** [opensource.gridstatus.io](https://opensource.gridstatus.io/) ([CAISO examples](https://opensource.gridstatus.io/en/latest/Examples/caiso/index.html))
- **Hosted data catalog:** [gridstatus.io/datasets](https://www.gridstatus.io/datasets)
- **Changelog:** [CHANGELOG.md](CHANGELOG.md)
- **Contributing:** [CONTRIBUTING.md](CONTRIBUTING.md)

## Community & help

- Questions or bugs? Open a [GitHub issue](https://github.com/gridstatus/gridstatus/issues).
- **Support:** Grid Status support resources are primarily focused on hosted API users, but we'll do our best to help with open-source questions and issues here.
- Stay updated: [LinkedIn](https://linkedin.com/company/grid-status) · [BlueSky](https://bsky.app/profile/gridstatus.io) · [blog](https://blog.gridstatus.io/)
