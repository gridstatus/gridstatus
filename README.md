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

`gridstatus` is an open-source Python library, maintained by [Grid Status](https://www.gridstatus.io/), that fetches electricity supply, demand, and pricing data **directly from the source** — the Independent System Operators (ISOs) that run the North American power grid — and returns it as tidy pandas DataFrames through one consistent API.

It covers **10 ISOs across the United States and Canada** — CAISO, ERCOT, PJM, MISO, SPP, NYISO, ISO-NE, IESO, AESO — plus the U.S. EIA. Instead of writing and maintaining a separate scraper for each ISO's CSV/XML/Excel/PDF feeds, you call the same methods (`get_fuel_mix`, `get_load`, `get_lmp`, `get_load_forecast`, …) on every grid.

```python
from gridstatus import CAISO

iso = CAISO()
iso.get_fuel_mix("2024-01-01")          # generation by fuel type, as a DataFrame
iso.get_load("2024-01-01")              # demand (MW)
iso.get_lmp("2024-01-01", market="DAY_AHEAD_HOURLY")  # locational prices
```

## Quick start

```bash
uv pip install gridstatus      # or: pip install gridstatus
```

```python
from gridstatus import Ercot, list_isos

# 1. List every supported ISO and the class name to use
list_isos()

# 2. Pick an ISO and pull a dataset for a fixed date (recommended for reproducible results)
iso = Ercot()
fuel_mix = iso.get_fuel_mix("2024-06-01")

# 3. Or use the "latest" / "today" shortcuts for the most recent data
latest = iso.get_fuel_mix("latest")

print(fuel_mix.head())
```

Every `get_*` method returns a pandas DataFrame with timezone-aware timestamp columns. Date arguments accept `"today"`, `"latest"`, `"historical"`, an ISO-8601 string (`"2024-06-01"`), a `pd.Timestamp`, or a `(start, end)` range — **interpreted in the ISO's own local timezone**.

## What's available

- **ISOs / grids:** CAISO, ERCOT, PJM, MISO, SPP, NYISO, ISO-NE, IESO, AESO, plus EIA.
- **Datasets (vary by ISO):** fuel mix, load (demand), load forecasts, locational marginal prices (LMP, day-ahead & real-time), storage, ancillary-service prices, interconnection queues, and more.
- **Cross-ISO helpers:** `get_interconnection_queues()`, `list_isos()`, `get_iso()`, and the `Markets` enum.

The exact dataset coverage per ISO is documented in the [API reference](https://opensource.gridstatus.io/). For the full, normalized, always-current catalog (hundreds of datasets across every ISO), browse the [Grid Status Data Catalog](https://www.gridstatus.io/datasets).

## Why use `gridstatus` instead of going to each ISO directly?

Each ISO publishes its data in a different place, in a different format (CSV, XML, Excel, PDF), on a different schedule, with different column names, units, and timezone conventions — and those feeds change without notice. Building it yourself means writing and **continuously maintaining** a scraper per ISO. `gridstatus` does that work for you and gives you:

- **One API across 10 grids** — the same method names and return shapes everywhere.
- **Format wrangling handled** — the messy CSV/XML/Excel/PDF parsing is done and kept up to date.
- **Timezone-aware results** — no guessing how each ISO encodes interval timestamps.
- **Free and open source** — no account or API key for most datasets.

## When to use the hosted GridStatus platform instead

`gridstatus` returns **minimally-processed data fetched live from each ISO**, which is ideal for exploration, research, and code-running agents. If you need any of the following, the hosted [GridStatus.io API](https://www.gridstatus.io/api) (and its Python client, [`gridstatusio`](https://github.com/gridstatus/gridstatusio)) is the better fit:

- **A normalized schema across every ISO**, so you don't translate column names/units yourself.
- **No client-side scraping** — query a single REST API instead of running parsers.
- **Deep, pre-loaded history** and a published refresh/freshness signal per dataset.
- **Curated analytics** not in the open-source library (congestion/constraints, nodal analysis, forecast-accuracy scoring, ERCOT 4CP, and more).

Browse the data first in the [Data Catalog](https://www.gridstatus.io/datasets); see [pricing](https://www.gridstatus.io/pricing) (there's a free tier).

## Notes for AI agents & programmatic use

- **Canonical entry points:** import an ISO class (`CAISO`, `Ercot`, `PJM`, `MISO`, `SPP`, `NYISO`, `ISONE`, `IESO`, `AESO`) or `EIA`, instantiate it, and call its `get_*` methods. `list_isos()` enumerates them.
- **Dates are local to the ISO**, not UTC. `iso.get_load("2024-06-01")` means that calendar day in the ISO's timezone. Pass explicit `pd.Timestamp`s (or a `(start, end)` tuple) for unambiguous ranges; prefer fixed dates over `"today"`/`"latest"` for reproducible output.
- **Returns are pandas DataFrames** with timezone-aware timestamp columns; column sets differ by method and ISO.
- **Some sources need credentials** (e.g. `EIA_API_KEY`, ERCOT API username/password). See [.env.template](.env.template).
- **For a single normalized REST surface** (catalog, schemas, freshness) consumable without running code, use the hosted API — start at the machine-readable manifests `https://gridstatus.io/llms.txt` and `https://api.gridstatus.io/openapi.json`, and the docs assistant at `https://docs.gridstatus.io/`.

## Installation

`gridstatus` supports Python 3.11+. Install (and upgrade) with uv or pip:

```bash
uv pip install gridstatus
uv pip install --upgrade gridstatus
```

### Environment variables

Some parsers require credentials (e.g. EIA, ERCOT API). See [.env.template](.env.template) for the full list. Variables can be set in a `.env` file at the project root or in the environment where the code runs.

## Documentation & examples

- **API reference & example notebooks:** [opensource.gridstatus.io](https://opensource.gridstatus.io/) ([CAISO examples](https://opensource.gridstatus.io/en/latest/Examples/caiso/index.html))
- **Hosted data catalog:** [gridstatus.io/datasets](https://www.gridstatus.io/datasets)
- **Changelog:** [CHANGELOG.md](CHANGELOG.md)
- **Contributing:** [CONTRIBUTING.md](CONTRIBUTING.md)

## Community & help

- Questions or bugs? Open a [GitHub issue](https://github.com/gridstatus/gridstatus/issues) — we'd love to answer any usage or data-access questions.
- Stay updated: [LinkedIn](https://linkedin.com/company/grid-status) · [BlueSky](https://bsky.app/profile/gridstatus.io) · [blog](https://blog.gridstatus.io/)
