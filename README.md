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

`gridstatus` is an open source Python library, maintained by [Grid Status](https://www.gridstatus.io/), that fetches electricity market data directly from North American independent system operators (ISOs), regional transmission organizations (RTOs), and the U.S. EIA.

<table>
  <tr>
    <td>
      ⚡ <strong>Have you considered our <a href="https://www.gridstatus.io/api">Hosted API</a>?</strong><br>
      It provides a consistent schema across markets, a maintained historical archive, and server-side filtering. Python users can query it with <a href="https://github.com/gridstatus/gridstatusio"><code>gridstatusio</code></a>; other languages can use the REST API directly.
    </td>
  </tr>
</table>

## Contents

- [Open source `gridstatus` vs. the hosted Grid Status API](#open-source-gridstatus-vs-the-hosted-grid-status-api)
- [Installation](#installation)
- [Getting started](#getting-started)
- [What's available](#whats-available)
- [Resources and help](#resources--help)

## Open source `gridstatus` vs. the hosted Grid Status API

Choose between the open source library and the hosted Grid Status API based on how you want to access and work with the data:

| Open source `gridstatus` | Hosted [Grid Status API](https://www.gridstatus.io/api) and [`gridstatusio`](https://github.com/gridstatus/gridstatusio) |
|--------------------------|-----------------------------------------------------------------------------------------------------------------------|
| Minimally processed data fetched directly from ISO and EIA sources | Hosted data with consistent column names, timestamp formats, and DST handling |
| Python library with source-specific integrations and fields | Single REST API with a Python client |
| No Grid Status account; most datasets require no API key | Grid Status API key with free and paid plans |
| Historical availability depends on each source's retention policy | Historical data queryable immediately |
| Filtering capabilities vary by source | Consistent server-side filtering by time, columns, and row values |

Use `gridstatus` when you want data directly from the source in Python, need source-specific fields, or do not want a Grid Status account.

Compared with integrating with each ISO yourself, `gridstatus` maintains the CSV, XML, Excel, PDF, and API parsers and handles source-specific timestamp conventions for you.

## Installation

`gridstatus` supports Python 3.10+. Install with uv or pip:

```bash
uv pip install gridstatus
# or
pip install gridstatus
```

Some sources require credentials, such as an EIA API key or ERCOT API username and password. See [.env.template](.env.template) for the full list and supported environment variables.

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

Most dataset methods return pandas DataFrames, with timezone-aware columns for time-indexed data; columns vary by method and source. Date arguments accept `"today"`, `"latest"`, `"historical"`, an ISO-8601 string (`"2024-06-01"`), a `pd.Timestamp`, or a `(start, end)` range — **interpreted in the ISO's own local timezone**. Use fixed dates for reproducible results.

## What's available

Available data includes load, fuel mix, forecasts, locational marginal prices (LMPs), storage, ancillary services, interconnection queues, and more. Coverage and historical availability vary by source.

- **ISOs / RTOs / grids:** CAISO, ERCOT, PJM, MISO, SPP, NYISO, ISO-NE, IESO, and AESO, plus the U.S. EIA.
- **Datasets (vary by ISO):** fuel mix, load (demand), load forecasts, locational marginal prices (LMP, day-ahead & real-time), storage, ancillary-service prices, interconnection queues, and more.

The exact dataset coverage per ISO is documented in the [API reference](https://opensource.gridstatus.io/).

## Resources & help

- **Open source:** [API reference and examples](https://opensource.gridstatus.io/) · [Changelog](CHANGELOG.md) · [Contributing](CONTRIBUTING.md)
- **Hosted API:** [data catalog](https://www.gridstatus.io/datasets) · [OpenAPI spec](https://api.gridstatus.io/openapi.json) · [docs assistant](https://docs.gridstatus.io/)
- **Support:** Open a [GitHub issue](https://github.com/gridstatus/gridstatus/issues) for questions or bugs. Grid Status support is primarily focused on hosted API users, but we'll do our best to help with open source issues.
