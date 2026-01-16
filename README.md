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

The `gridstatus` open source library is a Python library maintained by [Grid Status](https://www.gridstatus.io/) that provides a consistent API for accessing raw electricity supply, demand, and pricing data for the major Independent System Operators (ISOs) in the United States and Canada. It currently supports data from CAISO, SPP, ISONE, MISO, ERCOT, NYISO, PJM, IESO, AESO, and the EIA.

## GridStatus.io and Hosted API

This library provides minimally-processed data. If you need production-ready data, consider using our [hosted API](https://www.gridstatus.io/api) or visit [GridStatus.io](https://www.gridstatus.io/) to see the data in a web interface.

If you are trying to use our hosted API, you might want to check out the [`gridstatusio` library](https://github.com/gridstatus/gridstatusio).

## Community

- Need Help? Post a [GitHub issue](https://github.com/gridstatus/gridstatus/issues)
- Want to stay updated? Follow us on
  - [LinkedIn](https://linkedin.com/company/grid-status)
  - [BlueSky](https://bsky.app/profile/gridstatus.io)
- Read [our blog](https://blog.gridstatus.io/)
- Want to contribute? Read our [Contributing Guide](CONTRIBUTING.md)

## Installation

`gridstatus` supports python 3.11+. Install with uv

```
uv pip install gridstatus
```

Upgrade using the following command

```
uv pip install --upgrade gridstatus
```

## Environment Variables

- Some parsers require the use of environment variables
- See [.env.template](.env.template) for the required environment variables
- Environment variables can be set in the following ways
  - In a `.env` file in the root of the project
  - In the environment where the code is run


## Documentation and Examples

To learn more, visit the [documentation](https://opensource.gridstatus.io/) and view [example notebooks](https://opensource.gridstatus.io/en/latest/Examples/caiso/index.html).

## Get Help

We'd love to answer any usage or data access questions! Please let us know by posting a GitHub issue.
