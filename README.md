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

`gridstatus` is a Python library that provides a uniform API for accessing electricity supply, demand, and pricing data for the major Independent System Operators (ISOs) in the United States. It currently supports data from CAISO, SPP, ISONE, MISO, ERCOT, NYISO, PJM, IESO,  and the EIA.

## GridStatus.io and Hosted API
To preview some of the data this library provides access to, visit [GridStatus.io](https://www.gridstatus.io/).

If you are trying to use our hosted API, you might want to check out the gridstatusio library [here](https://github.com/gridstatus/gridstatusio).

To learn more about our hosted API visit: https://www.gridstatus.io/api.

## Community

- Need Help? Post a [GitHub issue](https://github.com/gridstatus/gridstatus/issues)
- Want to stay updated? Follow us on Twitter [@grid_status](https://twitter.com/grid_status)
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
  - The `EIA` class
  - The `ErcotAPI` class
- Environment variables can be set in the following ways
  - In a `.env` file in the root of the project
  - In the environment where the code is run
- See [.env.template](.env.template) for the required environment variables and how to register for them


## Documentation and Examples

To learn more, visit the [documentation](https://opensource.gridstatus.io/) and view [example notebooks](https://opensource.gridstatus.io/en/latest/Examples/caiso/index.html).

## Get Help

We'd love to answer any usage or data access questions! Please let us know by posting a GitHub issue.
