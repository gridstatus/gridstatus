<p align="center">
<img width=75% src="/isodata-header.png" alt="isodata logo" />
</p>

<p align="center">
    <a href="https://github.com/kmax12/isodata/actions?query=branch%3Amain+workflow%3ATests" target="_blank">
        <img src="https://github.com/kmax12/isodata/workflows/Tests/badge.svg?branch=main" alt="Tests" />
    </a>
    <a href="https://badge.fury.io/py/isodata" target="_blank">
        <img src="https://badge.fury.io/py/isodata.svg?maxAge=2592000" alt="PyPI Version" />
    </a>
</p>

<p align="center">
<a href="#install"><b>Install</b></a> — 
<a href="#getting-started"><b>Getting Started</b></a> — 
<a href="#method-availability"><b>Method Availability</b></a> —  
<a href="#lmp-pricing-data"><b>LMP Data</b></a> —  
<a href="#supported-lmp-markets"><b>Supported LMP Markets</b></a> —  
<a href="#feedback-welcome"><b>Feedback</b></a>
</p>

`isodata` provides standardized API to access energy data from the major Independent System Operators (ISOs) in the United States.

## Install

`isodata` supports python 3.7+. Install with pip

```
python -m pip install isodata
```

## Getting Started

First, we can see all of the ISOs that are supported

```python
>>> import isodata
>>> isodata.list_isos()
```

```
                                    Name     Id
0                         California ISO  caiso
1  Electric Reliability Council of Texas  ercot
2                           New York ISO  nyiso
3                   Southwest Power Pool    spp
4                                    PJM    pjm
5                       Midcontinent ISO   miso
6                        ISO New England  isone
```

Next, we can select an ISO we want to use

```python
>>> iso = isodata.get_iso('caiso')
>>> caiso = iso()
```

All ISOs have the same API. Here is how we can get the fuel mix

```python
>>> caiso.get_latest_fuel_mix()
```

```
ISO: California ISO
Total Production: 43104 MW
Time: 2022-08-03 18:25:00-07:00
+-------------+-------+-----------+
| Fuel        |    MW |   Percent |
|-------------+-------+-----------|
| Natural Gas | 19868 |      46.1 |
| Solar       |  5388 |      12.5 |
| Imports     |  4997 |      11.6 |
| Wind        |  3887 |       9   |
| Large Hydro |  3312 |       7.7 |
| Nuclear     |  2255 |       5.2 |
| Batteries   |  1709 |       4   |
| Geothermal  |   886 |       2.1 |
| Biomass     |   344 |       0.8 |
| Small hydro |   234 |       0.5 |
| Biogas      |   208 |       0.5 |
| Coal        |    16 |       0   |
| Other       |     0 |       0   |
+-------------+-------+-----------+
```

or the energy demand throughout the current day as a Pandas DataFrame

```python
>>> iso.get_demand_today()
```

```
                         Time   Demand
0   2022-08-03 00:00:00-07:00  30076.0
1   2022-08-03 00:05:00-07:00  29966.0
2   2022-08-03 00:10:00-07:00  29893.0
3   2022-08-03 00:15:00-07:00  29730.0
4   2022-08-03 00:20:00-07:00  29600.0
..                        ...      ...
219 2022-08-03 18:15:00-07:00  41733.0
220 2022-08-03 18:20:00-07:00  41690.0
221 2022-08-03 18:25:00-07:00  41718.0
222 2022-08-03 18:30:00-07:00  41657.0
223 2022-08-03 18:35:00-07:00  41605.0

[224 rows x 2 columns]
```

we can get today's supply in the same way

```python
>>> iso.get_supply_today()
```

```
                         Time  Supply
0   2022-08-03 00:00:00-07:00   31454
1   2022-08-03 00:05:00-07:00   31366
2   2022-08-03 00:10:00-07:00   30985
3   2022-08-03 00:15:00-07:00   30821
4   2022-08-03 00:20:00-07:00   30667
..                        ...     ...
220 2022-08-03 18:20:00-07:00   43096
221 2022-08-03 18:25:00-07:00   43104
222 2022-08-03 18:30:00-07:00   43013
223 2022-08-03 18:35:00-07:00   42885
224 2022-08-03 18:40:00-07:00   42875

[225 rows x 2 columns]
```

to get data for a specific day, use the historical method calls. For example,

```python
>>> iso.get_historical_demand("Jan 1, 2020")
```

```
                         Time  Demand
0   2020-01-01 00:00:00-08:00   21533
1   2020-01-01 00:05:00-08:00   21429
2   2020-01-01 00:10:00-08:00   21320
3   2020-01-01 00:15:00-08:00   21272
4   2020-01-01 00:20:00-08:00   21193
..                        ...     ...
284 2020-01-01 23:40:00-08:00   20383
285 2020-01-01 23:45:00-08:00   20297
286 2020-01-01 23:50:00-08:00   20242
287 2020-01-01 23:55:00-08:00   20128
288 2020-01-01 00:00:00-08:00   20025

[289 rows x 2 columns]
```

The best part is these APIs work across all the supported ISOs

## Method Availability

Here is the current status of availability of each method for each ISO

<!-- METHOD AVAILABILITY TABLE START -->
|                           | New York ISO   | California ISO   | Electric Reliability Council of Texas   | ISO New England   | Midcontinent ISO   | Southwest Power Pool   | PJM      |
|:--------------------------|:---------------|:-----------------|:----------------------------------------|:------------------|:-------------------|:-----------------------|:---------|
| `get_latest_status`       | &#10060;       | &#x2705;         | &#x2705;                                | &#10060;          | &#10060;           | &#10060;               | &#10060; |
| `get_latest_fuel_mix`     | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#x2705;           | &#x2705;               | &#x2705; |
| `get_fuel_mix_today`      | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#10060;           | &#10060;               | &#x2705; |
| `get_fuel_mix_yesterday`  | &#x2705;       | &#x2705;         | &#10060;                                | &#x2705;          | &#10060;           | &#10060;               | &#x2705; |
| `get_historical_fuel_mix` | &#x2705;       | &#x2705;         | &#10060;                                | &#x2705;          | &#10060;           | &#10060;               | &#x2705; |
| `get_latest_demand`       | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#x2705;           | &#x2705;               | &#x2705; |
| `get_demand_today`        | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#x2705;           | &#x2705;               | &#x2705; |
| `get_demand_yesterday`    | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#10060;           | &#10060;               | &#x2705; |
| `get_historical_demand`   | &#x2705;       | &#x2705;         | &#10060;                                | &#x2705;          | &#10060;           | &#10060;               | &#x2705; |
| `get_latest_supply`       | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#x2705;           | &#x2705;               | &#x2705; |
| `get_supply_today`        | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#10060;           | &#10060;               | &#x2705; |
| `get_supply_yesterday`    | &#x2705;       | &#x2705;         | &#10060;                                | &#x2705;          | &#10060;           | &#10060;               | &#x2705; |
| `get_historical_supply`   | &#x2705;       | &#x2705;         | &#10060;                                | &#x2705;          | &#10060;           | &#10060;               | &#x2705; |
<!-- METHOD AVAILABILITY TABLE END -->

## LMP Pricing Data

We are currently adding Locational Marginal Price (LMP). Even though each BA offers different markets, but you can query them with a standardized API

```python
>>> import isodata
>>> iso = isodata.NYISO()
>>> iso.get_lmp_today(iso.REAL_TIME_5_MIN, nodes="ALL")
```

```
                          Time           Market    Zone     LMP  Energy  Congestion  Losses
0    2022-08-08 00:05:00-04:00  REAL_TIME_5_MIN  CAPITL  125.15   90.63      -26.64    7.88
1    2022-08-08 00:05:00-04:00  REAL_TIME_5_MIN  CENTRL   92.17   90.63        0.00    1.54
2    2022-08-08 00:05:00-04:00  REAL_TIME_5_MIN  DUNWOD   99.52   90.63        0.00    8.89
3    2022-08-08 00:05:00-04:00  REAL_TIME_5_MIN  GENESE   92.53   90.62        0.00    1.91
4    2022-08-08 00:05:00-04:00  REAL_TIME_5_MIN     H Q   88.09   90.63        0.00   -2.54
...                        ...              ...     ...     ...     ...         ...     ...
3970 2022-08-08 22:00:00-04:00  REAL_TIME_5_MIN   NORTH  110.17  120.71        7.04   -3.50
3971 2022-08-08 22:00:00-04:00  REAL_TIME_5_MIN     NPX  236.60  120.72     -107.18    8.70
3972 2022-08-08 22:00:00-04:00  REAL_TIME_5_MIN     O H  121.23  120.72       -4.49   -3.98
3973 2022-08-08 22:00:00-04:00  REAL_TIME_5_MIN     PJM  146.13  120.71      -20.23    5.19
3974 2022-08-08 22:00:00-04:00  REAL_TIME_5_MIN    WEST  125.26  120.72       -5.02   -0.48

[3975 rows x 7 columns]
```

And here is querying CAISO

```
>>> import isodata
>>> iso = isodata.CAISO()
>>> iso.get_lmp_today(iso.DAY_AHEAD_HOURLY, nodes=None)
```

## Supported LMP Markets

<!-- LMP AVAILABILITY TABLE START -->
|                                       | Markets                                                    |
|:--------------------------------------|:-----------------------------------------------------------|
| Midcontinent ISO                      | `REAL_TIME_5_MIN`, `DAY_AHEAD_HOURLY`                      |
| California ISO                        | `REAL_TIME_15_MIN`, `REAL_TIME_HOURLY`, `DAY_AHEAD_HOURLY` |
| PJM                                   |                                                            |
| Electric Reliability Council of Texas |                                                            |
| Southwest Power Pool                  |                                                            |
| New York ISO                          | `REAL_TIME_5_MIN`, `DAY_AHEAD_5_MIN`                       |
| ISO New England                       | `REAL_TIME_5_MIN`, `REAL_TIME_HOURLY`, `DAY_AHEAD_HOURLY`  |
<!-- LMP AVAILABILITY TABLE END -->

## Feedback Welcome

`isodata` is under active development. If there is any particular data you would like access to, let us know by posting an issue or emailing kmax12@gmail.com.

## Related projects

- [pyiso](https://github.com/WattTime/pyiso)
