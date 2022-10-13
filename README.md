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
<a href="#installation"><b>Install</b></a> — 
<a href="#getting-started"><b>Getting Started</b></a> — 
<a href="#method-availability"><b>Method Availability</b></a> —  
<a href="#lmp-pricing-data"><b>LMP Data</b></a> —  
<a href="#supported-lmp-markets"><b>Supported LMP Markets</b></a> 
</p>

`isodata` provides a uniform API to access energy data from the major Independent System Operators (ISOs) in the United States.

Currently supports fuel mix, load, supply, load forecast, and LMP pricing data for CAISO, SPP, ISONE, MISO, Ercot, NYISO, and PJM. See [full availability](#method-availability) below.

We'd love to answer any usage or data access questions! Please let us know by posting a GitHub issue.

## Installation

`isodata` supports python 3.7+. Install with pip

```
python -m pip install isodata
```

Upgrade using the following command

```
python -m pip install --upgrade isodata
```

Check current version like this

```
>>> import isodata
>>> print(isodata.__version__)
0.6.0
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
>>> caiso.get_demand_today()
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
>>> caiso.get_supply_today()
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

Another dataset we can query is the load forecast

```
>>> nyiso = isodata.NYISO()
>>> nyiso.get_forecast_today()
```

```
                Forecast Time                      Time  Load Forecast
0   2022-08-19 00:00:00-04:00 2022-08-19 00:00:00-04:00          17078
1   2022-08-19 00:00:00-04:00 2022-08-19 01:00:00-04:00          16260
2   2022-08-19 00:00:00-04:00 2022-08-19 02:00:00-04:00          15631
3   2022-08-19 00:00:00-04:00 2022-08-19 03:00:00-04:00          15252
4   2022-08-19 00:00:00-04:00 2022-08-19 04:00:00-04:00          15195
..                        ...                       ...            ...
139 2022-08-19 00:00:00-04:00 2022-08-24 19:00:00-04:00          24340
140 2022-08-19 00:00:00-04:00 2022-08-24 20:00:00-04:00          23624
141 2022-08-19 00:00:00-04:00 2022-08-24 21:00:00-04:00          22585
142 2022-08-19 00:00:00-04:00 2022-08-24 22:00:00-04:00          21137
143 2022-08-19 00:00:00-04:00 2022-08-24 23:00:00-04:00          19717

[144 rows x 3 columns]
```

When supported, you can use the historical method calls to get data for a specific day in the past. For example,

```python
>>> caiso.get_historical_demand("Jan 1, 2020")
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
| `get_latest_status`       | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#10060;           | &#x2705;               | &#10060; |
| `get_latest_fuel_mix`     | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#x2705;           | &#x2705;               | &#x2705; |
| `get_latest_demand`       | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#x2705;           | &#x2705;               | &#x2705; |
| `get_latest_supply`       | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#x2705;           | &#x2705;               | &#x2705; |
| `get_fuel_mix_today`      | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#10060;           | &#10060;               | &#x2705; |
| `get_demand_today`        | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#x2705;           | &#x2705;               | &#x2705; |
| `get_forecast_today`      | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#x2705;           | &#x2705;               | &#x2705; |
| `get_supply_today`        | &#x2705;       | &#x2705;         | &#x2705;                                | &#x2705;          | &#10060;           | &#10060;               | &#x2705; |
| `get_storage_today`       | &#10060;       | &#x2705;         | &#10060;                                | &#10060;          | &#10060;           | &#10060;               | &#10060; |
| `get_historical_fuel_mix` | &#x2705;       | &#x2705;         | &#10060;                                | &#x2705;          | &#10060;           | &#10060;               | &#x2705; |
| `get_historical_demand`   | &#x2705;       | &#x2705;         | &#10060;                                | &#x2705;          | &#10060;           | &#10060;               | &#x2705; |
| `get_historical_forecast` | &#x2705;       | &#x2705;         | &#10060;                                | &#x2705;          | &#10060;           | &#10060;               | &#10060; |
| `get_historical_supply`   | &#x2705;       | &#x2705;         | &#10060;                                | &#x2705;          | &#10060;           | &#10060;               | &#x2705; |
| `get_historical_storage`  | &#10060;       | &#x2705;         | &#10060;                                | &#10060;          | &#10060;           | &#10060;               | &#10060; |
<!-- METHOD AVAILABILITY TABLE END -->

## LMP Pricing Data

We are currently adding Locational Marginal Price (LMP). Even though each BA offers different markets, but you can query them with a standardized API

```python
>>> import isodata
>>> nyiso = isodata.NYISO()
>>> nyiso.get_lmp_today("REAL_TIME_5_MIN", locations="ALL")
```

```
                          Time           Market Location Location Type    LMP  Energy  Congestion  Loss
0    2022-08-16 00:05:00-04:00  REAL_TIME_5_MIN   CAPITL          Zone  70.88   66.65        1.10  5.33
1    2022-08-16 00:05:00-04:00  REAL_TIME_5_MIN   CENTRL          Zone  68.91   66.64        0.00  2.27
2    2022-08-16 00:05:00-04:00  REAL_TIME_5_MIN   DUNWOD          Zone  75.44   66.65       -1.26  7.53
3    2022-08-16 00:05:00-04:00  REAL_TIME_5_MIN   GENESE          Zone  68.64   66.64        0.00  2.00
4    2022-08-16 00:05:00-04:00  REAL_TIME_5_MIN      H Q          Zone  64.58   66.65        0.00 -2.07
...                        ...              ...      ...           ...    ...     ...         ...   ...
3370 2022-08-16 20:15:00-04:00  REAL_TIME_5_MIN    NORTH          Zone  85.57   87.85        0.00 -2.28
3371 2022-08-16 20:15:00-04:00  REAL_TIME_5_MIN      NPX          Zone  78.73   87.85       15.36  6.24
3372 2022-08-16 20:15:00-04:00  REAL_TIME_5_MIN      O H          Zone  85.48   87.85        0.00 -2.37
3373 2022-08-16 20:15:00-04:00  REAL_TIME_5_MIN      PJM          Zone  94.45   87.85       -1.86  4.74
3374 2022-08-16 20:15:00-04:00  REAL_TIME_5_MIN     WEST          Zone  87.85   87.85        0.00  0.00

[3375 rows x 8 columns]
```

And here is querying CAISO

```python
>>> import isodata
>>> caiso = isodata.CAISO()
>>> caiso.get_lmp_today('DAY_AHEAD_HOURLY', locations=["TH_NP15_GEN-APND", "TH_SP15_GEN-APND", "TH_ZP26_GEN-APND"])
```

```
LMP_TYPE                      Time            Market          Location Location Type        LMP     Energy  Congestion     Loss
0        2022-08-16 00:00:00-07:00  DAY_AHEAD_HOURLY  TH_NP15_GEN-APND          None   89.48766   95.51493     -0.1531 -5.87417
1        2022-08-16 00:00:00-07:00  DAY_AHEAD_HOURLY  TH_SP15_GEN-APND          None   94.02489   95.51493      0.0000 -1.49003
2        2022-08-16 00:00:00-07:00  DAY_AHEAD_HOURLY  TH_ZP26_GEN-APND          None   90.57680   95.51493      0.0000 -4.93812
3        2022-08-16 01:00:00-07:00  DAY_AHEAD_HOURLY  TH_NP15_GEN-APND          None   86.38892   92.12283     -0.0223 -5.71162
4        2022-08-16 01:00:00-07:00  DAY_AHEAD_HOURLY  TH_SP15_GEN-APND          None   90.94366   92.12283      0.0000 -1.17917
..                             ...               ...               ...           ...        ...        ...         ...      ...
67       2022-08-16 22:00:00-07:00  DAY_AHEAD_HOURLY  TH_SP15_GEN-APND          None  131.45525  135.43710      0.0000 -3.98185
68       2022-08-16 22:00:00-07:00  DAY_AHEAD_HOURLY  TH_ZP26_GEN-APND          None  127.04000  135.43710      0.0000 -8.39710
69       2022-08-16 23:00:00-07:00  DAY_AHEAD_HOURLY  TH_NP15_GEN-APND          None  107.36120  113.91108      0.0000 -6.54989
70       2022-08-16 23:00:00-07:00  DAY_AHEAD_HOURLY  TH_SP15_GEN-APND          None  111.22278  113.91108      0.0000 -2.68830
71       2022-08-16 23:00:00-07:00  DAY_AHEAD_HOURLY  TH_ZP26_GEN-APND          None  108.01049  113.91108      0.0000 -5.90059

[72 rows x 8 columns]
```

You can see what markets are available by accessing the `markets` property of an iso. For, example

```
>>> caiso.markets
```

```
[<Markets.REAL_TIME_15_MIN: 'REAL_TIME_15_MIN'>, <Markets.REAL_TIME_HOURLY: 'REAL_TIME_HOURLY'>, <Markets.DAY_AHEAD_HOURLY: 'DAY_AHEAD_HOURLY'>]
```

The possible lmp query methods are `ISO.get_latest_lmp`, `ISO.get_lmp_today`, and `ISO.get_historical_lmp`.

## Supported LMP Markets

<!-- LMP AVAILABILITY TABLE START -->
|                                       | Markets                                                    |
|:--------------------------------------|:-----------------------------------------------------------|
| Midcontinent ISO                      | `REAL_TIME_5_MIN`, `DAY_AHEAD_HOURLY`                      |
| California ISO                        | `REAL_TIME_15_MIN`, `REAL_TIME_HOURLY`, `DAY_AHEAD_HOURLY` |
| PJM                                   | `REAL_TIME_5_MIN`, `REAL_TIME_HOURLY`, `DAY_AHEAD_HOURLY`  |
| Electric Reliability Council of Texas |                                                            |
| Southwest Power Pool                  |                                                            |
| New York ISO                          | `REAL_TIME_5_MIN`, `DAY_AHEAD_5_MIN`                       |
| ISO New England                       | `REAL_TIME_5_MIN`, `REAL_TIME_HOURLY`, `DAY_AHEAD_HOURLY`  |
<!-- LMP AVAILABILITY TABLE END -->

## Related projects

- [pyiso](https://github.com/WattTime/pyiso)
