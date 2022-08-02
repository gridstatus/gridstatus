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
>>> caiso.get_current_status()
```

```
California ISO
Time: 2022-08-02 10:25:00-07:00
Status: Normal
Reserves: 2994 MW
```

```python
>>> caiso.get_fuel_mix()
```

```
ISO: California ISO
Total Production: 32702 MW
Time: 2022-08-02 10:25:00-07:00
+-------------+-------+-----------+
| Fuel        |    MW |   Percent |
|-------------+-------+-----------|
| Solar       | 12851 |      39.3 |
| Natural Gas | 10146 |      31   |
| Imports     |  3783 |      11.6 |
| Nuclear     |  2256 |       6.9 |
| Wind        |  1530 |       4.7 |
| Large Hydro |   954 |       2.9 |
| Geothermal  |   879 |       2.7 |
| Biomass     |   340 |       1   |
| Biogas      |   209 |       0.6 |
| Small hydro |   171 |       0.5 |
| Coal        |    16 |       0   |
| Other       |     0 |       0   |
| Batteries   |  -433 |      -1.3 |
+-------------+-------+-----------+
```

## Where does the data come from?

isodata uses publically available APIs provided by the ISOs, but falls back to webscraping if an API isn't available.

## Feedback Welcome

`isodata` is under active development. If there is any particular data you would like access to, let us know by posting an issue or emailing kmax12@gmail.com.

## Related projects

- [pyiso](https://github.com/WattTime/pyiso)
