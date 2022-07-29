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

`isodata` provides standardized API to access energy data to the major Independent System Operators in the United States.

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
>>> iso = isodata.get_iso('pjm')
>>> pjm = iso()
```

All ISOs have the same API. Here is how we can get the fuel mix

```python
>>> pjm.get_fuel_mix()
```

```
Total Production: 110490 MW
Time: 2022-07-29T09:00:00
+------------------+---------+-----------+
| Fuel             |      MW |   Percent |
|------------------+---------+-----------|
| Gas              | 48778.5 |      44.1 |
| Nuclear          | 32309.7 |      29.2 |
| Coal             | 24781   |      22.4 |
| Solar            |  2293.2 |       2.1 |
| Other Renewables |   684   |       0.6 |
| Hydro            |   495.7 |       0.4 |
| Other            |   398.8 |       0.4 |
| Wind             |   314.2 |       0.3 |
| Multiple Fuels   |   217.8 |       0.2 |
| Oil              |   217.1 |       0.2 |
| Storage          |     0   |       0   |
+------------------+---------+-----------+
```

## Where does the data come from?

isodata uses publically available APIs provided by the ISOs, but falls back to webscraping if an API isn't available.

## Feedback Welcome

`isodata` is under active development. If there is any particular data you would like access to, let us know by posting an issue or emailing kmax12@gmail.com.

## Related projects

- [pyiso](https://github.com/WattTime/pyiso)
