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
>>> caiso.get_latest_status()
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

```python

>>> iso.get_day_ahead_prices(start_date="may 2, 2022",
                         num_days=1,
                         nodes=["TH_NP15_GEN-APND"])
```

```
LMP_TYPE	pnode	lmp	congestion	energy	losses	MGHG
interval start
2022-05-02 00:00:00-07:00	TH_NP15_GEN-APND	70.47015	0.00000	71.55783	-1.08768	0.0
2022-05-02 01:00:00-07:00	TH_NP15_GEN-APND	68.73617	0.00000	69.86093	-1.12476	0.0
2022-05-02 02:00:00-07:00	TH_NP15_GEN-APND	67.58898	0.00000	68.82788	-1.23890	0.0
2022-05-02 03:00:00-07:00	TH_NP15_GEN-APND	68.51088	0.00000	69.64611	-1.13523	0.0
2022-05-02 04:00:00-07:00	TH_NP15_GEN-APND	74.25415	0.00000	75.56136	-1.30721	0.0
2022-05-02 05:00:00-07:00	TH_NP15_GEN-APND	77.86464	0.00000	79.62434	-1.75970	0.0
2022-05-02 06:00:00-07:00	TH_NP15_GEN-APND	80.71256	0.00000	82.11675	-1.40420	0.0
2022-05-02 07:00:00-07:00	TH_NP15_GEN-APND	66.68011	0.00000	67.15016	-0.47005	0.0
2022-05-02 08:00:00-07:00	TH_NP15_GEN-APND	56.40186	7.88764	48.52878	-0.01456	0.0
2022-05-02 09:00:00-07:00	TH_NP15_GEN-APND	48.69254	9.08351	39.49055	0.11847	0.0
2022-05-02 10:00:00-07:00	TH_NP15_GEN-APND	41.55114	8.09662	33.41776	0.03676	0.0
2022-05-02 11:00:00-07:00	TH_NP15_GEN-APND	38.59000	10.35001	28.20333	0.03666	0.0
2022-05-02 12:00:00-07:00	TH_NP15_GEN-APND	36.89000	8.04264	28.82718	0.02018	0.0
2022-05-02 13:00:00-07:00	TH_NP15_GEN-APND	38.34660	8.08619	30.11885	0.14156	0.0
2022-05-02 14:00:00-07:00	TH_NP15_GEN-APND	40.00000	7.97681	31.85753	0.16566	0.0
2022-05-02 15:00:00-07:00	TH_NP15_GEN-APND	40.00000	6.59597	33.42743	-0.02340	0.0
2022-05-02 16:00:00-07:00	TH_NP15_GEN-APND	45.31324	8.08926	36.76443	0.45956	0.0
2022-05-02 17:00:00-07:00	TH_NP15_GEN-APND	59.39957	2.43405	56.88020	0.08532	0.0
2022-05-02 18:00:00-07:00	TH_NP15_GEN-APND	81.43412	0.00000	83.73688	-2.30276	0.0
2022-05-02 19:00:00-07:00	TH_NP15_GEN-APND	100.77406	0.00000	104.60251	-3.82845	0.0
2022-05-02 20:00:00-07:00	TH_NP15_GEN-APND	102.88135	0.00000	105.97585	-3.09449	0.0
2022-05-02 21:00:00-07:00	TH_NP15_GEN-APND	91.42214	0.00000	94.18167	-2.75952	0.0
2022-05-02 22:00:00-07:00	TH_NP15_GEN-APND	81.84891	0.00000	83.61315	-1.76424	0.0
2022-05-02 23:00:00-07:00	TH_NP15_GEN-APND	72.61741	0.00000	73.53662	-0.91921	0.0
```

## Method Availability

Here is the current status of availability of each method for each ISO

|                         | New York ISO | California ISO | Electric Reliability Council of Texas | ISO New England | Midcontinent ISO | Southwest Power Pool | PJM      |
| :---------------------- | :----------- | :------------- | :------------------------------------ | :-------------- | :--------------- | :------------------- | :------- |
| get_latest_status       | &#10060;     | &#x2705;       | &#x2705;                              | &#10060;        | &#10060;         | &#10060;             | &#10060; |
| get_latest_fuel_mix     | &#x2705;     | &#x2705;       | &#x2705;                              | &#x2705;        | &#x2705;         | &#x2705;             | &#x2705; |
| get_fuel_mix_today      | &#10060;     | &#x2705;       | &#x2705;                              | &#10060;        | &#10060;         | &#10060;             | &#x2705; |
| get_fuel_mix_yesterday  | &#10060;     | &#x2705;       | &#10060;                              | &#10060;        | &#10060;         | &#10060;             | &#x2705; |
| get_historical_fuel_mix | &#10060;     | &#x2705;       | &#10060;                              | &#10060;        | &#10060;         | &#10060;             | &#x2705; |
| get_latest_demand       | &#10060;     | &#x2705;       | &#x2705;                              | &#x2705;        | &#x2705;         | &#10060;             | &#10060; |
| get_demand_today        | &#10060;     | &#x2705;       | &#x2705;                              | &#x2705;        | &#10060;         | &#10060;             | &#10060; |
| get_demand_yesterday    | &#10060;     | &#x2705;       | &#x2705;                              | &#x2705;        | &#10060;         | &#10060;             | &#10060; |
| get_historical_demand   | &#10060;     | &#x2705;       | &#10060;                              | &#x2705;        | &#10060;         | &#10060;             | &#10060; |
| get_latest_supply       | &#10060;     | &#x2705;       | &#10060;                              | &#10060;        | &#10060;         | &#10060;             | &#x2705; |
| get_supply_today        | &#10060;     | &#x2705;       | &#10060;                              | &#10060;        | &#10060;         | &#10060;             | &#x2705; |
| get_supply_yesterday    | &#10060;     | &#x2705;       | &#10060;                              | &#10060;        | &#10060;         | &#10060;             | &#x2705; |
| get_historical_supply   | &#10060;     | &#x2705;       | &#10060;                              | &#10060;        | &#10060;         | &#10060;             | &#x2705; |

## Feedback Welcome

`isodata` is under active development. If there is any particular data you would like access to, let us know by posting an issue or emailing kmax12@gmail.com.

## Related projects

- [pyiso](https://github.com/WattTime/pyiso)
