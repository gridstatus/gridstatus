---
file_format: mystnb
kernelspec:
  name: python3
---

# LMP Pricing Data

## Overview

We are currently adding Locational Marginal Price (LMP). Even though each BA offers different markets, but you can query them with a standardized API

```{code-cell}
import isodata
nyiso = isodata.NYISO()
nyiso.get_lmp_today(market="REAL_TIME_5_MIN",
                    locations="ALL")
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

```{code-cell}
import isodata
caiso = isodata.CAISO()
caiso.get_lmp_today(market='DAY_AHEAD_HOURLY',
                    locations=["TH_NP15_GEN-APND", "TH_SP15_GEN-APND", "TH_ZP26_GEN-APND"])
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
caiso.markets
```

```
[<Markets.REAL_TIME_15_MIN: 'REAL_TIME_15_MIN'>, <Markets.REAL_TIME_HOURLY: 'REAL_TIME_HOURLY'>, <Markets.DAY_AHEAD_HOURLY: 'DAY_AHEAD_HOURLY'>]
```

The possible lmp query methods are `ISO.get_latest_lmp`, `ISO.get_lmp_today`, and `ISO.get_historical_lmp`.

## Supported Markets

Below are the currently support LMP markets.

<!-- LMP AVAILABILITY TABLE START -->
|                                       | Markets                                                    |
|:--------------------------------------|:-----------------------------------------------------------|
| Midcontinent ISO                      | `REAL_TIME_5_MIN`, `DAY_AHEAD_HOURLY`                      |
| California ISO                        | `REAL_TIME_15_MIN`, `REAL_TIME_HOURLY`, `DAY_AHEAD_HOURLY` |
| PJM                                   | `REAL_TIME_5_MIN`, `REAL_TIME_HOURLY`, `DAY_AHEAD_HOURLY`  |
| Electric Reliability Council of Texas |                                                            |
| Southwest Power Pool                  |                                                            |
| New York ISO                          | `REAL_TIME_5_MIN`, `DAY_AHEAD_HOURLY`                      |
| ISO New England                       | `REAL_TIME_5_MIN`, `REAL_TIME_HOURLY`, `DAY_AHEAD_HOURLY`  |
<!-- LMP AVAILABILITY TABLE END -->
