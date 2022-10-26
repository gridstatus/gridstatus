---
file_format: mystnb
kernelspec:
  name: python3
---

# LMP Pricing Data

## Overview

We are currently adding Locational Marginal Price (LMP). Even though each BA offers different markets, but you can query them with a standardized API

```{code-cell}
import gridstatus
nyiso = gridstatus.NYISO()
nyiso.get_lmp_today(market="REAL_TIME_5_MIN", locations="ALL")
```

And here is querying CAISO

```{code-cell}
import gridstatus
caiso = gridstatus.CAISO()
locations = ["TH_NP15_GEN-APND", "TH_SP15_GEN-APND", "TH_ZP26_GEN-APND"]
caiso.get_lmp_today(market='DAY_AHEAD_HOURLY', locations=locations)
```

You can see what markets are available by accessing the `markets` property of an iso. For, example

```{code-cell}
caiso.markets
```

The possible lmp query methods are `ISO.get_latest_lmp`, `ISO.get_lmp_today`, and `ISO.get_historical_lmp`.

## Supported Markets

Below are the currently support LMP markets.

<!-- LMP AVAILABILITY TABLE START -->

|                                       | Markets                                                    |
| :------------------------------------ | :--------------------------------------------------------- |
| Midcontinent ISO                      | `REAL_TIME_5_MIN`, `DAY_AHEAD_HOURLY`                      |
| California ISO                        | `REAL_TIME_15_MIN`, `REAL_TIME_HOURLY`, `DAY_AHEAD_HOURLY` |
| PJM                                   | `REAL_TIME_5_MIN`, `REAL_TIME_HOURLY`, `DAY_AHEAD_HOURLY`  |
| Electric Reliability Council of Texas |                                                            |
| Southwest Power Pool                  |                                                            |
| New York ISO                          | `REAL_TIME_5_MIN`, `DAY_AHEAD_HOURLY`                      |
| ISO New England                       | `REAL_TIME_5_MIN`, `REAL_TIME_HOURLY`, `DAY_AHEAD_HOURLY`  |

<!-- LMP AVAILABILITY TABLE END -->
