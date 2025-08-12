---
file_format: mystnb
kernelspec:
  name: python3
---

# LMP Pricing Data

## Support

Below are the currently supported LMP markets

<!-- LMP AVAILABILITY TABLE START -->
|       | Method    | REAL_TIME_5_MIN           | REAL_TIME_15_MIN          | DAY_AHEAD_HOURLY          | REAL_TIME_HOURLY          | REAL_TIME_HOURLY_FINAL   | REAL_TIME_HOURLY_PRELIM   |
|:------|:----------|:--------------------------|:--------------------------|:--------------------------|:--------------------------|:-------------------------|:--------------------------|
| AESO  | -         | -                         | -                         | -                         | -                         | -                        | -                         |
| CAISO | `get_lmp` | latest, today, historical | latest, today, historical | latest, today, historical | -                         | -                        | -                         |
| Ercot | `get_lmp` | -                         | -                         | -                         | -                         | -                        | -                         |
| IESO  | -         | -                         | -                         | -                         | -                         | -                        | -                         |
| ISONE | `get_lmp` | latest, today, historical | -                         | today, historical         | latest, today, historical | -                        | -                         |
| MISO  | `get_lmp` | latest, today, historical | -                         | today, historical         | -                         | historical               | historical                |
| NYISO | `get_lmp` | latest, today, historical | latest, today             | latest, today, historical | -                         | -                        | -                         |
| PJM   | `get_lmp` | latest, today, historical | -                         | today, historical         | today, historical         | -                        | -                         |
| SPP   | -         | -                         | -                         | -                         | -                         | -                        | -                         |

<!-- LMP AVAILABILITY TABLE END -->


## How to use

We are currently adding Locational Marginal Price (LMP). Even though each BA offers different markets, but you can query them with a standardized API

```{code-cell}
import gridstatus
nyiso = gridstatus.NYISO()
nyiso.get_lmp(date="today", market="REAL_TIME_5_MIN", locations="ALL")
```

And here is querying CAISO

```{code-cell}
import gridstatus
caiso = gridstatus.CAISO()
locations = ["TH_NP15_GEN-APND", "TH_SP15_GEN-APND", "TH_ZP26_GEN-APND"]
caiso.get_lmp(date="today", market='DAY_AHEAD_HOURLY', locations=locations)
```

You can see what markets are available by accessing the `markets` property of an iso. For, example

```{code-cell}
caiso.markets
```
