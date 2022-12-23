---
file_format: mystnb
kernelspec:
  name: python3
---

# LMP Pricing Data

## Support

Below are the currently supported LMP markets

<!-- LMP AVAILABILITY TABLE START -->
|       | Method    | REAL_TIME_5_MIN   | REAL_TIME_15_MIN   | REAL_TIME_HOURLY   | DAY_AHEAD_HOURLY   |
|:------|:----------|:------------------|:-------------------|:-------------------|:-------------------|
| CAISO | `get_lmp` | &#10060;          | &#x2705;           | &#x2705;           | &#x2705;           |
| Ercot | `get_spp` | &#10060;          | &#x2705;           | &#10060;           | &#x2705;           |
| ISONE | `get_lmp` | &#x2705;          | &#10060;           | &#x2705;           | &#x2705;           |
| MISO  | `get_lmp` | &#x2705;          | &#10060;           | &#10060;           | &#x2705;           |
| NYISO | `get_lmp` | &#x2705;          | &#10060;           | &#10060;           | &#x2705;           |
| PJM   | `get_lmp` | &#x2705;          | &#10060;           | &#x2705;           | &#x2705;           |
| SPP   | -         | &#10060;          | &#10060;           | &#10060;           | &#10060;           |

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

The possible lmp query methods are `ISO.get_latest_lmp`, `ISO.get_lmp_today`, and `ISO.get_lmp`.

