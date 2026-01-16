---
file_format: mystnb
kernelspec:
  name: python3
---

# LMP Pricing Data

Each ISO offers different LMP markets, but you can query them with a standardized API

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

You can see what markets are available by accessing the `markets` property of an ISO. For example

```{code-cell}
caiso.markets
```
