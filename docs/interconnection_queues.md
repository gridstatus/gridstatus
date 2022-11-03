---
file_format: mystnb
kernelspec:
  name: python3
---

# Interconnection Queues

All ISOs support retrieving interconnection queues with `iso.get_interconnection_queue` methods.

Each of the ISOs report slightly differently fields for their queues, so a best effort was made to standardize column names. When a column couldn't be standardized, it is appended to end of the returned dataframe.

Below you can see preview of each of ISOs interconnection queues

## NYISO

```{code-cell}
import gridstatus
nyiso = gridstatus.NYISO()
nyiso.get_interconnection_queue()
```

## CAISO
```{code-cell}
caiso = gridstatus.CAISO()
caiso.get_interconnection_queue()
```

## SPP
```{code-cell}
spp = gridstatus.SPP()
spp.get_interconnection_queue()
```


## Ercot
```{code-cell}

ercot = gridstatus.Ercot()
ercot.get_interconnection_queue()
```

## MISO
```{code-cell}

miso = gridstatus.MISO()
miso.get_interconnection_queue()
```

## ISONE
```{code-cell}

isone = gridstatus.ISONE()
isone.get_interconnection_queue()
```

## PJM
```{code-cell}

pjm = gridstatus.PJM()
pjm.get_interconnection_queue()
```