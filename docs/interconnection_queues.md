---
file_format: mystnb
kernelspec:
  name: python3
---

# Interconnection Queues

All ISOs support retreving interconnection queues with the `iso.get_interconnection_queue`

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

## SPP
```{code-cell}
spp = gridstatus.SPP()
spp.get_interconnection_queue()
```


## MISO
```{code-cell}
miso = gridstatus.MISO()
miso.get_interconnection_queue()
```

## Ercot
```{code-cell}
ercot = gridstatus.Ercot()
ercot.get_interconnection_queue()
```