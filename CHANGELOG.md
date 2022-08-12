# Changelog

## vNext

- NYISO: added LMP prices for 2 markets: real time 5 minute and day ahead 5 minute
- CAISO: added LMP prices for 2 market: real time 15 minute and day ahead hourly
- MISO: added LMP prices for 2 market: real time 5 minute and day head hourly
- PJM: blocked on finding current day
- SPP: data available, but what nodes?
- ISONE: add lmp prices for 3 markets: real time 5 minutes, real time hourly, day ahead hourly
- ERCOT:
- Bug Fixes

https://www.ercot.com/mp/data-products/markets/real-time-market?id=NP6-788-CD

https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId=856969377

Todos

- doucment query date for day ahead market

## v0.4.0 - Aug 4, 2022

- NYISO: added all demand, fuel mix, and supply methods
- PJM: add all demand methods for
- SPP: today and latest demand
- MISO: added get demand today and latest supply
- ISONE: now has complete coverage after adding all fuel mix and supply methods
- Ercot: added today and latest supply

## v0.3.0 - Aug 3, 2022

- complete coverage of all methods for CAISO
- partial coverage for ERCOT, ISONE, PJM
- initial coverage for NYISO, MISO, SPP

## v0.2.0 - July 29, 2022

- Added `isodata.list_isos` and `isodata.get_iso`

## v0.1.0 - July 28, 2022

- Added `get_fuel_mix()` to all 7 isos
- Library structure
