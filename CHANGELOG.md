# Changelog

## vNext

- Add interconnection queue to data for SPP, NYISO, ERCOT, ISONE, PJM, MISO, and CAISO
- Add `get_generators` and `get_loads` to NYISO

## v0.12.0 - Oct 28, 2022

- Can now use `"today"` are value for `end` when querying date range

    ```python
    nyiso.get_fuel_mix(start="Jan 1, 2022", end="today")
    ```


### Breaking Changes

- Simplify method naming. This applies to all method. See below for example

#### New API

```python
nyiso.get_fuel_mix("latest")
nyiso.get_fuel_mix("today")
nyiso.get_fuel_mix("jan 1, 2022")
```

#### Old API
```python
nyiso = gridstatus.NYISO()
nyiso.get_latest_fuel_mix()
nyiso.get_fuel_mix_today()
nyiso.get_historical_fuel_mix("jan 1, 2022")
```


## v0.11.0 - Oct 26, 2022

- Renamed library to `gridstatus`
- New [Documentation](https://docs.gridstatus.io)!
- Add Examples Notebooks
- Renamed all demand methods to load

## v0.10.0 - Oct 24, 2022

- Support both Generator and Zone for NYISO LMPs
- Optimize NYISO Date Range Queries over Historical Data

## v0.9.0 - Oct 21, 2022

- Support querying by date range for CAISO, PJM, NYISO, and ISONE `get_historical_*` Methods
- Add gas prices to CAISO
- Add GHG allowance price to CAISO

## v0.8.0 - Oct 13, 2022

- PJM: add lmp prices for 3 markets: real time 5 minutes, real time hourly, day ahead hourly
- Add notes to Ercot status
- Add `.status_homepage` url to ISOs that report a status
- Add Ercot Historical RTM Settlement Point Prices (SPPs)
- Refactor storage API to support non-battery storage types

## v0.7.0 - Aug 23, 2022

- Added load forecasting to NYISO, PJM, CAISO, ISONE, Ercot, SPP, MISO
- Add battery charging and discharging to CAISO
- Removed yesterday methods. Use `get_historical_*()` instead
- Add get latest status to SPP

## v0.6.0 - Aug 17, 2022

- ISONE: add system status
- NYISO: add system status
- Improve LMP return format
- Bug fixes

## v0.5.0 - Aug 12, 2022

- CAISO: added LMP prices for 3 market: real time 15 minute (FMM), real time hours (HASP), day ahead hourly (DAM)
- NYISO: added LMP prices for 2 markets: real time 5 minute and day ahead 5 minute
- MISO: added LMP prices for 2 market: real time 5 minute and day head hourly
- ISONE: add lmp prices for 3 markets: real time 5 minutes, real time hourly, day ahead hourly
- Bug fixes

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
