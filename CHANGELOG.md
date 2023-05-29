# Changelog

## vNext

- Add EIA 930 Region Data
- Add Hourly Weather and Forecast Zone Loads ERCOT

### `get_load_by_weather_zone`
```python
>>> import gridstatus
>>> iso = gridstatus.Ercot()
>>> iso.get_load_by_weather_zone("today")
                        Time            Interval Start              Interval End     COAST     EAST  FAR_WEST    NORTH   NORTH_C  SOUTHERN  SOUTH_C     WEST     TOTAL
0  2023-05-29 00:00:00-05:00 2023-05-29 00:00:00-05:00 2023-05-29 01:00:00-05:00  12802.47  1521.40   5502.14  1135.57  11420.05   3465.99  6992.00  1136.75  43976.38
1  2023-05-29 01:00:00-05:00 2023-05-29 01:00:00-05:00 2023-05-29 02:00:00-05:00  12290.54  1416.19   5388.76  1078.17  10732.84   3260.38  6566.67  1221.94  41955.50
2  2023-05-29 02:00:00-05:00 2023-05-29 02:00:00-05:00 2023-05-29 03:00:00-05:00  11883.53  1341.91   5433.55  1053.20  10243.94   3122.02  6322.52  1136.98  40537.65
3  2023-05-29 03:00:00-05:00 2023-05-29 03:00:00-05:00 2023-05-29 04:00:00-05:00  11635.55  1281.43   5488.97  1065.24   9821.62   3041.44  6151.03  1028.75  39514.03
4  2023-05-29 04:00:00-05:00 2023-05-29 04:00:00-05:00 2023-05-29 05:00:00-05:00  11563.59  1260.45   5513.39  1086.53   9813.76   3009.98  6106.10   958.43  39312.23
5  2023-05-29 05:00:00-05:00 2023-05-29 05:00:00-05:00 2023-05-29 06:00:00-05:00  11546.74  1312.40   5483.92  1116.81   9868.26   3083.50  6154.60   987.59  39553.82
6  2023-05-29 06:00:00-05:00 2023-05-29 06:00:00-05:00 2023-05-29 07:00:00-05:00  11472.33  1337.69   5498.00  1078.45   9950.64   3080.14  6241.64  1087.81  39746.69
7  2023-05-29 07:00:00-05:00 2023-05-29 07:00:00-05:00 2023-05-29 08:00:00-05:00  11597.62  1394.89   5505.14  1044.43  10157.48   3193.75  6351.98  1103.64  40348.91
8  2023-05-29 08:00:00-05:00 2023-05-29 08:00:00-05:00 2023-05-29 09:00:00-05:00  12191.42  1515.69   5543.69  1075.54  10948.29   3339.00  6676.52  1054.30  42344.45
9  2023-05-29 09:00:00-05:00 2023-05-29 09:00:00-05:00 2023-05-29 10:00:00-05:00  13055.09  1655.85   5638.38  1128.50  12185.78   3698.18  7186.46  1124.93  45673.17
10 2023-05-29 10:00:00-05:00 2023-05-29 10:00:00-05:00 2023-05-29 11:00:00-05:00  13936.82  1782.10   5711.86  1201.75  13608.80   4035.68  7758.54  1216.78  49252.32
11 2023-05-29 11:00:00-05:00 2023-05-29 11:00:00-05:00 2023-05-29 12:00:00-05:00  14686.72  1915.27   5815.34  1310.87  14806.77   4373.96  8346.08  1258.92  52513.94
12 2023-05-29 12:00:00-05:00 2023-05-29 12:00:00-05:00 2023-05-29 13:00:00-05:00  15401.73  2037.31   5880.35  1383.49  15928.52   4634.32  8845.87  1346.79  55458.39
>>> 
```

### get_load_by_forecast_zone
```python
>>> import gridstatus
>>> iso = gridstatus.Ercot()
>>> iso.get_load_by_forecast_zone("today")
                        Time            Interval Start              Interval End     NORTH     SOUTH     WEST   HOUSTON     TOTAL
0  2023-05-29 00:00:00-05:00 2023-05-29 00:00:00-05:00 2023-05-29 01:00:00-05:00  13527.07  11281.31  6814.18  12353.81  43976.38
1  2023-05-29 01:00:00-05:00 2023-05-29 01:00:00-05:00 2023-05-29 02:00:00-05:00  12712.64  10663.39  6719.82  11859.64  41955.50
2  2023-05-29 02:00:00-05:00 2023-05-29 02:00:00-05:00 2023-05-29 03:00:00-05:00  12136.45  10237.82  6696.51  11466.87  40537.65
3  2023-05-29 03:00:00-05:00 2023-05-29 03:00:00-05:00 2023-05-29 04:00:00-05:00  11660.22   9939.95  6686.32  11227.54  39514.03
4  2023-05-29 04:00:00-05:00 2023-05-29 04:00:00-05:00 2023-05-29 05:00:00-05:00  11640.31   9836.80  6677.03  11158.10  39312.23
5  2023-05-29 05:00:00-05:00 2023-05-29 05:00:00-05:00 2023-05-29 06:00:00-05:00  11764.91   9968.13  6678.87  11141.91  39553.82
6  2023-05-29 06:00:00-05:00 2023-05-29 06:00:00-05:00 2023-05-29 07:00:00-05:00  11854.48  10083.56  6738.40  11070.26  39746.69
7  2023-05-29 07:00:00-05:00 2023-05-29 07:00:00-05:00 2023-05-29 08:00:00-05:00  12097.60  10317.04  6743.06  11191.20  40348.91
8  2023-05-29 08:00:00-05:00 2023-05-29 08:00:00-05:00 2023-05-29 09:00:00-05:00  13016.18  10789.71  6774.37  11764.20  42344.45
9  2023-05-29 09:00:00-05:00 2023-05-29 09:00:00-05:00 2023-05-29 10:00:00-05:00  14413.92  11712.27  6949.33  12597.65  45673.17
10 2023-05-29 10:00:00-05:00 2023-05-29 10:00:00-05:00 2023-05-29 11:00:00-05:00  15993.79  12683.08  7126.87  13448.59  49252.32
11 2023-05-29 11:00:00-05:00 2023-05-29 11:00:00-05:00 2023-05-29 12:00:00-05:00  17376.62  13648.03  7316.87  14172.42  52513.94
12 2023-05-29 12:00:00-05:00 2023-05-29 12:00:00-05:00 2023-05-29 13:00:00-05:00  18653.93  14462.30  7479.66  14862.50  55458.39
```


### Breaking Changes

- `iso.get_load("latest")` now returns a dataframe in the same format as `iso.get_load("today")` with as much data that can be fetched in one request to underlying endpoint

## v0.21.0 - May 22,2023

- Add initial support for EIA V2 API
- Date ranges can be provided as either separate start/end arguments or a tuple to date.

```python
# both do the same thing
iso.get_load(start="Jan 1, 2023", end="March 1, 2023")
iso.get_load(date=("Jan 1, 2023", "March 1, 2023"))
```

### CAISO

- Add support for querying larger set of CAISO Oasis Datasets with `caiso.get_oasis_dateset`
- add `CAISO.get_curtailed_non_operational_generator_report` to parse this [daily report](http://www.caiso.com/market/Pages/OutageManagement/CurtailedandNonOperationalGenerators.aspx)
- Support hourly start/end time for CAISO LMPs

### SPP

- Update SPP fuel mix source. Add helper function to parse historical fuel mix data back to 2011.
- Add `SPP.get_ver_curtailments` to return curtailment data for SPP
- Support self scheduled vs market breakdown in `SPP.get_fuel_mix` using `detailed=True` parameter

### ISONE

- Add ISONE BTM solar

```
>>> iso = gridstatus.ISONE()
>>> iso.get_btm_solar("today")
                         Time            Interval Start              Interval End  BTM Solar
0   2023-04-14 00:00:00-04:00 2023-04-14 00:00:00-04:00 2023-04-14 00:05:00-04:00      0.000
1   2023-04-14 00:05:00-04:00 2023-04-14 00:05:00-04:00 2023-04-14 00:10:00-04:00      0.000
2   2023-04-14 00:10:00-04:00 2023-04-14 00:10:00-04:00 2023-04-14 00:15:00-04:00      0.000
3   2023-04-14 00:15:00-04:00 2023-04-14 00:15:00-04:00 2023-04-14 00:20:00-04:00      0.000
4   2023-04-14 00:20:00-04:00 2023-04-14 00:20:00-04:00 2023-04-14 00:25:00-04:00      0.000
..                        ...                       ...                       ...        ...
164 2023-04-14 13:40:00-04:00 2023-04-14 13:40:00-04:00 2023-04-14 13:45:00-04:00   4356.833
165 2023-04-14 13:45:00-04:00 2023-04-14 13:45:00-04:00 2023-04-14 13:50:00-04:00   4328.750
166 2023-04-14 13:50:00-04:00 2023-04-14 13:50:00-04:00 2023-04-14 13:55:00-04:00   4300.667
167 2023-04-14 13:55:00-04:00 2023-04-14 13:55:00-04:00 2023-04-14 14:00:00-04:00   4272.583
168 2023-04-14 14:00:00-04:00 2023-04-14 14:00:00-04:00 2023-04-14 14:05:00-04:00   4244.500

[169 rows x 4 columns]
```

### ERCOT

- `Ercot.get_fuel_mix("latest")` now returns last two days of data.

## v0.20.0 - March 24, 2023

- Add `Interval Start` and `Interval End` time stamps to every applicable time series to avoid ambiguity. The `Time` column will be dropped in favor of just these two columns in next release

  ### Breaking Changes

  - Removed `FuelMix` class. `iso.get_fuel_mix(date="latest")` now returns a DataFrame with a single row to make API consistent with other ways of calling the method.

## v0.19.0 - Feb 19, 2023

- Updated ISONE Interconnection Queue to contain completed and withdrawn projects
- Add all areas to PJM get_load
- Add load over time visualization

## v0.18.0 - Jan 27, 2023

- Update CAISO LMP markets to support real time five minute
- Fix bug affecting NYISO interconnection queues

  ### Breaking Changes

  The following changes were made to CAISO Market:

  - `REAL_TIME_HOURLY` removed since this market incorrectly mapped to the HASP market
  - `REAL_TIME_5_MIN` added and maps to the RTD market
  - `REAL_TIME_15_MIN` and `DAY_AHEAD_HOURLY` unchanged

## v0.17.0 - Dec 30, 2022

- Add CAISO LMP Heat Map Example Notebook
- Add Settlement Point Prices for ERCOT
- SPP: Add today/latest LMP for Real Time 5 minute and Day Ahead Hourly (DAM)
- ERCOT load data is now returned with 5 minute frequency
- Add a guide on contributing to gridstatus

## v0.16.0 - Dec 15, 2022

- Ercot Fuel Mix Endpoint URL updated to include more fuel sources
- Ercot get_load supports more historical data

## v0.15.0 - Dec 2, 2022

- Add ability to get renewable curtailment data for CAISO
- Add Ancillary Service Methods for CAISO
- Add Ancillary Service Prices for ERCOT
- Add Ability to save intermediate results to disk when fetching data across multiple requests using `save_to` parameter

## v0.14.0 - Nov 8, 2022

- Add `get_capacity_prices` to NYISO
- Fix ISONE Daylight Savings Time handling

## v0.13.0 - Nov 2, 2022

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
