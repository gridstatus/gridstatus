# Changelog

## vNext

### ERCOT

- Handle Energy Weighted Load Zone prices in real time SPPs
- Add ERCOT hourly wind forecast report
- Add ERCOT hourly solar forecast report
- Add `Ercot.get_60_day_sced_disclosure`
- Add `Ercot.get_60_day_dam_disclosure`

## v0.22.0 - July 3rd, 2023

### EIA

- Add EIA 930 Region Data

### NYISO

- Add NYISO BTM Solar Estimated Actuals and Forecast

#### `NYISO.get_btm_solar` and `NYISO.get_btm_solar_forecast`

```python
>>> import gridstatus
>>> iso = gridstatus.NYISO()
>>> iso.get_btm_solar("June 11, 2023")
Zone Name                      Time            Interval Start              Interval End   SYSTEM  CAPITL  CENTRL  DUNWOD  GENESE  HUD VL  LONGIL  MHK VL  MILLWD  N.Y.C.  NORTH    WEST
0         2023-06-11 00:00:00-04:00 2023-06-11 00:00:00-04:00 2023-06-11 01:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
1         2023-06-11 01:00:00-04:00 2023-06-11 01:00:00-04:00 2023-06-11 02:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
2         2023-06-11 02:00:00-04:00 2023-06-11 02:00:00-04:00 2023-06-11 03:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
3         2023-06-11 03:00:00-04:00 2023-06-11 03:00:00-04:00 2023-06-11 04:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
4         2023-06-11 04:00:00-04:00 2023-06-11 04:00:00-04:00 2023-06-11 05:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
5         2023-06-11 05:00:00-04:00 2023-06-11 05:00:00-04:00 2023-06-11 06:00:00-04:00    75.52   12.19    5.50    3.67    2.08   15.78   15.61    5.89    3.28    9.69   0.78    1.04
6         2023-06-11 06:00:00-04:00 2023-06-11 06:00:00-04:00 2023-06-11 07:00:00-04:00   347.14   52.22   45.36   11.78   18.37   52.20   74.82   27.10   10.11   37.13   4.66   13.39
7         2023-06-11 07:00:00-04:00 2023-06-11 07:00:00-04:00 2023-06-11 08:00:00-04:00   788.84  125.11  124.73   20.72   49.13  112.00  156.17   66.83   17.01   74.25  12.32   30.57
8         2023-06-11 08:00:00-04:00 2023-06-11 08:00:00-04:00 2023-06-11 09:00:00-04:00  1395.50  227.98  215.17   33.37   94.92  193.29  257.88  115.29   26.72  145.15  20.32   65.42
9         2023-06-11 09:00:00-04:00 2023-06-11 09:00:00-04:00 2023-06-11 10:00:00-04:00  2014.96  306.60  293.56   51.34  174.35  276.94  365.60  183.85   39.87  200.26  25.91   96.69
10        2023-06-11 10:00:00-04:00 2023-06-11 10:00:00-04:00 2023-06-11 11:00:00-04:00  2306.05  348.82  340.06   59.35  182.55  343.69  434.15  204.78   47.16  231.68  28.26   85.54
11        2023-06-11 11:00:00-04:00 2023-06-11 11:00:00-04:00 2023-06-11 12:00:00-04:00  2439.62  369.00  352.69   67.57  178.53  377.96  448.25  218.69   53.78  234.90  29.94  108.32
12        2023-06-11 12:00:00-04:00 2023-06-11 12:00:00-04:00 2023-06-11 13:00:00-04:00  2349.02  354.48  380.95   63.44  189.94  375.69  382.82  202.08   51.89  192.66  30.75  124.32
13        2023-06-11 13:00:00-04:00 2023-06-11 13:00:00-04:00 2023-06-11 14:00:00-04:00  2327.69  355.53  371.23   63.31  191.67  367.02  357.35  207.27   51.31  220.16  29.14  113.71
14        2023-06-11 14:00:00-04:00 2023-06-11 14:00:00-04:00 2023-06-11 15:00:00-04:00  2140.92  351.56  327.50   57.41  160.82  321.22  371.84  179.71   45.78  189.81  28.62  106.64
15        2023-06-11 15:00:00-04:00 2023-06-11 15:00:00-04:00 2023-06-11 16:00:00-04:00  1867.62  315.60  278.65   48.75  148.26  280.73  336.48  159.72   39.59  123.01  23.29  113.56
16        2023-06-11 16:00:00-04:00 2023-06-11 16:00:00-04:00 2023-06-11 17:00:00-04:00  1398.35  210.26  201.51   44.52   89.49  213.19  259.28  121.72   34.65  148.77  15.57   59.37
17        2023-06-11 17:00:00-04:00 2023-06-11 17:00:00-04:00 2023-06-11 18:00:00-04:00   913.03  121.45  145.47   27.15   58.16  130.41  198.08   70.26   21.10  104.86   9.42   26.67
18        2023-06-11 18:00:00-04:00 2023-06-11 18:00:00-04:00 2023-06-11 19:00:00-04:00   430.10   50.69   73.40   11.67   26.66   56.74   96.53   39.34    9.02   48.95   3.84   13.26
19        2023-06-11 19:00:00-04:00 2023-06-11 19:00:00-04:00 2023-06-11 20:00:00-04:00   107.35   11.48   21.66    2.37    9.35   14.07   22.17   10.27    2.04    8.60   1.04    4.30
20        2023-06-11 20:00:00-04:00 2023-06-11 20:00:00-04:00 2023-06-11 21:00:00-04:00     3.02    0.28    0.96    0.02    0.65    0.15    0.04    0.47    0.02    0.10   0.07    0.27
21        2023-06-11 21:00:00-04:00 2023-06-11 21:00:00-04:00 2023-06-11 22:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
22        2023-06-11 22:00:00-04:00 2023-06-11 22:00:00-04:00 2023-06-11 23:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
23        2023-06-11 23:00:00-04:00 2023-06-11 23:00:00-04:00 2023-06-12 00:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
>>> iso.get_btm_solar_forecast("today")
Zone Name                      Time            Interval Start              Interval End   SYSTEM  CAPITL  CENTRL  DUNWOD  GENESE  HUD VL  LONGIL  MHK VL  MILLWD  N.Y.C.  NORTH    WEST
0         2023-06-13 00:00:00-04:00 2023-06-13 00:00:00-04:00 2023-06-13 01:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
1         2023-06-13 01:00:00-04:00 2023-06-13 01:00:00-04:00 2023-06-13 02:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
2         2023-06-13 02:00:00-04:00 2023-06-13 02:00:00-04:00 2023-06-13 03:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
3         2023-06-13 03:00:00-04:00 2023-06-13 03:00:00-04:00 2023-06-13 04:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
4         2023-06-13 04:00:00-04:00 2023-06-13 04:00:00-04:00 2023-06-13 05:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
5         2023-06-13 05:00:00-04:00 2023-06-13 05:00:00-04:00 2023-06-13 06:00:00-04:00    57.32    3.63    9.98    2.10    3.93   10.66   11.98    3.31    2.01    8.07   0.29    1.38
6         2023-06-13 06:00:00-04:00 2023-06-13 06:00:00-04:00 2023-06-13 07:00:00-04:00   303.31   20.29   58.09    8.74   43.44   41.10   49.31   26.08    7.01   32.98   1.63   14.66
7         2023-06-13 07:00:00-04:00 2023-06-13 07:00:00-04:00 2023-06-13 08:00:00-04:00   799.45   56.94  157.19   21.35  119.39   99.10  120.88   75.64   15.77   84.06   3.72   45.43
8         2023-06-13 08:00:00-04:00 2023-06-13 08:00:00-04:00 2023-06-13 09:00:00-04:00  1395.52  108.66  264.74   31.27  177.04  170.41  234.83  129.29   23.84  162.06   7.32   86.08
9         2023-06-13 09:00:00-04:00 2023-06-13 09:00:00-04:00 2023-06-13 10:00:00-04:00  1894.43  161.33  333.21   39.50  214.77  227.34  335.53  180.70   30.16  238.29  10.76  122.86
10        2023-06-13 10:00:00-04:00 2023-06-13 10:00:00-04:00 2023-06-13 11:00:00-04:00  2212.83  201.05  351.76   46.03  236.31  259.80  416.81  216.86   34.63  288.12  13.13  148.32
11        2023-06-13 11:00:00-04:00 2023-06-13 11:00:00-04:00 2023-06-13 12:00:00-04:00  2404.65  229.98  354.43   53.17  240.34  290.11  481.46  238.53   39.90  305.56  14.55  156.63
12        2023-06-13 12:00:00-04:00 2023-06-13 12:00:00-04:00 2023-06-13 13:00:00-04:00  2508.15  245.19  352.06   61.52  241.77  326.65  506.34  251.76   46.07  307.98  15.72  153.11
13        2023-06-13 13:00:00-04:00 2023-06-13 13:00:00-04:00 2023-06-13 14:00:00-04:00  2564.26  260.66  349.02   67.90  238.71  352.04  514.83  256.89   51.74  301.20  17.23  154.05
14        2023-06-13 14:00:00-04:00 2023-06-13 14:00:00-04:00 2023-06-13 15:00:00-04:00  2499.92  278.79  338.70   69.54  216.72  355.34  501.53  254.09   53.64  279.68  17.90  134.01
15        2023-06-13 15:00:00-04:00 2023-06-13 15:00:00-04:00 2023-06-13 16:00:00-04:00  2259.78  272.63  315.10   60.82  186.29  325.26  447.05  245.04   47.69  241.39  16.73  101.79
16        2023-06-13 16:00:00-04:00 2023-06-13 16:00:00-04:00 2023-06-13 17:00:00-04:00  1784.95  229.94  259.40   44.24  145.55  246.00  343.06  222.07   35.28  180.64  13.31   65.47
17        2023-06-13 17:00:00-04:00 2023-06-13 17:00:00-04:00 2023-06-13 18:00:00-04:00  1160.11  152.03  182.57   25.18  103.51  145.48  194.02  182.51   19.87  107.41   8.26   39.27
18        2023-06-13 18:00:00-04:00 2023-06-13 18:00:00-04:00 2023-06-13 19:00:00-04:00   560.83   68.22   94.70   10.73   56.33   57.70   78.98  115.41    8.11   45.81   3.80   21.05
19        2023-06-13 19:00:00-04:00 2023-06-13 19:00:00-04:00 2023-06-13 20:00:00-04:00   160.49   20.51   28.38    2.26   16.44   13.18   18.44   41.29    1.71    9.91   1.33    7.07
20        2023-06-13 20:00:00-04:00 2023-06-13 20:00:00-04:00 2023-06-13 21:00:00-04:00    13.85    1.11    2.52    0.03    1.68    0.20    0.22    6.62    0.02    0.57   0.10    0.79
21        2023-06-13 21:00:00-04:00 2023-06-13 21:00:00-04:00 2023-06-13 22:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
22        2023-06-13 22:00:00-04:00 2023-06-13 22:00:00-04:00 2023-06-13 23:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
23        2023-06-13 23:00:00-04:00 2023-06-13 23:00:00-04:00 2023-06-14 00:00:00-04:00     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   0.00    0.00
```

### SPP

- Update to new SPP URLs

### Ercot

- Add Hourly Weather and Forecast Zone Loads ERCOT
- Add new ERCOT AS ECRS product
- Add `Ercot.get_as_monitor()`
- Add `Ercot.get_real_time_system_conditions()`
- Add `Ercot.get_unplanned_resource_outages()`
- Add `Ercot.get_highest_price_as_offer_selected()`
- Add `Ercot.get_as_reports()`
- Add `Ercot.get_hourly_resource_outage_capacity()`

#### `ERCOT.get_load_by_weather_zone`

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

#### `ERCOT.get_load_by_forecast_zone`

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
