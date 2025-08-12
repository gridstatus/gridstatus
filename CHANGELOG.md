# Changelog

## v0.31.0 - August 12, 2025

### What's Changed
* Reduce Error Logging from CAISO in [#583](https://github.com/gridstatus/gridstatus/pull/583)
* Update ERCOT Fuel Mix Detailed in [#584](https://github.com/gridstatus/gridstatus/pull/584)
* IESO New Price Data in [#582](https://github.com/gridstatus/gridstatus/pull/582)
* Explicit columns for IESO Intertie Actuals in [#586](https://github.com/gridstatus/gridstatus/pull/586)
* IESO Transmission Outages in [#585](https://github.com/gridstatus/gridstatus/pull/585)
* IESO Zonal Load in [#588](https://github.com/gridstatus/gridstatus/pull/588)
* IESO Price Location Fix in [#589](https://github.com/gridstatus/gridstatus/pull/589)
* IESO Price Location Fix Again in [#591](https://github.com/gridstatus/gridstatus/pull/591)
* IESO Transmission Limits in [#587](https://github.com/gridstatus/gridstatus/pull/587)
* IESO Market Launch URL in [#592](https://github.com/gridstatus/gridstatus/pull/592)
* IESO Real Time Totals in [#593](https://github.com/gridstatus/gridstatus/pull/593)
* Fix IESO Transmission Outages URLs in [#594](https://github.com/gridstatus/gridstatus/pull/594)
* Adequacy report update in [#590](https://github.com/gridstatus/gridstatus/pull/590)
* IESO Predispatch Prices in [#596](https://github.com/gridstatus/gridstatus/pull/596)
* IESO Renewable forecasts in [#595](https://github.com/gridstatus/gridstatus/pull/595)
* remove date filter in [#599](https://github.com/gridstatus/gridstatus/pull/599)
* IESO LMP Operating Reserves in [#598](https://github.com/gridstatus/gridstatus/pull/598)
* IESO Constraint Shadow Prices in [#597](https://github.com/gridstatus/gridstatus/pull/597)
* IESO Intertie Schedule and Flows Updated in [#600](https://github.com/gridstatus/gridstatus/pull/600)
* IESO Shadow Prices Day Ahead Hourly in [#602](https://github.com/gridstatus/gridstatus/pull/602)
* Fix real time codepath in [#603](https://github.com/gridstatus/gridstatus/pull/603)
* PJM Load Forecast 5 Min in [#601](https://github.com/gridstatus/gridstatus/pull/601)
* ERCOT DAM Total Energy Purchased and Sold in [#604](https://github.com/gridstatus/gridstatus/pull/604)
* Clean up IESO Tests and Methods in [#605](https://github.com/gridstatus/gridstatus/pull/605)
* ERCOT COP Adjustment Period Snapshot in [#606](https://github.com/gridstatus/gridstatus/pull/606)
* PJM 5 minute tie flows in [#607](https://github.com/gridstatus/gridstatus/pull/607)
* ERCOT COP Adjustment Period Data Fixes in [#608](https://github.com/gridstatus/gridstatus/pull/608)
* MISO look ahead hourly in mw in [#610](https://github.com/gridstatus/gridstatus/pull/610)
* Handle Duplicates in ERCOT DAM 60 Day Data in [#609](https://github.com/gridstatus/gridstatus/pull/609)
* ERCOT System Level ESR in [#613](https://github.com/gridstatus/gridstatus/pull/613)
* Add nomogram/branch shadow prices in [#611](https://github.com/gridstatus/gridstatus/pull/611)
* ISONE Load Forecast API in [#612](https://github.com/gridstatus/gridstatus/pull/612)
* AESO API Init in [#615](https://github.com/gridstatus/gridstatus/pull/615)
* make `AESO()` available at top level in [#617](https://github.com/gridstatus/gridstatus/pull/617)
* AESO Pool Price, Forecast Pool Price, and System Marginal Price in [#618](https://github.com/gridstatus/gridstatus/pull/618)
* Some tweaks to AESO interchange and reserves columns in [#620](https://github.com/gridstatus/gridstatus/pull/620)
* MISO Interchange 5 Min in [#619](https://github.com/gridstatus/gridstatus/pull/619)
* Add in 30 day rolling average in pool price in [#621](https://github.com/gridstatus/gridstatus/pull/621)
* AESO load and load forecast in [#622](https://github.com/gridstatus/gridstatus/pull/622)
* AESO Unit Status in [#624](https://github.com/gridstatus/gridstatus/pull/624)
* AESO System Marginal Price timeseries in [#623](https://github.com/gridstatus/gridstatus/pull/623)
* AESO Generator Outages in [#625](https://github.com/gridstatus/gridstatus/pull/625)
* MISO Interchange Hourly in [#626](https://github.com/gridstatus/gridstatus/pull/626)
* Add CAISO Wind and Solar RTD, RTPD Forecasts and Actuals in [#627](https://github.com/gridstatus/gridstatus/pull/627)
* NYISO BTM Solar Forecast Publish Time in [#630](https://github.com/gridstatus/gridstatus/pull/630)
* AESO Tx Outages in [#628](https://github.com/gridstatus/gridstatus/pull/628)
* AESO Renewables Forecasts in [#631](https://github.com/gridstatus/gridstatus/pull/631)
* CAISO Curtailment Report Update in [#629](https://github.com/gridstatus/gridstatus/pull/629)
* Fix `make docs` warnings in [#614](https://github.com/gridstatus/gridstatus/pull/614)
* Hardcode SPP List of Hubs and Interfaces in [#632](https://github.com/gridstatus/gridstatus/pull/632)
* IESO Predispatch in [#634](https://github.com/gridstatus/gridstatus/pull/634)
* AESO Daily Average Price in [#635](https://github.com/gridstatus/gridstatus/pull/635)
* Separate AESO generation actuals from forecasts in [#636](https://github.com/gridstatus/gridstatus/pull/636)
* Fix missing NYISO ICAP market report for December 2023 in [#633](https://github.com/gridstatus/gridstatus/pull/633)
* PJM Instantaneous Dispatch Rates in [#637](https://github.com/gridstatus/gridstatus/pull/637)
* IESO Safe Parsing to Fix Missing Values in [#638](https://github.com/gridstatus/gridstatus/pull/638)
* Fix CAISO Curtailment for July 2025 in [#640](https://github.com/gridstatus/gridstatus/pull/640)
* Add PJM Transfer Limits, Tie Flows, Scheduled Tie Flows, and more in [#639](https://github.com/gridstatus/gridstatus/pull/639)
* Fix PJM Regulation Market Monthly Column in [#642](https://github.com/gridstatus/gridstatus/pull/642)
* PJM List Datasets in [#641](https://github.com/gridstatus/gridstatus/pull/641)
* some PJM dataset date handling in [#644](https://github.com/gridstatus/gridstatus/pull/644)
* Add back peak time in [#645](https://github.com/gridstatus/gridstatus/pull/645)
* PJM list dataset column ordering in [#646](https://github.com/gridstatus/gridstatus/pull/646)
* Fix CAISO Curtailment Parsing Again in [#643](https://github.com/gridstatus/gridstatus/pull/643)
* add blank header to MISO IxQ `request` in [#648](https://github.com/gridstatus/gridstatus/pull/648)
* Update utils.py in [#651](https://github.com/gridstatus/gridstatus/pull/651)


**Full Changelog**: https://github.com/gridstatus/gridstatus/compare/v0.30.1...v0.31.0

## v0.30.1 - April 22, 2025

### What's Changed

* Fix PyPI Publishing in [#581](https://github.com/gridstatus/gridstatus/pull/581)

## v0.30.0 - April 18, 2025

### What's Changed

* Add py.typed File in [#531](https://github.com/gridstatus/gridstatus/pull/531)
* Add NYISO interconnection queue cluster projects in [#537](https://github.com/gridstatus/gridstatus/pull/537)
* ERCOT 60 Day DAM Disclosure Additions in [#533](https://github.com/gridstatus/gridstatus/pull/533)
* Fix ERCOT 60 Day DAM Columns in [#538](https://github.com/gridstatus/gridstatus/pull/538)
* ISONE Interchange Data in [#540](https://github.com/gridstatus/gridstatus/pull/540)
* CAISO GHG component of LMP in [#529](https://github.com/gridstatus/gridstatus/pull/529)
* ERCOT Solar and Wind Actual and Forecast Hourly Reports Update in [#541](https://github.com/gridstatus/gridstatus/pull/541)
* PJM Area Control Error in [#542](https://github.com/gridstatus/gridstatus/pull/542)
* NYISO Interface Flows and Lake Erie Circulation in [#539](https://github.com/gridstatus/gridstatus/pull/539)
* IESO MCP and HOEP in [#543](https://github.com/gridstatus/gridstatus/pull/543)
* ERCOT API SCED 60 Day Disclosure in [#545](https://github.com/gridstatus/gridstatus/pull/545)
* MISO Look Ahead outages in [#546](https://github.com/gridstatus/gridstatus/pull/546)
* EIA Generators in [#544](https://github.com/gridstatus/gridstatus/pull/544)
* ISONE Real Time Hourly LMPs in [#547](https://github.com/gridstatus/gridstatus/pull/547)
* Ercot Fuel Mix Detailed in [#550](https://github.com/gridstatus/gridstatus/pull/550)
* EIA Generators Fix Data Types in [#549](https://github.com/gridstatus/gridstatus/pull/549)
* ISONE 5 Min LMPs (via API) in [#551](https://github.com/gridstatus/gridstatus/pull/551)
* ERCOT AS Reports Bid Curve Column Type Update in [#552](https://github.com/gridstatus/gridstatus/pull/552)
* PJM dispatch reserves in [#553](https://github.com/gridstatus/gridstatus/pull/553)
* Keep reserve type in [#554](https://github.com/gridstatus/gridstatus/pull/554)
* Remove missing column in SPP interconnection queue in [#556](https://github.com/gridstatus/gridstatus/pull/556)
* CAISO Scheduling Point / Tie Combo LMPs in [#555](https://github.com/gridstatus/gridstatus/pull/555)
* CAISO Hasp LMP in [#557](https://github.com/gridstatus/gridstatus/pull/557)
* NYISO AS Prices in [#558](https://github.com/gridstatus/gridstatus/pull/558)
* EIA Handle More Fuel Mix Types in [#559](https://github.com/gridstatus/gridstatus/pull/559)
* CAISO 15 Min Tie Flows and Renewable Forecast in [#561](https://github.com/gridstatus/gridstatus/pull/561)
* Consistent SCED Timestamp Column Naming in [#560](https://github.com/gridstatus/gridstatus/pull/560)
* ERCOT SCED Timestamp Column Rename Fix in [#562](https://github.com/gridstatus/gridstatus/pull/562)
* Replace tabula (java) dependency in [#563](https://github.com/gridstatus/gridstatus/pull/563)
* Specify Format for PJM Datetimes in [#565](https://github.com/gridstatus/gridstatus/pull/565)
* MISO Load Zonal Hourly in [#567](https://github.com/gridstatus/gridstatus/pull/567)
* MISO Zonal Load Numeric Cols in [#568](https://github.com/gridstatus/gridstatus/pull/568)
* Security Updates April 2025 in [#566](https://github.com/gridstatus/gridstatus/pull/566)
* Historical data for MISO Zonal Load in [#569](https://github.com/gridstatus/gridstatus/pull/569)
* PJM Regulation Market in [#564](https://github.com/gridstatus/gridstatus/pull/564)
* PJM Round before Pivot in [#570](https://github.com/gridstatus/gridstatus/pull/570)
* ISONE Capacity 7 Day in [#572](https://github.com/gridstatus/gridstatus/pull/572)
* IESO Resource Adequacy Retry Logic in [#573](https://github.com/gridstatus/gridstatus/pull/573)
* IESO HOEP Real Time Fix Duplicates in [#576](https://github.com/gridstatus/gridstatus/pull/576)
* PJM LMP Real Time Hourly Unverified in [#575](https://github.com/gridstatus/gridstatus/pull/575)
* Revert "IESO HOEP Real Time Fix Duplicates" but Keep Test Updates in [#578](https://github.com/gridstatus/gridstatus/pull/578)
* Use params for caiso load forecast in [#577](https://github.com/gridstatus/gridstatus/pull/577)
* IESO Forecast Surplus Baseload in [#579](https://github.com/gridstatus/gridstatus/pull/579)
* IESO Intertie Actual Schedule Flow Hourly in [#580](https://github.com/gridstatus/gridstatus/pull/580)



## v0.29.1 - January 26, 2025

* PJM Hourly Demand Bid Data [#527](https://github.com/gridstatus/gridstatus/pull/527)
* feat(ENG 1120): Expand CAISO Load Forecast [#522](https://github.com/gridstatus/gridstatus/pull/522)
* Update Docs and README [#526](https://github.com/gridstatus/gridstatus/pull/525)
* Add CITATION.CFF and Script to Bump Version [#526](https://github.com/gridstatus/gridstatus/pull/526)
* Add read_csv Kwargs for ERCOT read_doc [#528](https://github.com/gridstatus/gridstatus/pull/528)
* upgrade lxml to 5.3.0 [#530](https://github.com/gridstatus/gridstatus/pull/530)
* Query current day data from real-time hourly market in ISO-NE [#532](https://github.com/gridstatus/gridstatus/pull/532)


## v0.29.0 - January 15, 2025

### Additions (New Features/Datasets)
* Add 5 Minute PJM Solar and Wind Forecast in [#446](https://github.com/gridstatus/gridstatus/pull/446)
* Add PJM IT SCED LMP 5 Minute in [#450](https://github.com/gridstatus/gridstatus/pull/450)
* Add ISO New England API Integration and Initial Datasets in [#452](https://github.com/gridstatus/gridstatus/pull/452)
* MISO Outages Forecast and Actuals Estimated in [#457](https://github.com/gridstatus/gridstatus/pull/457)
* ISONE Load Forecasts in [#460](https://github.com/gridstatus/gridstatus/pull/460)
* ERCOT DAM 60d AS Offers Data in [#464](https://github.com/gridstatus/gridstatus/pull/464)
* PJM Constraints Datasets in [#472](https://github.com/gridstatus/gridstatus/pull/472)
* MISO Constraints in [#476](https://github.com/gridstatus/gridstatus/pull/476)
* ISONE Hourly System Load in [#490](https://github.com/gridstatus/gridstatus/pull/490)
* IESO Resource Adequacy Report in [#482](https://github.com/gridstatus/gridstatus/pull/482)
* MISO API Pricing Data in [#493](https://github.com/gridstatus/gridstatus/pull/493)
* ERCOT Indicative LMPs in [#504](https://github.com/gridstatus/gridstatus/pull/504)
* PJM Settlements Verified LMPS in [#509](https://github.com/gridstatus/gridstatus/pull/509)
* PJM Settlements Verified Hourly LMPs in [#514](https://github.com/gridstatus/gridstatus/pull/514)
* NYISO Zonal Load Forecast in [#519](https://github.com/gridstatus/gridstatus/pull/519)

### Fixes
* Fix ERCOT API Hourly Solar and Wind Tests in [#449](https://github.com/gridstatus/gridstatus/pull/449)
* Resolve undated times correctly and bust cloudfront cache in [#451](https://github.com/gridstatus/gridstatus/pull/451)
* Fix ISONE API methods in [#454](https://github.com/gridstatus/gridstatus/pull/454)
* MISO Outages Fix for Missing Columns in [#459](https://github.com/gridstatus/gridstatus/pull/459)
* Add small fix to ERCOT temp method in [#463](https://github.com/gridstatus/gridstatus/pull/463)
* * Update to all IESO public report links in [#465](https://github.com/gridstatus/gridstatus/pull/465)
* ERCOT DAM LMP By Bus DST Fix in [#467](https://github.com/gridstatus/gridstatus/pull/467)
* DST Fix for energy_storage_resources in [#469](https://github.com/gridstatus/gridstatus/pull/469)
* ERCOT Fix Real Time AS Monitor, Real Time System Conditions, and Forecasts Publish Dates DST Issue in [#468](https://github.com/gridstatus/gridstatus/pull/468)
* ISO NE fuel mix 2024 fallback transition time fix in [#471](https://github.com/gridstatus/gridstatus/pull/471)
* SPP 5 Minute Dataset DST End Fixes in [#470](https://github.com/gridstatus/gridstatus/pull/470)
* Fix ERCOT AS Reports for DST in [#473](https://github.com/gridstatus/gridstatus/pull/473)
* Fix for ERCOT DAM AS Offers with Repeated Offers in [#474](https://github.com/gridstatus/gridstatus/pull/474)
* EIA Fix Grid Monitor (CO2 Emissions) in [#483](https://github.com/gridstatus/gridstatus/pull/483)
* Fix Tests for is_today and is_yesterday in [#489](https://github.com/gridstatus/gridstatus/pull/489)
* adequacy report columns in [#499](https://github.com/gridstatus/gridstatus/pull/499)
* Disambiguate `last_modified` timezone better in [#502](https://github.com/gridstatus/gridstatus/pull/502)
* * Update url base for caiso outlook in [#503](https://github.com/gridstatus/gridstatus/pull/503)
* Fix ERCOT Tests in [#507](https://github.com/gridstatus/gridstatus/pull/507)
* IESO Adequacy Forecast Report fixes in [#512](https://github.com/gridstatus/gridstatus/pull/512)
* * Update NYISO data sources in [#517](https://github.com/gridstatus/gridstatus/pull/517)
* Fix CAISO Generator Outages in [#520](https://github.com/gridstatus/gridstatus/pull/520)

### General Updates/Codebase Improvements

* Change Processing of Bid Curve to Array of Arrays in [#453](https://github.com/gridstatus/gridstatus/pull/453)
* Add mypy to work toward type safety in [#456](https://github.com/gridstatus/gridstatus/pull/456)
* ruff format over black in [#455](https://github.com/gridstatus/gridstatus/pull/455)
* Rename to Generation Outages MISO in [#461](https://github.com/gridstatus/gridstatus/pull/461)
* Raise NoDataFoundException When ERCOT Documents are Not Found in [#462](https://github.com/gridstatus/gridstatus/pull/462)
* Bump the pip group across 1 directory with 3 updates in [#458](https://github.com/gridstatus/gridstatus/pull/458)
* Update EIA Fuel Mix for New Power Storage Columns in [#475](https://github.com/gridstatus/gridstatus/pull/475)
* Remove ErcotAPI Dependency on GitHub File in [#477](https://github.com/gridstatus/gridstatus/pull/477)
* tune testing config in [#478](https://github.com/gridstatus/gridstatus/pull/478)
* Rename MISO LMP Weekly to Real Time 5 Min Final in [#479](https://github.com/gridstatus/gridstatus/pull/479)
* EIA Regional Data Keep NAs in [#485](https://github.com/gridstatus/gridstatus/pull/485)
* ERCOT Updates for Request Kwargs in [#484](https://github.com/gridstatus/gridstatus/pull/484)
* Ignore fixtures in [#491](https://github.com/gridstatus/gridstatus/pull/491)
* IESO Public Certificates in [#496](https://github.com/gridstatus/gridstatus/pull/496)
* Rename Columns for PJM Load Forecast Hourly in [#498](https://github.com/gridstatus/gridstatus/pull/498)
* rename to `load forecast` in [#501](https://github.com/gridstatus/gridstatus/pull/501)
* VCR Filter Headers in [#492](https://github.com/gridstatus/gridstatus/pull/492)
* VCR Setup (Part 1) in [#497](https://github.com/gridstatus/gridstatus/pull/497)
* Remove date parameter in [#510](https://github.com/gridstatus/gridstatus/pull/510)
* Change Bulk Download Default to True in [#508](https://github.com/gridstatus/gridstatus/pull/508)
* Typehints for EMIL for ERCOT Capacity Outages in [#513](https://github.com/gridstatus/gridstatus/pull/513)
* Update EIA Fuel Mix for Geothermal and Other Energy Storage in [#518](https://github.com/gridstatus/gridstatus/pull/518)
* Support Multiple API Keys for MISO API in [#516](https://github.com/gridstatus/gridstatus/pull/516)

## v0.28.0 - October 3, 2024

### Breaking Changes

- PJM requires an `api_key` on initialization (can be set as `PJM_API_KEY` environment variable)

### Non-Breaking Changes

- Added more methods to the `ErcotAPI` class which uses the new [Ercot API](https://data.ercot.com/) for fetching data
  - Eventually, the `ErcotAPI` will be the primary way to fetch data from ERCOT, but for now, we still need the `Ercot` class because the new API doesn't support all datasets.
- Add `pjm.get_gen_outages_by_type` to get generation outage data
- Flips the congestion sign on NYISO to be consistent with other ISOs. In the NYISO raw data, a negative congestion value means a higher LMP, which is the opposite of other ISOs. We flip the sign so that a negative congestion value means a lower LMP as it does in other ISOs.
- Adds ERCOT unplanned system outages (`ERCOT().get_unplanned_system_outages`)

## v0.27.0 - Mar 4, 2024

### Breaking Changes

- Dropped support for pandas < 2

## v0.26.0 - Feb 27, 2024

- Last release supporting pandas <2
- Add ERCOT DAM System Lambda (`ercot.get_dam_system_lambda`)
- Add ISONE solar and wind forecasts (`isone.get_solar_forecast` and `isone.get_wind_forecast`)

## v0.25.0 - Feb 20, 2024

### Development

- Added support for using poetry for dependency management for local development
- Dropped support for Python 3.8

### SPP

- Add `spp.get_solar_and_wind_forecast_short_term` and `spp.get_solar_and_wind_forecast_mid_term` for solar and wind forecasts
- Add `spp.get_load_forecast_short_term` and `spp.get_load_forecast_long_term` for load forecasts
  - This overlaps with the existing `spp.get_load_forecast` method, which we want to eventually remove in favor of these two methods.
- Add support for operating reserves

### EIA

- Add support to specify facets in get_dataset

### ERCOT

- Added initial support for using the ERCOT API


## v0.24.0 - Dec 27, 2023

### ERCOT

- Add `ercot.get_energy_storage_resources`
- Add support for RTM and DAM price correction datasets
- Add System Lambda
- Add support for RTM and DAM price correction datasets
- Add support for electrical bus and settlement point LMPS

## SPP

- Add support for generation capacity on outage
- Add support for SPP WEIS Real Time LMP
- Add "Status (Original)" column to interconnection queue data

### EIA

- Add `EIA.get_grid_monitor` dataset with hourly BA and Region emission data

### CAISO

- Improve CAISO curtailed non-operational generator report

### ISONE

- Parse ISONE interconnection queue project status columns

### PJM

- Update PJM `get_lmp` to return columns `Location Id`, `Location Name`, `Location Short Name` to avoid ambiguity.

### Breaking Changes

- `PJM.get_lmp` no longer return `Location`. That value is now `Location Id`.

### Bug Fixes

- Assorted DST handling fixes
- Ensure `sleep` parameter is handled correctly on all CAISO methods

## v0.23.0 - Sept 12, 2023

### ERCOT

- Support ECRS in ERCOT 60 Day DAM and SCED Reports
- Handle Energy Weighted Load Zone prices in real time SPPs
- Add ERCOT hourly wind forecast report
- Add ERCOT hourly solar forecast report
- Add `Ercot.get_60_day_sced_disclosure`
- Add `Ercot.get_60_day_dam_disclosure`
- Add support for specifying `forecast_type` to `Ercot.get_load_forecast`
- Add ERCOT System Lambda

### MISO

- Add support for historical DAM LMP

### EIA

- Add wholesale petroleum and natural gas daily spot prices.
- Add weekly spot prices and export totals for coal.
- Add handler for hourly fuel type data

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
