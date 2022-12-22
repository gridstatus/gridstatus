# Utility Scripts for ERCOT

This directory contains utility scripts for exploring ERCOT Market
Information List (EMIL) data, and working with  [`gridstatus/ercot.py`](../../gridstatus/ercot.py).

## Quick Start

### Generate Constant

Generates the Report Type ID (RTID) constant to be used in [`gridstatus/ercot.py`](../../gridstatus/ercot.py).

Pass in the Data Product ID or URL, e.g. [https://www.ercot.com/mp/data-products/data-product-details?id=NP6-905-CD](https://www.ercot.com/mp/data-products/data-product-details?id=NP6-905-CD):

```bash
$ ./generate-rtid-constant.sh https://www.ercot.com/mp/data-products/data-product-details?id=NP6-905-CD
# or
$ ./generate-rtid-constant.sh NP6-905-CD
```
```bash
# Settlement Point Prices at Resource Nodes, Hubs and Load Zones
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-905-CD
SETTLEMENT_POINT_PRICES_AT_RESOURCE_NODES_HUBS_AND_LOAD_ZONES_RTID = 12301
```

Or pass in the Report Type ID:

```bash
$ ./generate-rtid-constant.sh 12301
```
```bash
SETTLEMENT_POINT_PRICES_AT_RESOURCE_NODES_HUBS_AND_LOAD_ZONES_RTID = 12301
```

## Data Exploration

## Describe Data Product

Show the metadata for a Data Product by ID or URL:

```bash
$ ./describe-data-product.sh NP6-905-CD
# or
$ ./describe-data-product.sh https://www.ercot.com/mp/data-products/data-product-details?id=NP6-905-CD
```
```javascript
{
  "misDisplayType_s": "AGE",
  "internal-name": "Settlement Point Prices at Resource Nodes, Hubs and Load Zones",
  "market_s": "Nodal",
  "lastUpdatedDate_dt_tz": "CST6CDT",
  "objectGroupId": "ef23",
  "misDisplayDuration_i": "5",
  "productDescription_s": "Settlement Point Price for each Settlement Point, produced from SCED LMPs every 15 minutes.",
  /* snip */
}
```

### Get Report Type ID

Get the Report Type ID from a given Data Product by ID or URL:

```bash
$ ./get-report-type-id.sh NP6-905-CD
# or
$ ./get-report-type-id.sh https://www.ercot.com/mp/data-products/data-product-details?id=NP6-905-CD
```
```bash
12301
```

### Describe Report Type

Get metadata for a given Report Type ID:

```bash
$ ./describe-report-type.sh 12301
```
```javascript
{
  "ListDocsByRptTypeRes": {
    "DocumentList": [
      {
        "Document": {
          "ExpiredDate": "2022-12-27T23:59:59-06:00",
          "ILMStatus": "EXT",
          "SecurityStatus": "P",
          "ContentSize": "6101",
          "Extension": "zip",
          "ReportTypeID": "12301",
/* snip */
        }
      }
    ]
  }
}
```
