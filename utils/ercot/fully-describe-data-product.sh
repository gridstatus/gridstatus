#!/usr/bin/env bash
#
# Full description of data products and underlying reports
#
# Example:
#
#    ./fully-describe-data-product.sh NP4-181-ER
#    # Historical DAM Clearing Prices for Capacity
#    # https://www.ercot.com/mp/data-products/data-product-details?id=NP4-181-ER
#    HISTORICAL_DAM_CLEARING_PRICES_FOR_CAPACITY_RTID = 13091
#
#    ./fully-describe-data-product.sh https://www.ercot.com/mp/data-products/data-product-details?id=NP3-560-CD
#    # Seven-Day Load Forecast by Forecast Zone
#    # https://www.ercot.com/mp/data-products/data-product-details?id=NP3-560-CD
#    SEVEN_DAY_LOAD_FORECAST_BY_FORECAST_ZONE_RTID = 12311
#

DOC_LIST_URL='https://www.ercot.com/misapp/servlets/IceDocListJsonWS'
DATA_PRODUCT_DETAILS_URL='https://www.ercot.com/mp/data-products/data-product-details'

if [ -z "$1" ]; then
    echo "Usage: ${0} <data_product_id>" >&2
    exit 1
fi

data_product_id="${1}"

if [ -n "$(echo ${data_product_id} | grep '?id=')" ]; then
    data_product_id="$(echo ${data_product_id} | sed -e 's/.*?id=//g')"
fi

set -e

full_data_product_url="${DATA_PRODUCT_DETAILS_URL}?id=${data_product_id}"
tmp=$(mktemp)
curl -f -s "${full_data_product_url}" \
    | grep 'product =' \
    | sed -e 's/.*product = //g' -e 's,;</script.*,,g' \
    > $tmp \
    ;

if [ ! -s "${tmp}" ]; then
    echo "Failed to fetch ${full_url}" >&2
    exit 1
fi

report_type_id="$(jq -r .reportTypeId_i $tmp)"
product_name="$(jq -r .productName_s $tmp)"

full_document_list_url="${DOC_LIST_URL}?reportTypeId=${report_type_id}"
name=$(curl -f -s "${full_document_list_url}" \
    | jq -r .ListDocsByRptTypeRes.DocumentList[0].Document.ReportName \
    | tr 'a-z' 'A-Z' \
    | sed -e 's/[ -]/_/g' -e 's/,//g' -e 's/$/_RTID/g'\
    ;)

if [ -z "${name}" ]; then
    echo "Failed to fetch ${full_document_list_url}" >&2
    exit 1
fi

echo "# ${product_name}"
echo "# ${full_data_product_url}"
echo "${name} = ${report_type_id}"

rm -f $tmp
