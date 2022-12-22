#!/usr/bin/env bash
#
# Generate constant and comments for data products and underlying report type id
#
# Example:
#
#    ./generate-rtid-constant.sh NP4-181-ER
#    # Historical DAM Clearing Prices for Capacity
#    # https://www.ercot.com/mp/data-products/data-product-details?id=NP4-181-ER
#    HISTORICAL_DAM_CLEARING_PRICES_FOR_CAPACITY_RTID = 13091
#
#    ./generate-rtid-constant.sh https://www.ercot.com/mp/data-products/data-product-details?id=NP3-560-CD
#    # Seven-Day Load Forecast by Forecast Zone
#    # https://www.ercot.com/mp/data-products/data-product-details?id=NP3-560-CD
#    SEVEN_DAY_LOAD_FORECAST_BY_FORECAST_ZONE_RTID = 12311
#

DOC_LIST_URL='https://www.ercot.com/misapp/servlets/IceDocListJsonWS'
DATA_PRODUCT_DETAILS_URL='https://www.ercot.com/mp/data-products/data-product-details'

usage() {
    echo "Usage: ${0} <data_product_id|data_product_url|report_type_id>" >&2
}

main() {
    arg="${1}"
    if [ -z "${arg}" ]; then
        usage
        exit 1
    fi

    arg_id="$(echo ${arg} | sed -e 's/^.*details\?id=//g')"
    data_product_id="$(echo "${arg_id}" | grep -E '^[A-Z0-9]{1,}-[A-Z0-9\-]{1,}$')"
    report_type_id="$(echo "${arg_id}" | grep -E '^[A-Z0-9]{1,}$')"

    if [ -n "${data_product_id}" ]; then
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

        product_name="$(jq -r .productName_s $tmp)"
        echo "# ${product_name}"
        echo "# ${full_data_product_url}"

        report_type_id="$(jq -r .reportTypeId_i $tmp)"
    fi

    if [ -n "${report_type_id}" ]; then
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

        echo "${name} = ${report_type_id}"
    fi

    rm -f $tmp
}

main $@
