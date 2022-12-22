#!/usr/bin/env bash
#
# Get Report Type ID from Data Product
#
# Example:
#
#     $ ./get-report-type-id.sh NP4-190-CD
#     report_type_id: 12331
#

usage() {
    echo "Usage: ${0} <data_product_id|data_product_url>" >&2
}

main() {
    data_product_id="${1}"

    if [ -z "${data_product_id}" ]; then
        usage
        exit 1
    fi

    # clean up if URL in the form of https://www.ercot.com/mp/data-products/data-product-details?id=X
    data_product_id="$(echo ${data_product_id} | sed -e 's/^.*details\?id=//g')"

    set -e

    BASE_URL='https://www.ercot.com/mp/data-products/data-product-details'
    tmp=$(mktemp)
    curl -f -s ${BASE_URL}'?id='${data_product_id} \
        | grep 'product =' \
        | sed -e 's/.*product = //g' -e 's,;</script.*,,g' \
        > $tmp \
        ;

    if [ ! -s "${tmp}" ]; then
        echo "ERROR: Data Product '${data_product_id}' not found" >&2
        rm -f $tmp
        exit 1
    else
        jq -r .reportTypeId_i $tmp;
        rm -f $tmp
    fi
}
main $@
