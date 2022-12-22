#!/usr/bin/env bash
#
# Describes data product given ID
#
# Example:
#
#     $ ./describe-data-product.sh NP4-190-CD
#     {
#       "misDisplayType_s": "AGE",
#       "internal-name": "DAM Settlement Point Prices",
#       "market_s": "Nodal",
#     <snip>
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
    set -o pipefail
    curl -f -s ${BASE_URL}'?id='${data_product_id} \
        | grep 'product =' \
        | sed -e 's/.*product = //g' -e 's,;</script.*,,g' \
        > $tmp \
        ;
    set +o pipefail

    jq . $tmp;
    rm -f $tmp
}

main $@
