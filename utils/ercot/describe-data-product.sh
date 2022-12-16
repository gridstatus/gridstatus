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
#     report_type_id: 12331
#

if [ -z "$1" ]; then
    echo "Usage: ${0} <data_product_id>" >&2
    exit 1
fi

set -e

URL='https://www.ercot.com/mp/data-products/data-product-details'
tmp=$(mktemp)
curl -f -s ${URL}'?id='$1 \
    | grep 'product =' \
    | sed -e 's/.*product = //g' -e 's,;</script.*,,g' \
    > $tmp \
    ;

jq . $tmp;
echo "report_type_id: $(jq -r .reportTypeId_i $tmp)"

rm -f $tmp
