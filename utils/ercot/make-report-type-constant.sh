#!/usr/bin/env bash
#
# Produces constant name given Report Type ID, derived from
# Document.ReportName
#
# Example:
#
#     $ ./get-report-type-constant.sh 12331
#     DAM_SETTLEMENT_POINT_PRICES_RTID = 12331
#

if [ -z "$1" ]; then
    echo "Usage: ${0} <report_type_id>" >&2
    exit 1
fi

set -e

name=$(curl -f -s 'https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId='$1 \
    | jq -r .ListDocsByRptTypeRes.DocumentList[0].Document.ReportName \
    | tr 'a-z' 'A-Z' \
    | sed -e 's/[ -]/_/g' -e 's/,//g' -e 's/$/_RTID/g'\
       ;)
echo "${name} = ${1}"
