#!/usr/bin/env bash
#
# Describes report given Report Type ID
#
# Example:
#
#    $ ./describe-report-type.sh 12331
#    {
#      "ListDocsByRptTypeRes": {
#        "DocumentList": [
#          {
#            "Document": {
#              "ExpiredDate": "2023-01-15T23:59:59-06:00",
#    <snip>
#

if [ -z "$1" ]; then
    echo "Usage: ${0} <report_type_id>" >&2
    exit 1
fi

set -e

curl -f -s 'https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId='$1 | jq .
