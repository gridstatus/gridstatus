## ERCOT API

A module to enable access and usage of [ERCOT's API](https://apiexplorer.ercot.com/)

### Module Contents

*pubapi-apim-api.json* was downloaded from https://apiexplorer.ercot.com/api-details#api=pubapi-apim-api by selecting Open API 3 (JSON) from the "API Definition" dropdown. It contains metadata about all of the available endpoints in the ERCOT API.

*api_parser.py* contains utilities that unpack the data from pubapi-apim-api.json, making its contents accessible to this library.

*ercot_api.py* provides methods and a CLI to conveniently call the endpoints of the ERCOT API.