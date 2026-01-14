## ERCOT API

A module to enable access and usage of [ERCOT's API](https://apiexplorer.ercot.com/)

### Module Contents

*pubapi-apim-api.json* was downloaded from https://apiexplorer.ercot.com/api-details#api=pubapi-apim-api by selecting Open API 3 (JSON) from the "API Definition" dropdown. It contains metadata about all of the available endpoints in the ERCOT API.

*api_parser.py* contains utilities that unpack the data from pubapi-apim-api.json, making its contents accessible to this library.

*ercot_api.py* provides a method `hit_ercot_api` to conveniently call any endpoint of the ERCOT API. It also has a CLI with two actions:
- `list` will list all available endpoints
- `describe` will describe the details of a single endpoint, given with `--endpoint`

Run from the project root:

```bash
# List all endpoints
uv run python gridstatus/ercot_api/ercot_api.py list

# Describe a specific endpoint
uv run python gridstatus/ercot_api/ercot_api.py describe --endpoint /np6-346-cd/act_sys_load_by_fzn
```
