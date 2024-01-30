## ERCOT API

A module to enable access and usage of [ERCOT's API](https://apiexplorer.ercot.com/)

### Module Contents

*pubapi-apim-api.json* was downloaded from https://apiexplorer.ercot.com/api-details#api=pubapi-apim-api by selecting Open API 3 (JSON) from the "API Definition" dropdown. It contains metadata about all of the available endpoints in the ERCOT API.

*api_parser.py* contains utilities that unpack the data from pubapi-apim-api.json, making its contents accessible to this library.

*ercot_api.py* provides a method `hit_ercot_api` to conveniently call any endpoint of the ERCOT API. It also has a help CLI with two actions:
- `list` will list all available endpoints
- `describe` will describe the details of a single endpoint, given with `--endpoint`

List example:

     % python ercot_api.py list                                             
     /np3-233-cd/hourly_res_outage_cap
         Hourly Resource Outage Capacity
     /np3-565-cd/lf_by_model_weather_zone
         Seven-Day Load Forecast by Model and Weather Zone
     /np3-566-cd/lf_by_model_study_area
         Seven-Day Load Forecast by Model and Study Area
     ...
     /np6-86-cd/shdw_prices_bnd_trns_const
         SCED Shadow Prices and Binding Transmission Constraints
     /np6-905-cd/spp_node_zone_hub
         Settlement Point Prices at Resource Nodes, Hubs and Load Zones
     /np6-970-cd/rtd_lmp_node_zone_hub
         RTD Indicative LMPs by Resource Nodes, Load Zones and Hubs

Describe example:

     % python ercot_api.py describe --endpoint /np6-346-cd/act_sys_load_by_fzn
     Endpoint: /np6-346-cd/act_sys_load_by_fzn
     Summary:  Actual System Load by Forecast Zone
     Parameters:
         DSTFlag - boolean
         hourEnding - timestamp
         houstonFrom - float
         houstonTo - float
         northFrom - float
         northTo - float
         operatingDayFrom - timestamp
         operatingDayTo - timestamp
         southFrom - float
         southTo - float
         totalFrom - float
         totalTo - float
         westFrom - float
         westTo - float
