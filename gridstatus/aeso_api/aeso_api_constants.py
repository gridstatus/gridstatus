"""Constants for AESO API client."""

SUPPLY_DEMAND_COLUMN_MAPPING: dict[str, str] = {
    "total_max_generation_capability": "Total Max Generation Capability",
    "total_net_generation": "Total Net Generation",
    "net_to_grid_generation": "Net Actual Generation",
    "net_actual_interchange": "Net Actual Interchange",
    "alberta_internal_load": "Alberta Internal Load",
    "contingency_reserve_required": "Contingency Reserve Required",
    "dispatched_contigency_reserve_total": "Dispatched Contingency Reserve Total",
    "dispatched_contingency_reserve_gen": "Dispatched Contingency Reserve Gen",
    "dispatched_contingency_reserve_other": "Dispatched Contingency Reserve Other",
    "ffr_armed_dispatch": "FFR Armed Dispatch",
    "ffr_offered_volume": "FFR Offered Volume",
    "long_lead_time_volume": "Long Lead Time Volume",
}

FUEL_MIX_COLUMN_MAPPING: dict[str, str] = {
    "fuel_type": "Fuel Type",
    "aggregated_maximum_capability": "Maximum Capability",
    "aggregated_net_generation": "Net Generation",
    "aggregated_dispatched_contingency_reserve": "Dispatched Contingency Reserve",
}

INTERCHANGE_COLUMN_MAPPING: dict[str, str] = {
    "path": "Path",
    "actual_flow": "Actual Flow",
}

RESERVES_COLUMN_MAPPING: dict[str, str] = {
    "contingency_reserve_required": "Contingency Reserve Required",
    "dispatched_contigency_reserve_total": "Dispatched Contingency Reserve Total",
    "dispatched_contingency_reserve_gen": "Dispatched Contingency Reserve Gen",
    "dispatched_contingency_reserve_other": "Dispatched Contingency Reserve Other",
    "ffr_armed_dispatch": "FFR Armed Dispatch",
    "ffr_offered_volume": "FFR Offered Volume",
    "long_lead_time_volume": "Long Lead Time Volume",
}

ASSET_LIST_COLUMN_MAPPING: dict[str, str] = {
    "asset_ID": "Asset ID",
    "asset_name": "Asset Name",
    "asset_type": "Asset Type",
    "operating_status": "Operating Status",
    "pool_participant_ID": "Pool Participant ID",
    "pool_participant_name": "Pool Participant Name",
    "net_to_grid_asset_flag": "Net To Grid Asset Flag",
    "asset_incl_storage_flag": "Asset Include Storage Flag",
}

COLUMNS_TO_DROP: list[str] = [
    "last_updated_datetime_utc",
    "last_updated_datetime_mpt",
    "generation_data_list",
    "interchange_list",
]

ASSET_LIST_COLUMNS: list[str] = [
    "Time",
    "Asset ID",
    "Asset Name",
    "Asset Type",
    "Operating Status",
    "Pool Participant ID",
    "Pool Participant Name",
    "Net To Grid Asset Flag",
    "Asset Include Storage Flag",
]
