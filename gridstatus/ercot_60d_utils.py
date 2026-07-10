from enum import StrEnum

import numpy as np
import pandas as pd
import polars as pl

from gridstatus.gs_logging import setup_gs_logger


class CurveOutputFormat(StrEnum):
    """Output format for extracted offer curves.

    LIST: Returns Python list-of-lists per cell (default).
    PG_ARRAY_AS_STRING: Returns PostgreSQL array strings like '{{mw,price},{mw,price}}'
        directly, using ~3x less peak memory.
    """

    LIST = "list"
    PG_ARRAY_AS_STRING = "pg_array_as_string"


logger = setup_gs_logger()

DAM_GEN_RESOURCE_KEY = "dam_gen_resource"
DAM_LOAD_RESOURCE_KEY = "dam_load_resource"
DAM_GEN_RESOURCE_AS_OFFERS_KEY = "dam_gen_resource_as_offers"
DAM_LOAD_RESOURCE_AS_OFFERS_KEY = "dam_load_resource_as_offers"
DAM_ENERGY_ONLY_OFFER_AWARDS_KEY = "dam_energy_only_offer_awards"
DAM_ENERGY_ONLY_OFFERS_KEY = "dam_energy_only_offers"
DAM_PTP_OBLIGATION_BID_AWARDS_KEY = "dam_ptp_obligation_bid_awards"
DAM_PTP_OBLIGATION_BIDS_KEY = "dam_ptp_obligation_bids"
DAM_ENERGY_BID_AWARDS_KEY = "dam_energy_bid_awards"
DAM_ENERGY_BIDS_KEY = "dam_energy_bids"
DAM_PTP_OBLIGATION_OPTION_KEY = "dam_ptp_obligation_option"
DAM_PTP_OBLIGATION_OPTION_AWARDS_KEY = "dam_ptp_obligation_option_awards"
DAM_ESR_KEY = "dam_esr"
DAM_ESR_AS_OFFERS_KEY = "dam_esr_as_offers"
DAM_AS_ONLY_AWARDS_KEY = "dam_as_only_awards"
DAM_AS_ONLY_OFFERS_KEY = "dam_as_only_offers"

SCED_LOAD_RESOURCE_KEY = "sced_load_resource"
SCED_GEN_RESOURCE_KEY = "sced_gen_resource"
SCED_ESR_KEY = "sced_esr"
SCED_SMNE_KEY = "sced_smne"
SCED_AS_OFFER_UPDATES_IN_OP_HOUR_KEY = "sced_as_offer_updates_in_op_hour"
SCED_RESOURCE_AS_OFFERS_KEY = "sced_resource_as_offers"


# Same for both generation and load
DAM_RESOURCE_AS_OFFERS_COLUMNS = [
    "Interval Start",
    "Interval End",
    "QSE",
    "DME",
    "Resource Name",
    "Multi-Hour Block Flag",
    "Block Indicators",
    "RRSPFR Offer Curve",
    "RRSFFR Offer Curve",
    "RRSUFR Offer Curve",
    "ECRS Offer Curve",
    "OFFEC Offer Curve",
    "ONLINE NONSPIN Offer Curve",
    "REGUP Offer Curve",
    "REGDOWN Offer Curve",
    "OFFLINE NONSPIN Offer Curve",
]

DAM_GEN_RESOURCE_COLUMNS = [
    "Interval Start",
    "Interval End",
    "QSE",
    "DME",
    "Resource Name",
    "Resource Type",
    "Settlement Point Name",
    "Resource Status",
    "HSL",
    "LSL",
    "Start Up Hot",
    "Start Up Inter",
    "Start Up Cold",
    "Min Gen Cost",
    "Awarded Quantity",
    "Energy Settlement Point Price",
    "RegUp Awarded",
    "RegUp MCPC",
    "RegDown Awarded",
    "RegDown MCPC",
    "RRSPFR Awarded",
    "RRSFFR Awarded",
    "RRSUFR Awarded",
    "RRS MCPC",
    "ECRSSD Awarded",
    "ECRS MCPC",
    "NonSpin Awarded",
    "NonSpin MCPC",
    "QSE submitted Curve",
]

DAM_LOAD_RESOURCE_COLUMNS = [
    "Time",
    "Interval Start",
    "Interval End",
    "Resource Name",
    "Max Power Consumption for Load Resource",
    "Low Power Consumption for Load Resource",
    "RegUp Awarded",
    "RegUp MCPC",
    "RegDown Awarded",
    "RegDown MCPC",
    "RRSPFR Awarded",
    "RRSFFR Awarded",
    "RRSUFR Awarded",
    "RRS MCPC",
    "ECRSSD Awarded",
    "ECRSMD Awarded",
    "ECRS MCPC",
    "NonSpin Awarded",
    "NonSpin MCPC",
]


DAM_ENERGY_ONLY_OFFER_AWARDS_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Settlement Point Name",
    "QSE",
    "Offer ID",
    "Energy Only Offer Award in MW",
    "Settlement Point Price",
]

DAM_ENERGY_ONLY_OFFERS_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Settlement Point Name",
    "QSE",
    "Energy Only Offer ID",
    "Energy Only Offer Curve",
    "Multi-Hour Block Indicator",
    "Block or Curve indicator",
]

DAM_PTP_OBLIGATION_BID_AWARDS_COLUMNS = [
    "Interval Start",
    "Interval End",
    "QSE",
    "Settlement Point Source",
    "Settlement Point Sink",
    "Bid ID",
    "PtP Bid Award - MW",
    "PtP Bid - Price",
]

DAM_PTP_OBLIGATION_BIDS_COLUMNS = [
    "Interval Start",
    "Interval End",
    "QSE",
    "Settlement Point Source",
    "Settlement Point Sink",
    "Bid ID",
    "PtP Bid - MW",
    "PtP Bid - Price",
    "Multi-Hour Block Indicator",
]

DAM_ENERGY_BID_AWARDS_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Settlement Point Name",
    "QSE",
    "Bid ID",
    "Energy Only Bid Award in MW",
    "Settlement Point Price",
]

DAM_ENERGY_BIDS_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Settlement Point Name",
    "QSE",
    "Energy Only Bid ID",
    "Energy Only Bid Curve",
    "Multi-Hour Block Indicator",
    "Block or Curve indicator",
]

DAM_PTP_OBLIGATION_OPTION_COLUMNS = [
    "Interval Start",
    "Interval End",
    "QSE",
    "Settlement Point Source",
    "Settlement Point Sink",
    "Offer ID",
    "CRR ID",
    "MW",
    "Price",
    "Multi-Hour Block Indicator",
]

# All except Multi-Hour Block Indicator
DAM_PTP_OBLIGATION_OPTION_AWARDS_COLUMNS = DAM_PTP_OBLIGATION_OPTION_COLUMNS[:-1]

DAM_ESR_COLUMNS = [
    "Interval Start",
    "Interval End",
    "QSE",
    "DME",
    "Resource Name",
    "Resource Type",
    "Settlement Point Name",
    "Resource Status",
    "HSL",
    "LSL",
    "Start Up Hot",
    "Start Up Inter",
    "Start Up Cold",
    "Min Gen Cost",
    "Awarded Quantity",
    "Energy Settlement Point Price",
    "RegUp Awarded",
    "RegUp MCPC",
    "RegDown Awarded",
    "RegDown MCPC",
    "RRSPFR Awarded",
    "RRSFFR Awarded",
    "RRSUFR Awarded",
    "RRS MCPC",
    "ECRSSD Awarded",
    "ECRS MCPC",
    "NonSpin Awarded",
    "NonSpin MCPC",
    "QSE submitted Curve",
]

# Same columns as gen/load resource AS offers
DAM_ESR_AS_OFFERS_COLUMNS = DAM_RESOURCE_AS_OFFERS_COLUMNS[:]

DAM_AS_ONLY_AWARDS_COLUMNS = [
    "Interval Start",
    "Interval End",
    "QSE",
    "AS Type",
    "Offer ID",
    "Quantity1 Award",
    "Quantity2 Award",
    "Quantity3 Award",
    "Quantity4 Award",
    "Quantity5 Award",
    "Total Award",
    "MCPC",
]

DAM_AS_ONLY_OFFERS_COLUMNS = [
    "Interval Start",
    "Interval End",
    "QSE",
    "AS Type",
    "Offer ID",
    "Offer Curve",
]

SCED_GEN_RESOURCE_COLUMNS = [
    "SCED Timestamp",
    "QSE",
    "DME",
    "Resource Name",
    "Resource Type",
    "Telemetered Resource Status",
    "Output Schedule",
    "HSL",
    "HASL",
    "HDL",
    "LSL",
    "LASL",
    "LDL",
    "Base Point",
    "Telemetered Net Output",
    "AS Responsibility for RegUp",
    "AS Responsibility for RegDown",
    "AS Responsibility for RRS",
    "AS Responsibility for RRSFFR",
    "AS Responsibility for NonSpin",
    "AS Responsibility for ECRS",
    "SCED1 Offer Curve",
    "SCED2 Offer Curve",
    "Start Up Cold Offer",
    "Start Up Hot Offer",
    "Start Up Inter Offer",
    "Min Gen Cost",
    "SCED TPO Offer Curve",
    "Ramp Rate Up",
    "Ramp Rate Down",
    "AS Capability RegUp",
    "AS Capability RegDown",
    "AS Capability ECRS",
    "AS Capability NonSpin",
    "AS Capability RRSPF",
    "AS Capability RRSFF",
    "AS Awards NonSpin",
    "AS Awards RRSFFR",
    "AS Awards RRSPFR",
    "AS Awards RRSUFR",
    "AS Awards ECRS",
    "AS Awards RegUp",
    "AS Awards RegDown",
]

SCED_LOAD_RESOURCE_COLUMNS = [
    "SCED Timestamp",
    "QSE",
    "DME",
    "Resource Name",
    "Telemetered Resource Status",
    "Max Power Consumption",
    "Low Power Consumption",
    "Real Power Consumption",
    "HASL",
    "HDL",
    "LASL",
    "LDL",
    "Base Point",
    "AS Responsibility for RRS",
    "AS Responsibility for RRSFFR",
    "AS Responsibility for NonSpin",
    "AS Responsibility for RegUp",
    "AS Responsibility for RegDown",
    "AS Responsibility for ECRS",
    "SCED Bid to Buy Curve",
    "Ramp Rate Up",
    "Ramp Rate Down",
    "AS Capability RegUp",
    "AS Capability RegDown",
    "AS Capability ECRS",
    "AS Capability NonSpin",
    "AS Capability RRSPF",
    "AS Capability RRSFF",
    "AS Capability RRSUF",
    "AS Awards NonSpin",
    "AS Awards RRSFFR",
    "AS Awards RRSPFR",
    "AS Awards RRSUFR",
    "AS Awards ECRS",
    "AS Awards RegUp",
    "AS Awards RegDown",
    "Self Provided RRSFFR",
    "Self Provided RRSUFR",
    "Self Provided ECRS",
]

SCED_SMNE_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Interval Time",
    "Interval Number",
    "Resource Name",
    "Interval Value",
]

SCED_ESR_COLUMNS = [
    "SCED Timestamp",
    "QSE",
    "DME",
    "Resource Name",
    "Resource Type",
    "SCED1 Offer Curve",
    "SCED2 Offer Curve",
    "Output Schedule",
    "HSL",
    "HDL",
    "LSL",
    "LDL",
    "Telemetered Resource Status",
    "Base Point",
    "Telemetered Net Output",
    "Ramp Rate Up",
    "Ramp Rate Down",
    "AS Capability RegUp",
    "AS Capability RegDown",
    "AS Capability ECRS",
    "AS Capability NonSpin",
    "AS Capability RRSPF",
    "AS Capability RRSFF",
    "SOC",
    "Min SOC",
    "Max SOC",
    "AS Awards NonSpin",
    "AS Awards RRSFFR",
    "AS Awards RRSPFR",
    "AS Awards RRSUFR",
    "AS Awards ECRS",
    "AS Awards RegUp",
    "AS Awards RegDown",
    "Bid Type",
    "Start Up Cold Offer",
    "Start Up Hot Offer",
    "Start Up Inter Offer",
    "Min Gen Cost",
    "SCED TPO Offer Curve",
    "Proxy Extension",
]

SCED_AS_OFFER_UPDATES_IN_OP_HOUR_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Resource Name",
    "AS Type",
    "Count of Updates During Operating Period",
]

SCED_RESOURCE_AS_OFFERS_COLUMNS = [
    "SCED Timestamp",
    "Resource Name",
    "Curve Type",
    "URS Offer Curve",
    "DRS Offer Curve",
    "RRSPFR Offer Curve",
    "RRSUFR Offer Curve",
    "RRSFFR Offer Curve",
    "NonSpin Offer Curve",
    "ECRS Offer Curve",
]


def _categorize_strings(df):
    """Convert object columns to category dtype, skipping curve columns.

    Curve columns (ending in 'Curve') and 'Block Indicators' contain structured
    data (lists or serialized arrays) that should not be categorized.
    """
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()
    df = df.copy()
    for col in df.select_dtypes(include=["object"]).columns:
        if col.endswith("Curve") or col == "Block Indicators":
            continue
        df[col] = df[col].astype("category")
    return df


def match_gen_load_names(list1, list2):
    """Match generator and load names"""
    list1.sort()
    list2.sort()
    prefix_dict = {}
    for item in list2:
        prefix = item.split("_")[0]
        if prefix not in prefix_dict:
            prefix_dict[prefix] = []
        prefix_dict[prefix].append(item)

    result = {}
    for item in list1:
        prefix = item.split("_")[0]
        if prefix in prefix_dict and prefix_dict[prefix]:
            result[item] = prefix_dict[prefix].pop(0)
        else:
            print(f"No match found for {item}")

    return result


def make_storage_resources(data):
    sced_gen = data["sced_gen_resource"][
        ["Resource Name", "QSE", "DME", "Resource Type"]
    ].drop_duplicates()
    sced_gen_storage_names = sced_gen[sced_gen["Resource Type"] == "PWRSTR"][
        "Resource Name"
    ].unique()
    sced_load_all = data["sced_load_resource"]["Resource Name"].unique()
    matched_load_gen_names = match_gen_load_names(sced_gen_storage_names, sced_load_all)

    storage_resources = (
        pd.DataFrame(
            {
                "gridstatus_id": list(matched_load_gen_names.keys()),
                "ercot_gen_resource_name": list(matched_load_gen_names.keys()),
                "ercot_load_resource_name": list(matched_load_gen_names.values()),
            },
        )
        .merge(
            data["settlement_point_mapping"],
            how="left",
            left_on="ercot_gen_resource_name",
            right_on="Resource Name",
        )
        .drop(columns=["Resource Name"])
        .merge(
            sced_gen,
            how="left",
            left_on="ercot_gen_resource_name",
            right_on="Resource Name",
        )
        .drop(columns=["Resource Name"])
        .rename(columns={"Settlement Point Name": "settlement_point_name"})
    )

    # Fill missing settlement point names with manual matches
    manual_matches = {
        "ESTONIAN_BES1": "ESTONIAN_ALL",
        "FENCESLR_BESS1": "FENCESLR_ALL",
        "MV_VALV4_BESS": "MV_VALV4_RN",
        "RVRVLYS_ESS1": "RVRVLYS_ALL",
        "RVRVLYS_ESS2": "RVRVLYS_ALL",
        "WFTANK_ESS1": "WFTANK_ESS1",
        "LONESTAR_BESS": "LONESTAR_RN",
    }
    storage_resources["settlement_point_name"] = storage_resources[
        "settlement_point_name"
    ].fillna(storage_resources["ercot_gen_resource_name"].map(manual_matches))

    # Get SARA data and merge with storage_resources
    cols = [
        "gridstatus_id",
        "unit_name",
        "ercot_gen_resource_name",
        "ercot_load_resource_name",
        "settlement_point_name",
        "qse",
        "dme",
        "resource_type",
        "county",
        "zone",
        "in_service_year",
        "installed_capacity_rating",
        "summer_capacity_mw",
        "generation_interconnection_project_code",
    ]
    storage_resources = (
        storage_resources.merge(
            data["sara"],
            how="left",
            left_on="ercot_gen_resource_name",
            right_on="Unit Code",
        )
        .drop(columns=["Unit Code", "Fuel", "New Planned Project Additions to Report"])
        .rename(
            columns={
                "Unit Name": "unit_name",
                "County": "county",
                "Zone": "zone",
                "Generation Interconnection Project Code": "generation_interconnection_project_code",  # noqa
                "In Service Year": "in_service_year",
                "Installed Capacity Rating": "installed_capacity_rating",
                "Summer Capacity (MW)": "summer_capacity_mw",
                "QSE": "qse",
                "DME": "dme",
                "Resource Type": "resource_type",
            },
        )[cols]
    )

    return pl.from_pandas(storage_resources)


def _categorize_strings_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Polars version of _categorize_strings(): cast String columns to
    Categorical, skipping curve columns and Block Indicators."""
    return df.with_columns(
        pl.col(c).cast(pl.Categorical)
        for c, dtype in df.schema.items()
        if dtype == pl.String and not (c.endswith("Curve") or c == "Block Indicators")
    )


def _is_present(expr: pl.Expr) -> pl.Expr:
    """Whether a numeric curve value is present: not null and not NaN.

    Matches the np.isnan() checks in the pandas curve extractors, where both
    missing fields (null) and explicit NaN values mark an absent pair.
    """
    value = expr.cast(pl.Float64)
    return value.is_not_null() & value.is_not_nan()


def extract_curve_as_pg_string_expr(mw_cols, price_cols):
    """Polars expression version of extract_curve_as_pg_string().

    Builds PG array strings like '{{100.0,25.5},{200.0,60.0}}' from paired
    MW/price columns without leaving polars (no pandas copy of the frame).
    """
    pieces = [
        pl.when(
            _is_present(pl.col(mw)) & _is_present(pl.col(price)),
        ).then(
            pl.concat_str(
                [
                    pl.lit("{"),
                    pl.col(mw).round(2).cast(pl.Float64).cast(pl.String),
                    pl.lit(","),
                    pl.col(price).round(2).cast(pl.Float64).cast(pl.String),
                    pl.lit("}"),
                ],
            ),
        )
        for mw, price in zip(mw_cols, price_cols)
    ]
    joined = pl.concat_str(pieces, separator=",", ignore_nulls=True)
    return (
        pl.when(joined != "")
        .then(pl.concat_str([pl.lit("{"), joined, pl.lit("}")]))
        .otherwise(None)
    )


def extract_curve_as_pg_string(df, mw_cols, price_cols):
    """Like extract_curve() but returns PG array strings directly.

    Returns pd.Series of strings like '{{100.0,25.5},{200.0,60.0}}'
    instead of Python list-of-lists. ~3x less peak memory.
    """
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()
    mw_arr = df[mw_cols].round(2).values  # (N, blocks) float64
    price_arr = df[price_cols].round(2).values
    valid = ~(np.isnan(mw_arr) | np.isnan(price_arr))

    result = np.empty(len(df), dtype=object)
    for i in range(len(df)):
        pairs = []
        for j in range(len(mw_cols)):
            if valid[i, j]:
                pairs.append(f"{{{mw_arr[i, j]},{price_arr[i, j]}}}")
        result[i] = "{" + ",".join(pairs) + "}" if pairs else None
    return pd.Series(result, index=df.index)


def extract_curve(
    df,
    curve_name=None,
    mw_suffix="-MW",
    price_suffix="-Price",
    mw_cols=None,
    price_cols=None,
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
):
    """Extract offer curve from dataframe columns.

    Supports two modes:
    1. Auto-detect columns by curve_name prefix (default):
       Looks for columns like "{curve_name}-MW1", "{curve_name}-Price1"

    2. Explicit column lists:
       Pass mw_cols and price_cols directly for custom column patterns
       e.g., mw_cols=["QUANTITY_MW1", "QUANTITY_MW2"],
             price_cols=["PRICE1_URS", "PRICE2_URS"]

    Args:
        df: DataFrame with curve data columns.
        curve_name: Prefix for auto-detecting MW/Price columns.
        mw_suffix: Suffix for MW columns in auto-detect mode.
        price_suffix: Suffix for price columns in auto-detect mode.
        mw_cols: Explicit list of MW column names.
        price_cols: Explicit list of price column names.
        output_format: CurveOutputFormat.LIST (default) returns Python list-of-lists
            per cell. CurveOutputFormat.PG_ARRAY_AS_STRING returns PG array strings like
            '{{mw,price},{mw,price}}' directly, using ~3x less peak memory.
    """
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()
    if mw_cols is None or price_cols is None:
        # Auto-detect by prefix
        mw_cols = [x for x in df.columns if x.startswith(curve_name + mw_suffix)]
        price_cols = [x for x in df.columns if x.startswith(curve_name + price_suffix)]

    if len(mw_cols) == 0 or len(price_cols) == 0:
        return np.nan

    if output_format == CurveOutputFormat.PG_ARRAY_AS_STRING:
        return extract_curve_as_pg_string(df, mw_cols, price_cols)

    # Vectorized extraction using numpy arrays
    mw_arr = df[mw_cols].round(2).values
    price_arr = df[price_cols].round(2).values
    n_rows = len(df)
    n_points = len(mw_cols)

    # Build curves using vectorized operations
    curves = []
    for i in range(n_rows):
        curve = [
            [float(mw_arr[i, j]), float(price_arr[i, j])]
            for j in range(n_points)
            if not (np.isnan(mw_arr[i, j]) or np.isnan(price_arr[i, j]))
        ]
        curves.append(curve if curve else None)

    return pd.Series(curves, index=df.index)


def extract_curve_expr(mw_cols: list[str], price_cols: list[str]) -> pl.Expr:
    """Polars expression version of extract_curve() for the LIST output
    format.

    Builds List(List(Float64)) cells like [[mw, price], ...] from paired
    MW/price columns, with all-null rows becoming null cells.
    """
    pairs = [
        pl.when(
            _is_present(pl.col(mw)) & _is_present(pl.col(price)),
        ).then(
            pl.concat_arr(
                [
                    pl.col(mw).cast(pl.Float64).round(2),
                    pl.col(price).cast(pl.Float64).round(2),
                ],
            ),
        )
        for mw, price in zip(mw_cols, price_cols)
    ]
    curve = pl.concat_list(pairs).list.drop_nulls()
    return pl.when(curve.list.len() > 0).then(curve).cast(pl.List(pl.List(pl.Float64)))


def _extract_curve_expr_by_prefix(
    df: pl.DataFrame,
    curve_name: str,
    mw_suffix: str = "-MW",
    price_suffix: str = "-Price",
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
) -> pl.Expr | None:
    """Prefix-detected polars version of extract_curve() supporting both
    output formats. Returns None when the frame has no matching curve columns
    (extract_curve() returned np.nan in that case)."""
    mw_cols = [x for x in df.columns if x.startswith(curve_name + mw_suffix)]
    price_cols = [x for x in df.columns if x.startswith(curve_name + price_suffix)]
    if len(mw_cols) == 0 or len(price_cols) == 0:
        return None
    if output_format == CurveOutputFormat.PG_ARRAY_AS_STRING:
        return extract_curve_as_pg_string_expr(mw_cols, price_cols)
    return extract_curve_expr(mw_cols, price_cols)


def process_dam_gen(
    df: pl.DataFrame,
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
) -> pl.DataFrame:
    time_cols = [
        "Interval Start",
        "Interval End",
    ]

    resource_cols = [
        "QSE",
        "DME",
        "Resource Name",
        "Resource Type",
        "Settlement Point Name",
    ]

    telemetry_cols = [
        "Resource Status",
        "HSL",
        "LSL",
        "Start Up Hot",
        "Start Up Inter",
        "Start Up Cold",
        "Min Gen Cost",
    ]

    energy_award_cols = [
        "Awarded Quantity",
        "Energy Settlement Point Price",
    ]

    as_cols = [
        "RegUp Awarded",
        "RegUp MCPC",
        "RegDown Awarded",
        "RegDown MCPC",
        "RRSPFR Awarded",
        "RRSFFR Awarded",
        "RRSUFR Awarded",
        "RRS MCPC",
        "ECRSSD Awarded",
        "ECRS MCPC",
        "NonSpin Awarded",
        "NonSpin MCPC",
    ]

    curve = "QSE submitted Curve"

    curve_expr = _extract_curve_expr_by_prefix(
        df,
        "QSE submitted Curve",
        output_format=output_format,
    )
    if curve_expr is None:
        curve_expr = pl.lit(None, dtype=pl.Float64)
    df = df.with_columns(curve_expr.alias(curve))

    all_cols = resource_cols + telemetry_cols + energy_award_cols + as_cols + [curve]

    df = df.with_columns(
        pl.lit(None, dtype=pl.Float64).alias(col)
        for col in all_cols
        if col not in df.columns
    )

    df = df.select(time_cols + all_cols)

    return _categorize_strings_polars(df)


def process_dam_load(df: pl.DataFrame) -> pl.DataFrame:
    time_cols = [
        "Time",
        "Interval Start",
        "Interval End",
    ]

    resource_cols = ["Load Resource Name"]

    telemetry_cols = [
        "Max Power Consumption for Load Resource",
        "Low Power Consumption for Load Resource",
    ]

    as_cols = [
        "RegUp Awarded",
        "RegUp MCPC",
        "RegDown Awarded",
        "RegDown MCPC",
        "RRSPFR Awarded",
        "RRSFFR Awarded",
        "RRSUFR Awarded",
        "RRS MCPC",
        "ECRSSD Awarded",
        "ECRSMD Awarded",
        "ECRS MCPC",
        "NonSpin Awarded",
        "NonSpin MCPC",
    ]

    all_cols = resource_cols + telemetry_cols + as_cols

    df = df.with_columns(
        pl.lit(None, dtype=pl.Float64).alias(col)
        for col in all_cols
        if col not in df.columns
    )

    df = df.select(time_cols + all_cols)

    # rename for consistency
    # with gen columns
    df = df.rename(
        {
            "Load Resource Name": "Resource Name",
        },
    )

    return _categorize_strings_polars(df)


def process_dam_esr(
    df: pl.DataFrame,
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
) -> pl.DataFrame:
    time_cols = [
        "Interval Start",
        "Interval End",
    ]

    resource_cols = [
        "QSE",
        "DME",
        "Resource Name",
        "Resource Type",
        "Settlement Point Name",
    ]

    telemetry_cols = [
        "Resource Status",
        "HSL",
        "LSL",
        "Start Up Hot",
        "Start Up Inter",
        "Start Up Cold",
        "Min Gen Cost",
    ]

    energy_award_cols = [
        "Awarded Quantity",
        "Energy Settlement Point Price",
    ]

    as_cols = [
        "RegUp Awarded",
        "RegUp MCPC",
        "RegDown Awarded",
        "RegDown MCPC",
        "RRSPFR Awarded",
        "RRSFFR Awarded",
        "RRSUFR Awarded",
        "RRS MCPC",
        "ECRSSD Awarded",
        "ECRS MCPC",
        "NonSpin Awarded",
        "NonSpin MCPC",
    ]

    curve = "QSE submitted Curve"

    curve_expr = _extract_curve_expr_by_prefix(
        df,
        "QSE submitted Curve",
        output_format=output_format,
    )
    if curve_expr is None:
        curve_expr = pl.lit(None, dtype=pl.Float64)
    df = df.with_columns(curve_expr.alias(curve))

    all_cols = resource_cols + telemetry_cols + energy_award_cols + as_cols + [curve]

    df = df.with_columns(
        pl.lit(None, dtype=pl.Float64).alias(col)
        for col in all_cols
        if col not in df.columns
    )

    df = df.select(time_cols + all_cols)

    return _categorize_strings_polars(df)


def process_dam_esr_as_offers(
    df: pl.DataFrame,
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
) -> pl.DataFrame:
    return process_as_offer_curves(df, output_format=output_format)


def process_dam_or_gen_load_as_offers(
    df: pl.DataFrame,
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
) -> pl.DataFrame:
    if "QSE" not in df.columns:
        # after Interval End
        df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias("QSE"))

    if "DME" not in df.columns:
        # after QSE
        df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias("DME"))

    df = df.rename(
        {
            old: "Resource Name"
            for old in ("Load Resource Name", "Generation Resource Name")
            if old in df.columns
        },
    )

    return process_as_offer_curves(df, output_format=output_format)


def process_as_offer_curves(
    df: pl.DataFrame,
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
) -> pl.DataFrame:
    block_columns = [col for col in df.columns if col.startswith("BLOCK INDICATOR")]
    block_count = len(block_columns)

    # Older files will not have all of these ancillary services
    all_ancillary_services = [
        "RRSPFR",
        "RRSFFR",
        "RRSUFR",
        "ECRS",
        "OFFEC",
        "ONLINE NONSPIN",
        "REGUP",
        "REGDOWN",
        "OFFLINE NONSPIN",
    ]

    # Correct ordering of ancillary services columns
    as_offer_curve_column_names = [
        f"{service} Offer Curve" for service in all_ancillary_services
    ]

    # Check for which ancillary services are present in the file. We must use replace
    # to not miss ONLINE and OFFLINE NONSPIN
    ancillary_services_in_file = [
        col.replace("PRICE1 ", "") for col in df.columns if col.startswith("PRICE1")
    ]

    present_ancillary_services = [
        s for s in all_ancillary_services if s in ancillary_services_in_file
    ]

    missing_ancillary_services = list(
        set(all_ancillary_services) - set(present_ancillary_services),
    )

    ancillary_services_column_lists = []

    # Construct a list of lists like [["PRICE1 RRSPFR", "QUANTITY MW1", "PRICE2
    # RRSPFR", "QUANTITY MW2"], ...] to iterate over them and extract offer curve data
    for service in present_ancillary_services:
        service_columns = []
        for i in range(1, block_count + 1):
            service_columns.extend([f"PRICE{i} {service}", f"QUANTITY MW{i}"])

        ancillary_services_column_lists.append(service_columns)

    group_keys = ["Interval Start", "Interval End", "Resource Name", "QSE", "DME"]

    n_blocks_expr = (
        pl.sum_horizontal(pl.col(c).is_not_null() for c in block_columns)
        if block_columns
        else pl.lit(0)
    )
    has_price_exprs = []
    for index, column_list in enumerate(ancillary_services_column_lists):
        price_columns = [c for c in column_list if c.startswith("PRICE")]
        has_price_exprs.append(
            (
                pl.any_horizontal(pl.col(c).is_not_null() for c in price_columns)
                if price_columns
                else pl.lit(True)
            ).alias(f"__has_price_{index}"),
        )

    df = df.with_columns(n_blocks_expr.alias("__n_blocks"), *has_price_exprs)
    # Sort by the group keys with nulls last so group iteration order matches
    # the old sorted pandas groupby; maintain_order keeps the original row
    # order within groups.
    df = df.sort(group_keys, nulls_last=True, maintain_order=True)

    constructed_data = []

    # Group by each interval and resource name because each resource can have multiple
    # rows at one interval. These rows represent different AS products.
    # We must use dropna=False because QSE and DME may be all null (polars
    # group_by keeps null keys by default).
    for key, group in df.group_by(group_keys, maintain_order=True):
        interval_start, interval_end, resource_name, qse, dme = key

        # Find the block list with the most non-null elements which represents the
        # number of blocks where the resource made an offer. Block columns that
        # are all null within the group are dropped from the list.
        block_row_idx = int(group["__n_blocks"].arg_max())
        max_block_list = [
            group[c][block_row_idx]
            for c in block_columns
            if group[c].null_count() < group.height
        ]

        group_data = {
            "Interval Start": interval_start,
            "Interval End": interval_end,
            "QSE": qse,
            "DME": dme,
            "Resource Name": resource_name,
            "Multi-Hour Block Flag": group["Multi-Hour Block Flag"][0],
            "Block Indicators": max_block_list,
        }

        # Iterate through each ancillary service and extract the offer curve data
        for index, column_list in enumerate(ancillary_services_column_lists):
            # Drop rows where all prices are NaN. This should leave us with only 1 row
            subset = group.filter(pl.col(f"__has_price_{index}"))

            if subset.height > 1:
                # We've identified an issue where
                # there are sometimes multiple offers for the same service at the same
                # interval. In theory this should never happen. The QUANTITY MW are
                # only different by 0.1, so we just take the row with the lowest
                # quantity. This is a temporary fix until we can figure out why this
                # is happening.
                logger.info(
                    f"Found {subset.height} rows for {resource_name}across columns "
                    f"{column_list}. Taking the row with the lowest quantity",
                )
                subset = subset.sort("QUANTITY MW1", nulls_last=True).head(1)

            if subset.height == 0:
                curve = None
            else:
                # Only keep the number of block indicators that are non-null
                if block_columns:
                    block_values = subset.select(block_columns).row(0)
                    keep_block_count = sum(
                        1 for v in block_values if v is not None and v == v
                    )
                else:
                    keep_block_count = 0

                kept_columns = column_list[: keep_block_count * 2]

                if not kept_columns:
                    curve = None
                else:
                    subset_values = np.asarray(
                        [
                            0
                            if v is None or (isinstance(v, float) and np.isnan(v))
                            else v
                            for v in subset.select(kept_columns).row(0)
                        ],
                    )

                    if output_format == CurveOutputFormat.PG_ARRAY_AS_STRING:
                        pairs = []
                        for i in range(0, len(subset_values), 2):
                            pairs.append(
                                f"{{{subset_values[i]},{subset_values[i + 1]}}}",
                            )
                        curve = "{" + ",".join(pairs) + "}"
                    else:
                        curve = []
                        for i in range(0, len(subset_values), 2):
                            curve.append(subset_values[i : i + 2].tolist())

            curve_name = f"{present_ancillary_services[index]} Offer Curve"
            group_data[curve_name] = curve

            for service in missing_ancillary_services:
                group_data[f"{service} Offer Curve"] = None

        constructed_data.append(group_data)

    non_null_block_dtypes = [
        df.schema[c] for c in block_columns if df[c].null_count() < df.height
    ]
    block_dtype = non_null_block_dtypes[0] if non_null_block_dtypes else pl.Null

    curve_dtype = (
        pl.String
        if output_format == CurveOutputFormat.PG_ARRAY_AS_STRING
        else pl.List(pl.List(pl.Float64))
    )

    # The old pandas implementation rebuilt this frame from the group keys,
    # which pandas infers as nanosecond datetimes; pin ns to keep the output
    # dtype identical.
    schema = {
        "Interval Start": pl.Datetime("ns", df.schema["Interval Start"].time_zone),
        "Interval End": pl.Datetime("ns", df.schema["Interval End"].time_zone),
        "QSE": df.schema["QSE"],
        "DME": df.schema["DME"],
        "Resource Name": df.schema["Resource Name"],
        "Multi-Hour Block Flag": df.schema["Multi-Hour Block Flag"],
        "Block Indicators": pl.List(block_dtype),
    }
    for service in all_ancillary_services:
        schema[f"{service} Offer Curve"] = (
            curve_dtype if service in present_ancillary_services else pl.Null
        )

    out = pl.from_dicts(constructed_data, schema=schema)

    out = out.select(
        [
            "Interval Start",
            "Interval End",
            "QSE",
            "DME",
            "Resource Name",
            "Multi-Hour Block Flag",
            "Block Indicators",
        ]
        + as_offer_curve_column_names,
    )

    return _categorize_strings_polars(out)


def process_dam_energy_only_offer_awards(df: pl.DataFrame) -> pl.DataFrame:
    rename_map = {"Settlement Point": "Settlement Point Name", "QSE Name": "QSE"}
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    df = df.select(DAM_ENERGY_ONLY_OFFER_AWARDS_COLUMNS).sort(
        ["Interval Start", "Settlement Point Name"],
        nulls_last=True,
        maintain_order=True,
    )
    return _categorize_strings_polars(df)


def process_dam_energy_only_offers(
    df: pl.DataFrame,
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
) -> pl.DataFrame:
    rename_map = {
        "Settlement Point": "Settlement Point Name",
        "QSE Name": "QSE",
        "Block/Curve indicator": "Block or Curve indicator",
    }
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    curve_name = "Energy Only Offer"

    curve_expr = _extract_curve_expr_by_prefix(
        df,
        curve_name,
        mw_suffix=" MW",
        price_suffix=" Price",
        output_format=output_format,
    )
    if curve_expr is None:
        curve_expr = pl.lit(None, dtype=pl.Float64)
    df = df.with_columns(curve_expr.alias(curve_name + " Curve"))

    df = df.select(DAM_ENERGY_ONLY_OFFERS_COLUMNS).sort(
        ["Interval Start", "Settlement Point Name"],
        nulls_last=True,
        maintain_order=True,
    )
    return _categorize_strings_polars(df)


def process_dam_ptp_obligation_bid_awards(df: pl.DataFrame) -> pl.DataFrame:
    rename_map = {"QSE Name": "QSE"}
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    df = df.select(DAM_PTP_OBLIGATION_BID_AWARDS_COLUMNS).sort(
        ["Interval Start", "QSE"],
        nulls_last=True,
        maintain_order=True,
    )
    return _categorize_strings_polars(df)


def process_dam_ptp_obligation_bids(df: pl.DataFrame) -> pl.DataFrame:
    rename_map = {"QSE Name": "QSE"}
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    df = df.select(DAM_PTP_OBLIGATION_BIDS_COLUMNS).sort(
        ["Interval Start", "QSE"],
        nulls_last=True,
        maintain_order=True,
    )
    return _categorize_strings_polars(df)


def process_dam_energy_bid_awards(df: pl.DataFrame) -> pl.DataFrame:
    rename_map = {"Settlement Point": "Settlement Point Name", "QSE Name": "QSE"}
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    df = df.select(DAM_ENERGY_BID_AWARDS_COLUMNS).sort(
        ["Interval Start", "Settlement Point Name"],
        nulls_last=True,
        maintain_order=True,
    )
    return _categorize_strings_polars(df)


def process_dam_energy_bids(
    df: pl.DataFrame,
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
) -> pl.DataFrame:
    rename_map = {
        "Settlement Point": "Settlement Point Name",
        "QSE Name": "QSE",
        "Block/Curve indicator": "Block or Curve indicator",
    }
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    curve_name = "Energy Only Bid"

    curve_expr = _extract_curve_expr_by_prefix(
        df,
        curve_name,
        mw_suffix=" MW",
        price_suffix=" Price",
        output_format=output_format,
    )
    if curve_expr is None:
        curve_expr = pl.lit(None, dtype=pl.Float64)
    df = df.with_columns(curve_expr.alias(curve_name + " Curve"))

    df = df.select(DAM_ENERGY_BIDS_COLUMNS).sort(
        ["Interval Start", "Settlement Point Name"],
        nulls_last=True,
        maintain_order=True,
    )
    return _categorize_strings_polars(df)


def process_dam_ptp_obligation_option(df: pl.DataFrame) -> pl.DataFrame:
    rename_map = {"QSE Name": "QSE"}
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    df = df.select(DAM_PTP_OBLIGATION_OPTION_COLUMNS).sort(
        ["Interval Start", "QSE"],
        nulls_last=True,
        maintain_order=True,
    )
    return _categorize_strings_polars(df)


def process_dam_ptp_obligation_option_awards(df: pl.DataFrame) -> pl.DataFrame:
    rename_map = {"QSE Name": "QSE"}
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    df = df.select(DAM_PTP_OBLIGATION_OPTION_AWARDS_COLUMNS).sort(
        ["Interval Start", "QSE"],
        nulls_last=True,
        maintain_order=True,
    )
    return _categorize_strings_polars(df)


def process_dam_as_only_awards(df: pl.DataFrame) -> pl.DataFrame:
    rename_map = {
        "QSE Name": "QSE",
        "Quantity1_Award": "Quantity1 Award",
        "Quantity2_Award": "Quantity2 Award",
        "Quantity3_Award": "Quantity3 Award",
        "Quantity4_Award": "Quantity4 Award",
        "Quantity5_Award": "Quantity5 Award",
        "Total_Award": "Total Award",
    }
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    df = df.with_columns(
        pl.lit(None, dtype=pl.Float64).alias(col)
        for col in DAM_AS_ONLY_AWARDS_COLUMNS
        if col not in df.columns
    )

    df = df.select(DAM_AS_ONLY_AWARDS_COLUMNS).sort(
        ["Interval Start", "QSE", "AS Type", "Offer ID"],
        nulls_last=True,
        maintain_order=True,
    )
    return _categorize_strings_polars(df)


def process_dam_as_only_offers(
    df: pl.DataFrame,
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
) -> pl.DataFrame:
    rename_map = {"QSE Name": "QSE"}
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    curve_expr = _extract_curve_expr_by_prefix(
        df,
        "AS Only Offer",
        mw_suffix=" MW",
        price_suffix=" Price",
        output_format=output_format,
    )
    if curve_expr is None:
        curve_expr = pl.lit(None, dtype=pl.Float64)
    df = df.with_columns(curve_expr.alias("Offer Curve"))

    df = df.with_columns(
        pl.lit(None, dtype=pl.Float64).alias(col)
        for col in DAM_AS_ONLY_OFFERS_COLUMNS
        if col not in df.columns
    )

    df = df.select(DAM_AS_ONLY_OFFERS_COLUMNS).sort(
        ["Interval Start", "QSE", "AS Type", "Offer ID"],
        nulls_last=True,
        maintain_order=True,
    )
    return _categorize_strings_polars(df)


def _finalize_sced_frame(
    df: pl.DataFrame,
    curve_specs: list[tuple[str, str]],
    rename_map: dict[str, str],
    output_columns: list[str],
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
) -> pl.DataFrame:
    """Shared tail for the SCED process functions: extract offer curves,
    apply renames, null-fill missing columns, select the final column set,
    and categorize strings."""
    curve_exprs = []
    for curve_name, out_col in curve_specs:
        expr = _extract_curve_expr_by_prefix(
            df,
            curve_name,
            output_format=output_format,
        )
        if expr is None:
            expr = pl.lit(None, dtype=pl.Float64)
        curve_exprs.append(expr.alias(out_col))
    if curve_exprs:
        df = df.with_columns(curve_exprs)

    df = df.rename(
        {old: new for old, new in rename_map.items() if old in df.columns},
    )
    df = df.with_columns(
        pl.lit(None, dtype=pl.Float64).alias(col)
        for col in output_columns
        if col not in df.columns
    )
    return _categorize_strings_polars(df.select(output_columns))


def process_sced_gen(
    df: pl.DataFrame,
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
) -> pl.DataFrame:
    # Strip whitespace from column names
    df = df.rename({c: c.strip() for c in df.columns if c != c.strip()})
    return _finalize_sced_frame(
        df,
        curve_specs=[
            ("SCED1 Curve", "SCED1 Offer Curve"),
            ("SCED2 Curve", "SCED2 Offer Curve"),
            ("Submitted TPO", "SCED TPO Offer Curve"),
        ],
        # standardized to same naming as load
        # clean up column names
        rename_map={
            "Ancillary Service RRS": "AS Responsibility for RRS",
            "Ancillary Service RRSFFR": "AS Responsibility for RRSFFR",
            "Ancillary Service NSRS": "AS Responsibility for NonSpin",
            "Ancillary Service REGUP": "AS Responsibility for RegUp",
            "Ancillary Service REGDN": "AS Responsibility for RegDown",
            "Ancillary Service ECRS": "AS Responsibility for ECRS",
            # Rename REGUP -> RegUp, REGDN -> RegDown, NSPIN -> NonSpin
            "AS Capability REGUP": "AS Capability RegUp",
            "AS Capability REGDN": "AS Capability RegDown",
            "AS Capability NSPIN": "AS Capability NonSpin",
            "AS Awards REGUP": "AS Awards RegUp",
            "AS Awards REGDN": "AS Awards RegDown",
            "AS Awards NSPIN": "AS Awards NonSpin",
        },
        output_columns=SCED_GEN_RESOURCE_COLUMNS,
        output_format=output_format,
    )


def process_sced_load(
    df: pl.DataFrame,
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
) -> pl.DataFrame:
    return _finalize_sced_frame(
        df,
        curve_specs=[("SCED Bid to Buy Curve", "SCED Bid to Buy Curve")],
        rename_map={
            # Rename REGUP -> RegUp, REGDN -> RegDown, NSPIN -> NonSpin
            "AS Capability REGUP": "AS Capability RegUp",
            "AS Capability REGDN": "AS Capability RegDown",
            "AS Capability NSPIN": "AS Capability NonSpin",
            "AS Awards REGUP": "AS Awards RegUp",
            "AS Awards REGDN": "AS Awards RegDown",
            "AS Awards NSPIN": "AS Awards NonSpin",
        },
        output_columns=SCED_LOAD_RESOURCE_COLUMNS,
        output_format=output_format,
    )


def process_sced_esr(
    df: pl.DataFrame,
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
) -> pl.DataFrame:
    # SOC columns added in Feb 2026 ESR data
    return _finalize_sced_frame(
        df,
        curve_specs=[
            ("SCED1 Curve", "SCED1 Offer Curve"),
            ("SCED2 Curve", "SCED2 Offer Curve"),
            ("Submitted TPO", "SCED TPO Offer Curve"),
        ],
        rename_map={
            # Rename REGUP -> RegUp, REGDN -> RegDown, NSPIN -> NonSpin
            "AS Capability REGUP": "AS Capability RegUp",
            "AS Capability REGDN": "AS Capability RegDown",
            "AS Capability NSPIN": "AS Capability NonSpin",
            "AS Awards REGUP": "AS Awards RegUp",
            "AS Awards REGDN": "AS Awards RegDown",
            "AS Awards NSPIN": "AS Awards NonSpin",
            # Rename Bid_Type -> Bid Type
            "Bid_Type": "Bid Type",
            # Rename SOC columns
            "State of Charge": "SOC",
            "Minimum SOC": "Min SOC",
            "Maximum SOC": "Max SOC",
        },
        output_columns=SCED_ESR_COLUMNS,
        output_format=output_format,
    )


def process_sced_as_offer_updates_in_op_hour(df):
    """Process SCED AS Offer Updates in Operating Hour data.

    This data tracks the count of Ancillary Service offer updates
    made by resources during operating hours.

    Expects df to already have Interval Start/End from parse_doc().
    """
    return _categorize_strings_polars(
        df.select(SCED_AS_OFFER_UPDATES_IN_OP_HOUR_COLUMNS),
    )


def process_sced_resource_as_offers(
    df: pl.DataFrame,
    output_format: CurveOutputFormat | str = CurveOutputFormat.LIST,
) -> pl.DataFrame:
    """Process SCED Resource AS Offers data.

    This data contains ancillary service offer curves at the SCED timestamp level.
    Each row has price/quantity pairs for each AS type across 6 blocks.

    Source columns: PRICEn_URS, PRICEn_DRS, PRICEn_RRSPF, PRICEn_RRSUF,
                   PRICEn_RRSFF, PRICEn_NS, PRICEn_ECRS, QUANTITY_MWn (n=1-6)

    Creates offer curves for each AS type. Format depends on output_format:
    list-of-lists like [[mw, price], ...] or PG array strings.

    Args:
        df: DataFrame with raw SCED resource AS offers data.
        output_format: "list" (default) returns list-of-lists per cell.
            "pg_array_as_string" returns PG array strings like '{{mw,price},{mw,price}}'
            directly, using ~3x less peak memory.
    """
    # ERCOT renamed the AS-price column suffixes in late March 2026
    # (_URS->_REGUP, _DRS->_REGDN, _NS->_NSPIN, _RRSPF->_RRSPFR,
    # _RRSUF->_RRSUFR, _RRSFF->_RRSFFR). Rename them back to the original names
    # so the curve-type and curve-extraction logic below works unchanged for
    # both old and new files.
    new_to_old_suffix = {
        "_REGUP": "_URS",
        "_REGDN": "_DRS",
        "_RRSPFR": "_RRSPF",
        "_RRSUFR": "_RRSUF",
        "_RRSFFR": "_RRSFF",
        "_NSPIN": "_NS",
    }

    def _rename_suffix(col: str) -> str:
        for new_suffix, old_suffix in new_to_old_suffix.items():
            if col.endswith(new_suffix):
                return col[: -len(new_suffix)] + old_suffix
        return col

    df = df.rename({c: _rename_suffix(c) for c in df.columns})

    # First create a curve_type column with the logic:
    # regulation down : values only in _DRS columns and not in other columns
    # offline: values in _NS and optionally _ECRS columns and not in other columns
    # online : values in any column except for _DRS
    price_cols = [c for c in df.columns if c.startswith("PRICE")]
    drs_cols = [c for c in price_cols if c.endswith("_DRS")]
    ns_cols = [c for c in price_cols if c.endswith("_NS")]
    non_drs_cols = [c for c in price_cols if not c.endswith("_DRS")]
    non_drs_ns_ecrs_cols = [c for c in non_drs_cols if not c.endswith(("_NS", "_ECRS"))]

    def _has_value(cols: list[str]) -> pl.Expr:
        # Treat null and NaN values as "no value" (same as zero). ERCOT
        # corrected files (April 4, 2026+) use NaN instead of zero for empty
        # AS Sub-Type Offer Prices (see ERCOT notice M-B040326-01). Unlike
        # pandas fillna(0), polars fill_null() does not fill NaN, so both are
        # filled explicitly.
        return pl.any_horizontal(
            pl.col(c).cast(pl.Float64).fill_nan(0).fill_null(0) != 0 for c in cols
        )

    has_drs = _has_value(drs_cols)
    has_non_drs = _has_value(non_drs_cols)
    has_ns = _has_value(ns_cols)
    has_non_drs_ns_ecrs = _has_value(non_drs_ns_ecrs_cols)

    df = df.with_columns(
        pl.when(has_non_drs & has_non_drs_ns_ecrs)
        .then(pl.lit("Online"))
        .when(has_ns & ~has_drs & ~has_non_drs_ns_ecrs)
        .then(pl.lit("Offline"))
        .when(has_drs & ~has_non_drs)
        .then(pl.lit("Regulation Down"))
        .otherwise(pl.lit("unknown"))
        .alias("Curve Type"),
    )

    if df["Curve Type"].eq("unknown").any():
        raise ValueError("Unknown curve type found")

    # Map source column suffixes to output curve names
    as_type_mapping = {
        "URS": "URS Offer Curve",
        "DRS": "DRS Offer Curve",
        "RRSPF": "RRSPFR Offer Curve",
        "RRSUF": "RRSUFR Offer Curve",
        "RRSFF": "RRSFFR Offer Curve",
        "NS": "NonSpin Offer Curve",
        "ECRS": "ECRS Offer Curve",
    }

    # Find the number of blocks by counting QUANTITY_MW columns
    qty_cols = sorted([col for col in df.columns if col.startswith("QUANTITY_MW")])
    block_count = len(qty_cols)

    if block_count == 0:
        return df

    # Extract MW column names (shared across all AS types)
    mw_cols = [f"QUANTITY_MW{i}" for i in range(1, block_count + 1)]

    use_pg = output_format == CurveOutputFormat.PG_ARRAY_AS_STRING
    null_dtype = pl.String if use_pg else pl.List(pl.List(pl.Float64))

    # Extract curves for each AS type
    curve_exprs = []
    for as_suffix, curve_col in as_type_mapping.items():
        as_price_cols = [f"PRICE{i}_{as_suffix}" for i in range(1, block_count + 1)]
        as_price_cols = [c for c in as_price_cols if c in df.columns]

        if not as_price_cols:
            curve_exprs.append(pl.lit(None, dtype=null_dtype).alias(curve_col))
            continue

        extract_fn = extract_curve_as_pg_string_expr if use_pg else extract_curve_expr
        curve_exprs.append(
            extract_fn(
                mw_cols=mw_cols[: len(as_price_cols)],
                price_cols=as_price_cols,
            ).alias(curve_col),
        )

    df = df.with_columns(curve_exprs)

    return _categorize_strings_polars(df.select(SCED_RESOURCE_AS_OFFERS_COLUMNS))


# # backup for more node names
# pd.read_html("https://www.ercot.com/content/cdr/html/current_np6788.html", skiprows=3)[0][0] # noqa
# # todo add in QSE
# # todo prefix and county match
