import numpy as np
import pandas as pd

from gridstatus.gs_logging import setup_gs_logger

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

SCED_LOAD_RESOURCE_KEY = "sced_load_resource"
SCED_GEN_RESOURCE_KEY = "sced_gen_resource"
SCED_SMNE_KEY = "sced_smne"


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

SCED_GEN_RESOURCE_COLUMNS = [
    "Interval Start",
    "Interval End",
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
    "Start Up Cold Offer",
    "Start Up Hot Offer",
    "Start Up Inter Offer",
    "Min Gen Cost",
    "SCED TPO Offer Curve",
]

SCED_LOAD_RESOURCE_COLUMNS = [
    "Interval Start",
    "Interval End",
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
]

SCED_SMNE_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Interval Time",
    "Interval Number",
    "Resource Name",
    "Interval Value",
]


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

    return storage_resources


def extract_curve(df, curve_name, mw_suffix="-MW", price_suffix="-Price"):
    mw_cols = [x for x in df.columns if x.startswith(curve_name + mw_suffix)]
    price_cols = [x for x in df.columns if x.startswith(curve_name + price_suffix)]

    if len(mw_cols) == 0 or len(price_cols) == 0:
        return np.nan

    def combine_mw_price(row):
        return [
            [mw, price]
            for mw, price in zip(row[mw_cols], row[price_cols])
            if pd.notnull(mw) and pd.notnull(price)
        ]

    # round price columns to 2 decimal places
    df[price_cols] = df[price_cols].round(2)
    return df.apply(combine_mw_price, axis=1)


def process_dam_gen(df):
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

    df[curve] = extract_curve(df, "QSE submitted Curve")

    all_cols = resource_cols + telemetry_cols + energy_award_cols + as_cols + [curve]

    for col in all_cols:
        if col not in df.columns:
            df[col] = np.nan

    df = df[time_cols + all_cols]

    return df


def process_dam_load(df):
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

    for col in all_cols:
        if col not in df.columns:
            df[col] = np.nan

    df = df[time_cols + all_cols]

    # rename for consistency
    # with gen columns
    df = df.rename(
        columns={
            "Load Resource Name": "Resource Name",
        },
    )

    return df


def process_dam_or_gen_load_as_offers(df):
    if "QSE" not in df.columns:
        # after Interval End
        index = df.columns.tolist().index("Interval End") + 1
        df.insert(index, "QSE", np.nan)

    if "DME" not in df.columns:
        # after QSE
        index = df.columns.tolist().index("QSE") + 1
        df.insert(index, "DME", np.nan)

    df = df.rename(
        columns={
            "Load Resource Name": "Resource Name",
            "Generation Resource Name": "Resource Name",
        },
    )

    return process_as_offer_curves(df)


def process_as_offer_curves(df):
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

    # Check for which ancillary services are present in the file
    ancillary_services_in_file = [
        col.split(" ")[1] for col in df.columns if col.startswith("PRICE1")
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

    constructed_data = []

    # Group by each interval and resource name because each resource can have multiple
    # rows at one interval. These rows represent different AS products.
    for (interval_start, interval_end, resource_name, qse, dme), group in df.groupby(
        # We must use dropna=False because QSE and DME may be all null
        ["Interval Start", "Interval End", "Resource Name", "QSE", "DME"],
        dropna=False,
    ):
        # Find the block list with the most non-null elements which represents the
        # number of blocks where the resource made an offer
        block_lists = (
            group[block_columns].dropna(axis="columns", how="all").values.tolist()
        )

        max_block_list = max(
            block_lists,
            key=lambda x: len([elem for elem in x if not pd.isnull(elem)]),
        )

        group_data = {
            "Interval Start": interval_start,
            "Interval End": interval_end,
            "QSE": qse,
            "DME": dme,
            "Resource Name": resource_name,
            "Multi-Hour Block Flag": group["Multi-Hour Block Flag"].iloc[0],
            "Block Indicators": max_block_list,
        }

        # Iterate through each ancillary service and extract the offer curve data
        for index, column_list in enumerate(ancillary_services_column_lists):
            # Drop rows where all prices are NaN. This should leave us with only 1 row
            price_columns = [c for c in column_list if c.startswith("PRICE")]

            subset = group[column_list + block_columns].dropna(
                axis="rows",
                how="all",
                subset=price_columns,
            )

            if len(subset) > 1:
                # We've identified an issue where
                # there are sometimes multiple offers for the same service at the same
                # interval. In theory this should never happen. The QUANTITY MW are
                # only different by 0.1, so we just take the row with the lowest
                # quantity. This is a temporary fix until we can figure out why this
                # is happening.
                logger.info(
                    f"Found {len(subset)} rows for {resource_name}across columns "
                    f"{column_list}. Taking the row with the lowest quantity",
                )
                subset = subset.sort_values("QUANTITY MW1").head(1)

            # Only keep the number of block indicators that are non-null
            keep_block_count = subset[block_columns].notna().sum().sum()
            subset = subset.drop(columns=block_columns).loc[
                :,
                column_list[: keep_block_count * 2],
            ]

            if subset.empty:
                curve = None
            else:
                # Convert the column values to a list of lists like
                # [[price1, quantity1], [price2, quantity2], ...]
                subset_values = subset.replace({np.nan: 0}).values[0]

                curve = []
                for i in range(0, len(subset_values), 2):
                    # Iterate through 2 columns at a time to get the price and quantity
                    curve.append(subset_values[i : i + 2].tolist())

            curve_name = f"{present_ancillary_services[index]} Offer Curve"
            group_data[curve_name] = curve

            for service in missing_ancillary_services:
                group_data[f"{service} Offer Curve"] = None

        constructed_data.append(group_data)

    df = pd.DataFrame(constructed_data).replace({None: pd.NA})[
        [
            "Interval Start",
            "Interval End",
            "QSE",
            "DME",
            "Resource Name",
            "Multi-Hour Block Flag",
            "Block Indicators",
        ]
        + as_offer_curve_column_names
    ]

    return df


def process_dam_energy_only_offer_awards(df):
    df = df.rename(
        columns={"Settlement Point": "Settlement Point Name", "QSE Name": "QSE"},
    )

    return df[DAM_ENERGY_ONLY_OFFER_AWARDS_COLUMNS].sort_values(
        ["Interval Start", "Settlement Point Name"],
    )


def process_dam_energy_only_offers(df):
    df = df.rename(
        columns={
            "Settlement Point": "Settlement Point Name",
            "QSE Name": "QSE",
            "Block/Curve indicator": "Block or Curve indicator",
        },
    )

    curve_name = "Energy Only Offer"

    df[curve_name + " Curve"] = extract_curve(
        df,
        curve_name,
        mw_suffix=" MW",
        price_suffix=" Price",
    )

    return df[DAM_ENERGY_ONLY_OFFERS_COLUMNS].sort_values(
        ["Interval Start", "Settlement Point Name"],
    )


def process_dam_ptp_obligation_bid_awards(df):
    df = df.rename(columns={"QSE Name": "QSE"})

    return df[DAM_PTP_OBLIGATION_BID_AWARDS_COLUMNS].sort_values(
        ["Interval Start", "QSE"],
    )


def process_dam_ptp_obligation_bids(df):
    df = df.rename(columns={"QSE Name": "QSE"})

    return df[DAM_PTP_OBLIGATION_BIDS_COLUMNS].sort_values(["Interval Start", "QSE"])


def process_dam_energy_bid_awards(df):
    df = df.rename(
        columns={"Settlement Point": "Settlement Point Name", "QSE Name": "QSE"},
    )

    return df[DAM_ENERGY_BID_AWARDS_COLUMNS].sort_values(
        ["Interval Start", "Settlement Point Name"],
    )


def process_dam_energy_bids(df):
    df = df.rename(
        columns={
            "Settlement Point": "Settlement Point Name",
            "QSE Name": "QSE",
            "Block/Curve indicator": "Block or Curve indicator",
        },
    )

    curve_name = "Energy Only Bid"

    df[curve_name + " Curve"] = extract_curve(
        df,
        curve_name,
        mw_suffix=" MW",
        price_suffix=" Price",
    )

    return df[DAM_ENERGY_BIDS_COLUMNS].sort_values(
        ["Interval Start", "Settlement Point Name"],
    )


def process_dam_ptp_obligation_option(df):
    df = df.rename(columns={"QSE Name": "QSE"})

    return df[DAM_PTP_OBLIGATION_OPTION_COLUMNS].sort_values(["Interval Start", "QSE"])


def process_dam_ptp_obligation_option_awards(df):
    df = df.rename(columns={"QSE Name": "QSE"})

    return df[DAM_PTP_OBLIGATION_OPTION_AWARDS_COLUMNS].sort_values(
        ["Interval Start", "QSE"],
    )


def process_sced_gen(df):
    time_cols = [
        "Interval Start",
        "Interval End",
        "SCED Timestamp",
    ]

    resource_cols = ["QSE", "DME", "Resource Name", "Resource Type"]

    telemetry_cols = [
        "Telemetered Resource Status",
        "Output Schedule",
        "HSL",
        "HASL",
        "HDL",
        "LSL",
        "LASL",
        "LDL",
        "Base Point",
        "Telemetered Net Output ",
    ]

    as_cols = [
        "Ancillary Service REGUP",
        "Ancillary Service REGDN",
        "Ancillary Service RRS",
        "Ancillary Service RRSFFR",
        "Ancillary Service NSRS",
        "Ancillary Service ECRS",
    ]

    tpo_cols = [
        "Start Up Cold Offer",
        "Start Up Hot Offer",
        "Start Up Inter Offer",
        "Min Gen Cost",
        "SCED TPO Offer Curve",
    ]

    sced1_offer_col = "SCED1 Offer Curve"

    df[sced1_offer_col] = extract_curve(df, "SCED1 Curve")
    df[tpo_cols[-1]] = extract_curve(df, "Submitted TPO")

    all_cols = resource_cols + telemetry_cols + as_cols + [sced1_offer_col] + tpo_cols

    for col in all_cols:
        if col not in df.columns:
            df[col] = np.nan

    df = df[time_cols + all_cols]

    # standardized to same naming as load
    # clean up column names
    df = df.rename(
        columns={
            "Ancillary Service RRS": "AS Responsibility for RRS",
            "Ancillary Service RRSFFR": "AS Responsibility for RRSFFR",
            "Ancillary Service NSRS": "AS Responsibility for NonSpin",
            "Ancillary Service REGUP": "AS Responsibility for RegUp",
            "Ancillary Service REGDN": "AS Responsibility for RegDown",
            "Ancillary Service ECRS": "AS Responsibility for ECRS",
            # remove space
            "Telemetered Net Output ": "Telemetered Net Output",
        },
    )

    return df


def process_sced_load(df):
    time_cols = [
        "Interval Start",
        "Interval End",
        "SCED Timestamp",
    ]

    resource_cols = ["QSE", "DME", "Resource Name"]

    telemetry_cols = [
        "Telemetered Resource Status",
        "Max Power Consumption",
        "Low Power Consumption",
        "Real Power Consumption",
        "HASL",
        "HDL",
        "LASL",
        "LDL",
        "Base Point",
    ]

    as_cols = [
        "AS Responsibility for RRS",
        "AS Responsibility for RRSFFR",
        "AS Responsibility for NonSpin",
        "AS Responsibility for RegUp",
        "AS Responsibility for RegDown",
        "AS Responsibility for ECRS",
    ]

    bid_curve_col = "SCED Bid to Buy Curve"

    df[bid_curve_col] = extract_curve(df, "SCED Bid to Buy Curve")

    all_cols = resource_cols + telemetry_cols + as_cols + [bid_curve_col]
    for col in all_cols:
        if col not in df.columns:
            df[col] = np.nan

    df = df[time_cols + all_cols]

    return df


# # backup for more node names
# pd.read_html("https://www.ercot.com/content/cdr/html/current_np6788.html", skiprows=3)[0][0] # noqa
# # todo add in QSE
# # todo prefix and county match
