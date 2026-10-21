ISONE_OFFER_ID_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Masked Lead Participant ID",
    "Masked Asset ID",
]

ISONE_OFFER_SEGMENT_COLUMNS = [
    f"Segment {number} {field}" for number in range(1, 11) for field in ["Price", "MW"]
]

ISONE_ANCILLARY_SERVICES_ENERGY_OFFERS_COLUMNS = ISONE_OFFER_ID_COLUMNS + [
    "TMSR Price",
    "TMNSR Price",
    "TMOR Price",
    "EIR Price",
    "Offer Max MW",
]

ISONE_ANCILLARY_SERVICES_ARD_OFFERS_COLUMNS = ISONE_OFFER_ID_COLUMNS + [
    "TMSR Price",
    "TMOR Price",
    "Offer Max MW",
]

ISONE_ANCILLARY_SERVICES_DRR_OFFERS_COLUMNS = (
    ISONE_ANCILLARY_SERVICES_ENERGY_OFFERS_COLUMNS.copy()
)

ISONE_ARD_BIDS_COLUMNS = (
    ISONE_OFFER_ID_COLUMNS
    + ["Claim 10", "Claim 30", "Minimum Consumption", "Maximum Consumption"]
    + ["ARD Bid Curve"]
    + ["Unit Status", "Max Daily Award Limit"]
)

ISONE_DRR_OFFERS_COLUMNS = (
    ISONE_OFFER_ID_COLUMNS
    + [
        "Max Daily Energy",
        "Minimum Reduction",
        "Maximum Reduction",
        "Interruption Cost",
    ]
    + ["DRR Offer Curve"]
    + ["Claim 10", "Claim 30", "Unit Status", "Max Daily Award Limit"]
)

ISONE_ENERGY_OFFERS_COLUMNS = (
    ISONE_OFFER_ID_COLUMNS
    + [
        "Must Take Energy",
        "Maximum Daily Energy Available",
        "Economic Maximum",
        "Economic Minimum",
        "Cold Startup Price",
        "Intermediate Startup Price",
        "Hot Startup Price",
        "No Load Price",
    ]
    + ["Energy Offer Curve"]
    + ["Claim 10", "Claim 30", "Unit Status", "Max Daily Award Limit"]
)

ISONE_IMPORT_OFFERS_EXPORT_BIDS_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Market Type",
    "Masked Customer ID",
    "Masked Originating Location ID",
    "Masked Destination Location ID",
    "Emergency Flag",
    "Direction",
    "Transaction Type",
    "Price",
    "Bid MW",
]
