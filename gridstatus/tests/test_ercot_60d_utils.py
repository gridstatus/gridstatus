import numpy as np
import pandas as pd

from gridstatus.ercot_60d_utils import process_sced_resource_as_offers


def _make_sced_resource_as_offers_df(rows):
    """Helper to build a DataFrame matching the raw SCED Resource AS Offers schema.

    Each row dict should contain at minimum "SCED Timestamp" and "Resource Name".
    Price columns (PRICE{n}_{suffix}) and QUANTITY_MW{n} columns are filled from
    the row dict, defaulting to 0 for any missing columns.
    """
    as_suffixes = ["URS", "DRS", "RRSPF", "RRSUF", "RRSFF", "NS", "ECRS"]
    n_blocks = 6

    all_cols = ["SCED Timestamp", "Resource Name"]
    for i in range(1, n_blocks + 1):
        all_cols.append(f"QUANTITY_MW{i}")
    for i in range(1, n_blocks + 1):
        for suffix in as_suffixes:
            all_cols.append(f"PRICE{i}_{suffix}")

    data = []
    for row in rows:
        record = {col: 0 for col in all_cols}
        record.update(row)
        data.append(record)

    return pd.DataFrame(data, columns=all_cols)


class TestProcessScedResourceAsOffers:
    """Tests for process_sced_resource_as_offers curve type detection."""

    def test_online_curve_type_with_zeros(self):
        """Online: has values in non-DRS columns (e.g. URS), zeros elsewhere."""
        df = _make_sced_resource_as_offers_df(
            [
                {
                    "SCED Timestamp": "2026-01-15 10:00",
                    "Resource Name": "GEN_A",
                    "QUANTITY_MW1": 100,
                    "PRICE1_URS": 25.0,
                    # All other PRICE columns default to 0
                },
            ],
        )
        result = process_sced_resource_as_offers(df)
        assert result["Curve Type"].iloc[0] == "Online"

    def test_online_curve_type_with_nans(self):
        """Online: has values in non-DRS columns, NaN elsewhere.

        This tests the fix for ERCOT notice M-B040326-01 where corrected files
        use NaN instead of zero for empty AS Sub-Type Offer Prices.
        """
        df = _make_sced_resource_as_offers_df(
            [
                {
                    "SCED Timestamp": "2026-01-15 10:00",
                    "Resource Name": "GEN_A",
                    "QUANTITY_MW1": 100,
                    "PRICE1_URS": 25.0,
                },
            ],
        )
        # Replace zeros with NaN in all price columns (simulating corrected files)
        price_cols = [c for c in df.columns if c.startswith("PRICE")]
        df[price_cols] = df[price_cols].replace(0, np.nan)

        result = process_sced_resource_as_offers(df)
        assert result["Curve Type"].iloc[0] == "Online"

    def test_regulation_down_curve_type_with_zeros(self):
        """Regulation Down: has values only in DRS columns, zeros elsewhere."""
        df = _make_sced_resource_as_offers_df(
            [
                {
                    "SCED Timestamp": "2026-01-15 10:00",
                    "Resource Name": "GEN_B",
                    "QUANTITY_MW1": 50,
                    "PRICE1_DRS": 10.0,
                    # All other PRICE columns default to 0
                },
            ],
        )
        result = process_sced_resource_as_offers(df)
        assert result["Curve Type"].iloc[0] == "Regulation Down"

    def test_regulation_down_curve_type_with_nans(self):
        """Regulation Down: has values only in DRS columns, NaN elsewhere."""
        df = _make_sced_resource_as_offers_df(
            [
                {
                    "SCED Timestamp": "2026-01-15 10:00",
                    "Resource Name": "GEN_B",
                    "QUANTITY_MW1": 50,
                    "PRICE1_DRS": 10.0,
                },
            ],
        )
        price_cols = [c for c in df.columns if c.startswith("PRICE")]
        non_drs_price_cols = [c for c in price_cols if not c.endswith("_DRS")]
        df[non_drs_price_cols] = df[non_drs_price_cols].replace(0, np.nan)

        result = process_sced_resource_as_offers(df)
        assert result["Curve Type"].iloc[0] == "Regulation Down"

    def test_offline_curve_type_with_zeros(self):
        """Offline: has values in NS (and optionally ECRS), zeros elsewhere."""
        df = _make_sced_resource_as_offers_df(
            [
                {
                    "SCED Timestamp": "2026-01-15 10:00",
                    "Resource Name": "GEN_C",
                    "QUANTITY_MW1": 75,
                    "PRICE1_NS": 15.0,
                    # All other PRICE columns default to 0
                },
            ],
        )
        result = process_sced_resource_as_offers(df)
        assert result["Curve Type"].iloc[0] == "Offline"

    def test_offline_curve_type_with_nans(self):
        """Offline: has values in NS columns, NaN elsewhere."""
        df = _make_sced_resource_as_offers_df(
            [
                {
                    "SCED Timestamp": "2026-01-15 10:00",
                    "Resource Name": "GEN_C",
                    "QUANTITY_MW1": 75,
                    "PRICE1_NS": 15.0,
                },
            ],
        )
        price_cols = [c for c in df.columns if c.startswith("PRICE")]
        non_ns_price_cols = [c for c in price_cols if not c.endswith("_NS")]
        df[non_ns_price_cols] = df[non_ns_price_cols].replace(0, np.nan)

        result = process_sced_resource_as_offers(df)
        assert result["Curve Type"].iloc[0] == "Offline"

    def test_mixed_curve_types_with_nans(self):
        """Multiple rows with different curve types, all using NaN for empty prices."""
        df = _make_sced_resource_as_offers_df(
            [
                {
                    "SCED Timestamp": "2026-01-15 10:00",
                    "Resource Name": "GEN_ONLINE",
                    "QUANTITY_MW1": 100,
                    "PRICE1_URS": 25.0,
                },
                {
                    "SCED Timestamp": "2026-01-15 10:00",
                    "Resource Name": "GEN_REGDOWN",
                    "QUANTITY_MW1": 50,
                    "PRICE1_DRS": 10.0,
                },
                {
                    "SCED Timestamp": "2026-01-15 10:00",
                    "Resource Name": "GEN_OFFLINE",
                    "QUANTITY_MW1": 75,
                    "PRICE1_NS": 15.0,
                },
            ],
        )
        # Replace all zeros with NaN in price columns
        price_cols = [c for c in df.columns if c.startswith("PRICE")]
        df[price_cols] = df[price_cols].replace(0, np.nan)

        result = process_sced_resource_as_offers(df)
        assert list(result["Curve Type"]) == ["Online", "Regulation Down", "Offline"]

    def test_offer_curves_with_nans_exclude_nan_pairs(self):
        """Verify that offer curve extraction skips NaN price entries."""
        df = _make_sced_resource_as_offers_df(
            [
                {
                    "SCED Timestamp": "2026-01-15 10:00",
                    "Resource Name": "GEN_A",
                    "QUANTITY_MW1": 100,
                    "QUANTITY_MW2": 200,
                    "PRICE1_URS": 25.0,
                    "PRICE2_URS": 50.0,
                },
            ],
        )
        # Replace zeros with NaN in price columns (non-URS prices become NaN)
        price_cols = [c for c in df.columns if c.startswith("PRICE")]
        df[price_cols] = df[price_cols].replace(0, np.nan)

        result = process_sced_resource_as_offers(df)

        # URS curve should have the two valid pairs
        urs_curve = result["URS Offer Curve"].iloc[0]
        assert len(urs_curve) == 2
        assert urs_curve[0] == [100.0, 25.0]
        assert urs_curve[1] == [200.0, 50.0]

        # DRS curve should be None (all prices are NaN)
        assert result["DRS Offer Curve"].iloc[0] is None

    def test_output_columns(self):
        """Verify output has the expected columns."""
        df = _make_sced_resource_as_offers_df(
            [
                {
                    "SCED Timestamp": "2026-01-15 10:00",
                    "Resource Name": "GEN_A",
                    "QUANTITY_MW1": 100,
                    "PRICE1_URS": 25.0,
                },
            ],
        )
        result = process_sced_resource_as_offers(df)
        expected_cols = [
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
        assert list(result.columns) == expected_cols
