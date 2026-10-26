import pandas as pd

from gridstatus.isone_constants import ISONE_OFFER_SEGMENT_COLUMNS


def build_offer_curve(df: pd.DataFrame) -> pd.Series:
    """Build [MW, Price] pairs in segment order, preserving source precision.

    Entirely missing segments are omitted; a partially missing pair is invalid.
    An offer without reported segments has an empty curve. MW is not accumulated.
    """
    values = df[ISONE_OFFER_SEGMENT_COLUMNS].apply(pd.to_numeric, errors="raise")
    curves = []
    for row in values.itertuples(index=False, name=None):
        curve = []
        for price, mw in zip(row[::2], row[1::2]):
            if pd.isna(price) and pd.isna(mw):
                continue
            if pd.isna(price) or pd.isna(mw):
                raise ValueError("Offer segment must contain both MW and Price")
            curve.append([float(mw), float(price)])
        curves.append(curve)
    return pd.Series(curves, index=df.index, dtype="object")
