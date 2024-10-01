import pandas as pd


def check_latest_value_time(df: pd.DataFrame, file: str):
    """Check the latest value time in the DataFrame"""
    value_col_map = {
        "demand": "Current demand",
        "fuelsource": "Solar",
        "storage": "Total batteries",
    }
    value_col = value_col_map[file]
    latest_time_str = df.loc[df[value_col].last_valid_index(), "Time"]
    latest_time = pd.Timestamp(latest_time_str, tz="US/Pacific")
    return latest_time
