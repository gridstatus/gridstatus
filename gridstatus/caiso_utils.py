import pandas as pd


def check_latest_value_time(df: pd.DataFrame, column: str):
    """Check if the latest value time is from the previous day and update the date accordingly

    Args:
        df (pd.DataFrame): DataFrame to check
        column (str): Column to check

    Returns:
        pd.Timestamp: Latest time
    """
    latest_time_str = df.loc[df[column].last_valid_index(), "Time"]
    latest_time = pd.Timestamp(latest_time_str, tz="US/Pacific")
    return latest_time
