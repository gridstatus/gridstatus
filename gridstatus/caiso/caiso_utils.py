import pandas as pd
import polars as pl


# NOTE: Currently the gridstatus.CAISO.default_timezone is in the caiso.py file, which imports caiso_utils.py and thus
# it can't be set to the value without causing a circular import, which is why they are hardcoded here.
def make_timestamp(
    time_str: str,
    today: pd.Timestamp,
    timezone: str = "US/Pacific",
) -> pd.Timestamp:
    hour, minute = map(int, time_str.split(":"))
    ts = pd.Timestamp(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=hour,
        minute=minute,
    )
    ts = ts.tz_localize(timezone, ambiguous=True)
    return ts


def check_latest_value_time(df: pl.DataFrame, column: str) -> pd.Timestamp:
    """Check if the latest value time is from the previous day and update the date accordingly

    Args:
        df (pl.DataFrame): DataFrame to check
        column (str): Column to check

    Returns:
        pd.Timestamp: Latest time
    """
    current_local_date = pd.Timestamp.now(tz="US/Pacific").date()
    non_null = df.filter(pl.col(column).is_not_null())
    latest_time_str = non_null[-1, "Time"]
    latest_time = make_timestamp(
        latest_time_str,
        today=current_local_date,
        timezone="US/Pacific",
    )
    return latest_time
