import pandas as pd


# NOTE: Currently the gridstatus.CAISO.default_timezone is in the caiso.py file, which imports caiso_utils.py and thus
# it can't be set to the value without causing a circular import, which is why they are hardcoded here.
def make_timestamp(time_str: str, today: pd.Timestamp, timezone: str = "US/Pacific"):
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


def check_latest_value_time(df: pd.DataFrame, column: str):
    """Check if the latest value time is from the previous day and update the date accordingly

    Args:
        df (pd.DataFrame): DataFrame to check
        column (str): Column to check

    Returns:
        pd.Timestamp: Latest time
    """
    current_local_date = pd.Timestamp.now(tz="US/Pacific").date()
    latest_time_str = df.loc[df[column].last_valid_index(), "Time"]
    latest_time = make_timestamp(
        latest_time_str,
        today=current_local_date,
        timezone="US/Pacific",
    )
    return latest_time
