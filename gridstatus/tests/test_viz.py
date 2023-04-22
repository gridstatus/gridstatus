import pandas as pd
import plotly

import gridstatus


def test_dam_heat_map():
    iso = gridstatus.CAISO()

    df = iso.get_lmp(
        date="today",  # you can change to desired date
        market="DAY_AHEAD_HOURLY",
        locations=[
            "TH_NP15_GEN-APND",
            "TH_SP15_GEN-APND",
            "TH_ZP26_GEN-APND",
        ],  # you can change to desired locations
    )

    fig = gridstatus.viz.dam_heat_map(df)
    assert isinstance(fig, plotly.graph_objs._figure.Figure)

    # check if works with hour too
    # not the best test since we dont know if
    # the viz is actually using it instead of time
    df["Hour"] = df["Time"].dt.hour
    fig = gridstatus.viz.dam_heat_map(df)
    assert isinstance(fig, plotly.graph_objs._figure.Figure)

# uncomment to skipped test, should be fixed now with new changes to load_over_time function
# @pytest.mark.skip(reason="Failed. TODO Fix")


def test_load_over_time():

    iso = gridstatus.CAISO()

    df = iso.get_load("today")

    fig = gridstatus.viz.load_over_time(df)
    assert isinstance(fig, plotly.graph_objs._figure.Figure)

    fig = gridstatus.viz.load_over_time(df, iso="CAISO")
    assert isinstance(fig, plotly.graph_objs._figure.Figure)


# unit test 1 - test with an empty df


def test_load_over_time_emptydataframe():
    """
    Test returns None when an empty dataframe is provided.
    """
    df = pd.DataFrame()
    fig = gridstatus.viz.load_over_time(df)
    assert fig is None, "Output should be None when an empty dataframe is provided"

# unit test 2 - test df with only nonnumeric columns


def test_load_over_time_zeronumericcolumns():
    """
    Test returns None if no numeric columns in df.
    """
    data = {
        "Time": pd.date_range(start="2021-01-01", periods=5, freq="D"),
        "Load": ["A", "B", "C", "D", "E"]
    }
    df = pd.DataFrame(data)
    fig = gridstatus.viz.load_over_time(df)
    assert fig is None, "Output should be None when no numeric columns are present"

# unit test 3: test df with only one numeric column


def test_load_over_time_onenumericcolumns():
    """
    Test line fig for df with one numeric column.
    """
    data = {
        "Time": pd.date_range(start="2021-01-01", periods=5, freq="D"),
        "Load": [100, 200, 150, 250, 300]
    }
    df = pd.DataFrame(data)
    fig = gridstatus.viz.load_over_time(df)
    assert isinstance(
        fig, plotly.graph_objs._figure.Figure), "Output is not a Plotly Figure instance"

# unit test 4:test df with two numeric columns


def test_load_over_time_twonumericcolumns():
    """
    Test line fig for df with two numeric columns.
    """
    data = {
        "Time": pd.date_range(start="2021-01-01", periods=5, freq="D"),
        "Load": [100, 200, 150, 250, 300],
        "Load2": [50, 100, 75, 125, 150]
    }
    df = pd.DataFrame(data)
    fig = gridstatus.viz.load_over_time(df)
    assert isinstance(
        fig, plotly.graph_objs._figure.Figure), "Output is not a Plotly Figure instance"

# unit test 5: test df with three numeric columns


def test_load_over_time_threenumericcolumns():
    """
    Test line fig for df with three numeric columns.
    """
    data = {
        "Time": pd.date_range(start="2021-01-01", periods=5, freq="D"),
        "Load": [100, 200, 150, 250, 300],
        "Load2": [50, 100, 75, 125, 150],
        "Load3": [10, 30, 60, 40, 20]
    }
    df = pd.DataFrame(data)
    fig = gridstatus.viz.load_over_time(df)
    assert isinstance(
        fig, plotly.graph_objs._figure.Figure), "Output is not a Plotly Figure instance"

# unit test 6: test case when iso string is provided


def test_load_over_time_withiso():
    """
    Test line fig with correct title if iso provided.
    """
    data = {
        "Time": pd.date_range(start="2021-01-01", periods=5, freq="D"),
        "Load": [100, 200, 150, 250, 300]
    }
    df = pd.DataFrame(data)
    iso = "TEST_ISO"
    fig = gridstatus.viz.load_over_time(df, iso=iso)
    test_title = f"Load Over Time - {iso}", "Title not correct when ISO provided"
    assert fig.layout.title.text == test_title
