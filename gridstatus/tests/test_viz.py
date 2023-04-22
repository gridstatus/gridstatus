import plotly
import pytest
import pandas as pd
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

# unit test 1 - valid df with numeric columns


def test_load_over_time_validdataframe():
    """
    Test for creation a line graph with a valid dataframe input containing numeric columns.
    """
    data = {
        "Time": pd.date_range(start="2021-01-01", periods=5, freq="D"),
        "Load": [100, 200, 150, 250, 300]
    }
    df = pd.DataFrame(data)
    fig = gridstatus.viz.load_over_time(df)
    assert isinstance(
        fig, plotly.graph_objs._figure.Figure), "Output is not a Plotly Figure instance"

# unit test 2 - test with an empty df


def test_load_over_time_emptydataframe():
    """
    Test returns None when an empty dataframe is provided.
    """
    df = pd.DataFrame()
    fig = gridstatus.viz.load_over_time(df)
    assert fig is None, "Output should be None when an empty dataframe is provided"

# unit test 3 - test df with only nonnumeric columns


def test_load_over_time_zeronumericcolumns():
    """
    Test func returns None when there are no numeric columns in the dataframe.
    """
    data = {
        "Time": pd.date_range(start="2021-01-01", periods=5, freq="D"),
        "Load": ["A", "B", "C", "D", "E"]
    }
    df = pd.DataFrame(data)
    fig = gridstatus.viz.load_over_time(df)
    assert fig is None, "Output should be None when no numeric columns are present"

# unit test 4: test df with only one numeric column


def test_load_over_time_onenumericcolumns():
    """
    Test creation of a line fig with one numeric columns in the df.
    """
    data = {
        "Time": pd.date_range(start="2021-01-01", periods=5, freq="D"),
        "Load": [100, 200, 150, 250, 300]
    }
    df = pd.DataFrame(data)
    fig = gridstatus.viz.load_over_time(df)
    assert isinstance(
        fig, plotly.graph_objs._figure.Figure), "Output is not a Plotly Figure instance"

# unit test 5:test df with two numeric columns


def test_load_over_time_twonumericcolumns():
    """
    Test func creates a line fig with two numeric columns in the dataframe.
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

# unit test 6: test df with three numeric columns


def test_load_over_time_threenumericcolumns():
    """
    Test func creates a line fig with three numeric columns in the dataframe.
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

# unit test 7: test case when iso string is provided


def test_load_over_time_withiso():
    """
    Test func creates a line fig with the correct title when an iso is provided.
    """
    data = {
        "Time": pd.date_range(start="2021-01-01", periods=5, freq="D"),
        "Load": [100, 200, 150, 250, 300]
    }
    df = pd.DataFrame(data)
    iso = "TEST_ISO"
    fig = gridstatus.viz.load_over_time(df, iso=iso)
    assert fig.layout.title.text == f"Load Over Time - {iso}", "Title is incorrect when ISO is provided"
