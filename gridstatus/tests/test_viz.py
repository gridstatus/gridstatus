import pandas as pd
import plotly
import pytest

import gridstatus


@pytest.mark.parametrize("explicit_hour", [False, True])
def test_dam_heat_map(explicit_hour):
    times = pd.date_range("2026-01-15", periods=2, freq="h", tz="US/Pacific")
    locations = ["TH_NP15_GEN-APND", "TH_SP15_GEN-APND", "TH_ZP26_GEN-APND"]
    df = pd.DataFrame(
        {
            "Time": list(times) * 3,
            "Location": [location for location in locations for _ in times],
            "LMP": [10.25, 20.75, -5.25, 0.75, 30.25, 40.75],
        },
    )
    if explicit_hour:
        # Deliberately differ from Time to verify that Hour takes precedence.
        df["Hour"] = [11, 12] * 3

    fig = gridstatus.viz.dam_heat_map(df)
    assert isinstance(fig, plotly.graph_objs._figure.Figure)
    assert list(fig.data[0].x) == ([11, 12] if explicit_hour else [0, 1])
    assert list(fig.data[0].y) == locations
    assert fig.data[0].z.tolist() == [[10, 21], [-5, 1], [30, 41]]
    assert "01/15/2026" in fig.layout.title.text


@pytest.mark.skip(reason="Failed. TODO Fix")
def test_load_over_time():
    iso = gridstatus.CAISO()

    df = iso.get_load("today")

    fig = gridstatus.viz.load_over_time(df)
    assert isinstance(fig, plotly.graph_objs._figure.Figure)

    fig = gridstatus.viz.load_over_time(df, iso="CAISO")
    assert isinstance(fig, plotly.graph_objs._figure.Figure)
