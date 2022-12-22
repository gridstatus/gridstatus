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
    # not the best test since we dont know if the viz is actually using it instead of time
    df["Hour"] = df["Time"].dt.hour
    fig = gridstatus.viz.dam_heat_map(df)
    assert isinstance(fig, plotly.graph_objs._figure.Figure)
