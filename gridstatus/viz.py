import plotly.express as px


def dam_heat_map(df):
    """Create a heat map of day-ahead location marginal prices.

    Args:
        df (pd.DataFrame): A DataFrame with columns "Time", "Location", and "LMP".
        If Hour is specified, it will be used as the x-axis. Otherwise, the hour ending will be used instead of Time


    Returns:
        plotly.graph_objects.Figure: A heat map of day-ahead location marginal prices.
    """

    if "Hour" not in df.columns:
        df["Hour"] = df["Time"].dt.hour

    date_str = df["Time"].dt.date[0].strftime("%m/%d/%Y")
    title = "Day-Ahead Location Marginal Prices on " + date_str + " ($/MWh)"

    heat_map_data = df.pivot(
        index="Location",
        columns="Hour",
        values="LMP",
    ).round(0)

    fig = px.imshow(
        heat_map_data,
        text_auto=True,
        title=title,
        template="plotly_dark",
    )
    fig.update_xaxes(
        title="Hour Ending",
        tickmode="array",
        tickvals=list(range(1, 25)),
    )
    fig.update_yaxes(title="")
    fig.update_layout(title_x=0.5)
    return fig
