import plotly.express as px


def dam_heatmap(df):
    """Create a heatmap of day-ahead location marginal prices.

    Args:
        df (pd.DataFrame): A DataFrame with columns "Time", "Location", and "LMP".

    Returns:
        plotly.graph_objects.Figure: A heatmap of day-ahead location marginal prices.
    """

    date_str = df["Time"].dt.date[0].strftime("%m/%d/%Y")
    title = "Day-Ahead Location Marginal Prices on " + date_str + " ($/MWh)"

    heatmap_data = df.pivot(
        index="Location",
        columns="Hour",
        values="LMP",
    ).round(0)

    fig = px.imshow(
        heatmap_data,
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
