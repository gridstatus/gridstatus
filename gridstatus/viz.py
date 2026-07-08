import plotly.express as px
import polars as pl


def dam_heat_map(df: pl.DataFrame):
    """Create a heat map of day-ahead location marginal prices.

    Arguments:
        df (polars.DataFrame): A DataFrame with columns "Time", "Location", and "LMP".
            If Hour is specified, it will be used as the x-axis.
            Otherwise, the hour ending will be used instead of Time


    Returns:
        plotly.graph_objects.Figure: A heat map of day-ahead location marginal prices.
    """

    if "Hour" not in df.columns:
        df = df.with_columns(pl.col("Time").dt.hour().alias("Hour"))

    date_str = df["Time"].dt.date()[0].strftime("%m/%d/%Y")
    title = "Day-Ahead Location Marginal Prices on " + date_str + " ($/MWh)"

    heat_map_data = (
        df.pivot(on="Hour", index="Location", values="LMP")
        .sort("Location")
        .with_columns(pl.exclude("Location").round(0))
    )

    hours = [c for c in heat_map_data.columns if c != "Location"]

    fig = px.imshow(
        heat_map_data.select(hours).to_numpy(),
        x=[int(h) for h in hours],
        y=heat_map_data["Location"].to_list(),
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


def load_over_time(df: pl.DataFrame, iso: str | None = None):
    """Create a line graph of load dataframe"""
    y = "Load"
    if len(df.columns) > 3:
        y = df.columns[2:]

    title = "Load Over Time"
    if iso:
        title += " - " + iso

    fig = px.line(
        df,
        x="Time",
        y=y,
        title=title,
    )
    # show legend
    fig.update_layout(
        legend=dict(
            orientation="h",
            title_text=None,
            y=-0.2,
        ),
    )
    fig.update_yaxes(title_text="MW")

    return fig
