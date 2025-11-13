import pandas as pd
import plotly.express as px


def dam_heat_map(df):
    """Create a heat map of day-ahead location marginal prices.

    Arguments:
        df (pandas.DataFrame): A DataFrame with columns "Time", "Location", and "LMP".
            If Hour is specified, it will be used as the x-axis.
            Otherwise, the hour ending will be used instead of Time


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


def load_over_time(df, iso=None):
    """Create a line graph of load dataframe"""
    y = "Load"
    if len(df.columns) > 3:
        y = df.columns[2:]

    title = "Load Over Time"
    if iso:
        title += " - " + iso

    fig = px.line(
        df,
        x=df["Time"],
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


def show_figure(fig, filename, source=None, width=1200, height=800, scale=1.5):
    """
    Show and save a plotly figure.

    Args:
        fig (plotly.graph_objs.Figure): The plotly figure to display and save.
        filename (str): The name of the file to save the figure as.
        source (str, optional): The source to display in
            the bottom-right corner of the figure.
        width (int, optional): The width of the saved image. Defaults to 1200.
        height (int, optional): The height of the saved image. Defaults to 800.
        scale (float, optional): The scale factor for the saved image. Defaults to 1.5.

    Returns:
        plotly.graph_objs.Figure: The displayed and saved figure.
    """
    if source:
        fig.add_annotation(
            x=1,
            y=-0.15,
            xref="paper",
            yref="paper",
            text=f"Source: {source}",
            showarrow=False,
            font=dict(size=12, color="white"),
            align="right",
            xanchor="right",
            yanchor="bottom",
        )

    fig.update_layout(template="plotly_dark+presentation")
    fig.write_image(filename, width=width, height=height, scale=scale)
    return fig


def time_series_peak(
    df,
    time_col,
    series_col,
    resample_freq="W",
    title=None,
    ylabel=None,
    width=1200,
    height=800,
    scale=1.5,
    unit="GW",
    annotation_time_fmt="%Y-%m-%d %H:%M",
    filename=None,
    source=None,
):
    """
    Plot and save the time series peak values.

    Args:
        df (pandas.DataFrame): The input dataframe.
        time_col (str): The name of the column containing time values.
        series_col (str): The name of the column containing series values.
        resample_freq (str, optional): The frequency to resample
            the time series. Defaults to 'W'.
        title (str, optional): The title of the plot.
        ylabel (str, optional): The label for the y-axis.
        width (int, optional): The width of the saved image.
            Defaults to 1200.
        height (int, optional): The height of the saved image.
            Defaults to 800.
        scale (float, optional): The scale factor for the saved image.
            Defaults to 1.5.
        unit (str, optional): The unit of measurement for the series values.
             Defaults to 'GW'.
        annotation_time_fmt (str, optional): The format
            for displaying time in annotations.
            Defaults to '%Y-%m-%d %H:%M'.
        filename (str, optional): The name of the file to save the figure as.
        source (str, optional): The source to display in the
          bottom-right corner of the figure.

    Returns:
        plotly.graph_objs.Figure: The displayed and saved figure.
    """
    df = (
        df.groupby(pd.Grouper(key=time_col, freq=resample_freq))
        .apply(lambda x: x.loc[x[series_col].idxmax()])
        .reset_index(drop=True)
    )

    df = df.sort_values(by=time_col)
    df[f"cum_peak_{series_col}"] = df[series_col].cummax()

    fig = px.line(
        df,
        y=[f"cum_peak_{series_col}", series_col],
        x=time_col,
        title=title,
    )

    fig.update_traces(line=dict(width=3))
    fig.update_traces(
        line=dict(dash="dash"),
        selector=dict(name=f"cum_peak_{series_col}"),
    )

    fig.update_layout(legend_title_text=None)
    fig.update_traces(name="Peak Value", selector=dict(name=series_col))
    fig.update_traces(
        name="Previous Record Peak",
        selector=dict(name=f"cum_peak_{series_col}"),
    )

    fig.update_layout(legend=dict(x=0.3, y=-0.1, orientation="h"))
    fig.update_xaxes(title_text=None)
    fig.update_yaxes(title_text=ylabel)

    fig.update_xaxes(
        range=[df[time_col].min(), df[time_col].max() + pd.Timedelta(days=7)],
    )

    peaks = df[df[series_col] == df[f"cum_peak_{series_col}"]]
    peaks = (
        peaks.groupby(pd.Grouper(key=time_col, freq="Y"))
        .apply(lambda x: x.loc[x[series_col].idxmax()])
        .reset_index(drop=True)
    )
    for i, row in peaks.iterrows():
        fig.add_annotation(
            x=row[time_col],
            y=row[series_col],
            text=f"{row[series_col]:.2f} {unit}<br>{row[time_col].strftime(annotation_time_fmt)}",  # noqa: E501
            showarrow=True,
            arrowhead=1,
            ax=0,
            ay=-40,
        )

    if not filename:
        filename = title.replace(" ", "_").lower() + ".png"

    return show_figure(fig, filename, source, width=width, height=height, scale=scale)
