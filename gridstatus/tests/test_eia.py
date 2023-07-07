import pandas as pd
import pytest

import gridstatus


def _check_interchange(df):
    columns = [
        "Interval Start",
        "Interval End",
        "From BA",
        "From BA Name",
        "To BA",
        "To BA Name",
        "MW",
    ]
    # assert interval start and interval end are datetimes in utc
    assert df["Interval Start"].dtype == "datetime64[ns, UTC]"
    assert df["Interval End"].dtype == "datetime64[ns, UTC]"
    assert df.shape[0] > 0
    assert df.columns.tolist() == columns


def _check_region_data(df):
    columns = [
        "Interval Start",
        "Interval End",
        "Respondent",
        "Respondent Name",
        "Load",
        "Load Forecast",
        "Net Generation",
        "Total Interchange",
    ]

    assert df["Interval Start"].dtype == "datetime64[ns, UTC]"
    assert df["Interval End"].dtype == "datetime64[ns, UTC]"
    assert df.shape[0] > 0
    assert df.columns.tolist() == columns


def test_rto_interchange():
    eia = gridstatus.EIA()

    start = "2020-01-01"
    end = "2020-01-04"

    df = eia.get_dataset(
        dataset="electricity/rto/interchange-data",
        start=start,
        end=end,
        verbose=True,
    )

    assert df["Interval End"].min().date() == pd.Timestamp(start).date()
    assert df["Interval End"].max().date() == pd.Timestamp(end).date()

    _check_interchange(df)


def test_rto_region_data():
    eia = gridstatus.EIA()
    start = "2020-01-01"
    end = "2020-01-04"
    df = eia.get_dataset(
        dataset="electricity/rto/region-data",
        start=start,
        end=end,
        verbose=True,
    )

    assert df["Interval End"].min().date() == pd.Timestamp(start).date()
    assert df["Interval End"].max().date() == pd.Timestamp(end).date()
    _check_region_data(df)


def test_list_routes():
    eia = gridstatus.EIA()

    routes = eia.list_routes("electricity/rto/")

    assert "interchange-data" in [r["id"] for r in routes["routes"]]


def test_other_dataset():
    eia = gridstatus.EIA()

    start = "2020-01-01"
    end = "2020-01-04"

    # dataset that doesnt have a handler yet
    df = eia.get_dataset(
        dataset="electricity/rto/fuel-type-data",
        start=start,
        end=end,
        verbose=True,
    )

    cols = [
        "period",
        "respondent",
        "respondent-name",
        "fueltype",
        "type-name",
        "value",
        "value-units",
    ]

    assert df.columns.tolist() == cols
    assert df.shape[0] > 0


@pytest.mark.slow
def test_eia_grid_monitor():
    eia = gridstatus.EIA()
    cols = [
        "Area Id",
        "Area Name",
        "Area Type",
        "Interval Start",
        "Interval End",
        "Demand",
        "Demand Forecast",
        "Net Generation",
        "Total Interchange",
        "NG: COL",
        "NG: NG",
        "NG: NUC",
        "NG: OIL",
        "NG: WAT",
        "NG: SUN",
        "NG: WND",
        "NG: UNK",
        "NG: OTH",
        "Positive Gen from COL",
        "Positive Gen from NG",
        "Positive Gen from OIL",
        "Positive Generation",
        "Consumed Electricity",
        "CO2 Factor: COL",
        "CO2 Factor: NG",
        "CO2 Factor: OIL",
        "CO2 Emissions: COL",
        "CO2 Emissions: NG",
        "CO2 Emissions: OIL",
        "CO2 Emissions: Other",
        "CO2 Emissions Generated",
        "CO2 Emissions Imported",
        "CO2 Emissions Exported",
        "CO2 Emissions Consumed",
    ]
    df = eia.get_grid_monitor()

    assert df.columns.tolist() == cols
