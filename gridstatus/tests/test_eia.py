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


def _check_fuel_type(df):
    columns = [
        "Interval Start",
        "Interval End",
        "Respondent",
        "Respondent Name",
        "Coal",
        "Hydro",
        "Natural gas",
        "Nuclear",
        "Other",
        "Petroleum",
        "Solar",
        "Wind",
    ]

    assert df["Interval Start"].dtype == "datetime64[ns, UTC]"
    assert df["Interval End"].dtype == "datetime64[ns, UTC]"
    assert df.shape[0] > 0
    assert df.columns.tolist() == columns


def test_list_routes():
    eia = gridstatus.EIA()

    routes = eia.list_routes("electricity/rto/")

    assert "interchange-data" in [r["id"] for r in routes["routes"]]


def test_rto_interchange():
    eia = gridstatus.EIA()

    start = "2020-01-01"
    end = "2020-01-03"

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


def test_fuel_type():
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

    _check_fuel_type(df)


def test_daily_spots_and_futures():
    eia = gridstatus.EIA(api_key="abcd")  # no need for API key to scrape.

    d = eia.get_daily_spots_and_futures()

    cols_petrol = [
        "date",
        "product",
        "area",
        "price",
        "percent_change",
    ]

    assert d["petroleum"].columns.tolist() == cols_petrol
    assert d["petroleum"].shape[0] > 0
    cols_ng = [
        "date",
        "region",
        "natural_gas_price",
        "natural_gas_percent_change",
        "electricity_price",
        "electricity_percent_change",
        "spark_spread",
    ]

    assert d["natural_gas"].columns.tolist() == cols_ng
    assert d["natural_gas"].shape[0] > 0


def test_get_coal_spots():
    eia = gridstatus.EIA(api_key="abcd")  # no need for API key to scrape.

    d = eia.get_coal_spots()

    cols_spot_price = [
        "week_ending_date",
        "central_appalachia_price_short_ton",
        "northern_appalachia_price_short_ton",
        "illinois_basin_price_short_ton",
        "powder_river_basin_price_short_ton",
        "uinta_basin_price_short_ton",
        "central_appalachia_price_mmbtu",
        "northern_appalachia_price_mmbtu",
        "illinois_basin_price_mmbtu",
        "powder_river_basin_price_mmbtu",
        "uinta_basin_price_mmbtu",
    ]

    cols_coal = [
        "delivery_month",
        "coal_min",
        "coal_max",
        "coal_exports",
    ]

    cols_coke = [
        "delivery_month",
        "coke_min",
        "coke_max",
        "coke_exports",
    ]

    assert d["weekly_spots"].columns.tolist() == cols_spot_price
    assert d["weekly_spots"].shape[0] > 0

    assert d["coal_exports"].columns.tolist() == cols_coal
    assert d["coal_exports"].shape[0] > 0

    assert d["coke_exports"].columns.tolist() == cols_coke
    assert d["coke_exports"].shape[0] > 0


@pytest.mark.slow
def test_eia_grid_monitor():
    eia = gridstatus.EIA()
    cols = [
        "Interval Start",
        "Interval End",
        "Area Id",
        "Area Name",
        "Area Type",
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
        "CO2 Emissions Intensity for Generated Electricity",
        "CO2 Emissions Intensity for Consumed Electricity",
    ]
    df = eia.get_grid_monitor(area_id="CISO")

    assert df.columns.tolist() == cols
