import pandas as pd

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


def test_daily_spots_and_futures():
    eia = gridstatus.EIA(api_key="abcd")  # no need for API key to scrape.

    d = eia.get_daily_spots_and_futures()

    cols_petrol = [
        "product",
        "area",
        "price",
        "percent_change",
    ]

    assert d["df_petrol"].columns.tolist() == cols_petrol
    assert d["df_petrol"].shape[0] > 0
    cols_ng = [
        "region",
        "natural_gas_price",
        "natural_gas_percent_change",
        "electricity_price",
        "electricity_percent_change",
        "spark_spread",
    ]

    assert d["df_ng"].columns.tolist() == cols_ng
    assert d["df_ng"].shape[0] > 0


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
