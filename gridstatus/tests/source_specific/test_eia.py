import datetime
from typing import List

import pandas as pd
import polars as pl
import pytest

import gridstatus
from gridstatus.eia import EIA, HENRY_HUB_TIMEZONE
from gridstatus.eia_constants import (
    CANCELED_OR_POSTPONED_GENERATOR_COLUMNS,
    EIA_FUEL_MIX_COLUMNS,
    GENERATOR_FLOAT_COLUMNS,
    GENERATOR_INT_COLUMNS,
    OPERATING_GENERATOR_COLUMNS,
    PLANNED_GENERATOR_COLUMNS,
    RETIRED_GENERATOR_COLUMNS,
)
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

api_vcr = setup_vcr(
    source="eia",
    record_mode=RECORD_MODE,
)


def _null_count(df: pl.DataFrame) -> int:
    return df.null_count().select(pl.sum_horizontal(pl.all())).item()


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
    assert isinstance(df.schema["Interval Start"], pl.Datetime)
    assert df.schema["Interval Start"].time_zone == "UTC"
    assert isinstance(df.schema["Interval End"], pl.Datetime)
    assert df.schema["Interval End"].time_zone == "UTC"
    assert df.height > 0
    assert df.columns == columns


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

    assert isinstance(df.schema["Interval Start"], pl.Datetime)
    assert df.schema["Interval Start"].time_zone == "UTC"
    assert isinstance(df.schema["Interval End"], pl.Datetime)
    assert df.schema["Interval End"].time_zone == "UTC"
    assert df.height > 0
    assert df.columns == columns


def _check_region_subba_data(df):
    columns = [
        "Interval Start",
        "Interval End",
        "BA",
        "BA Name",
        "Subregion",
        "Subregion Name",
        "MW",
    ]

    assert isinstance(df.schema["Interval Start"], pl.Datetime)
    assert df.schema["Interval Start"].time_zone == "UTC"
    assert isinstance(df.schema["Interval End"], pl.Datetime)
    assert df.schema["Interval End"].time_zone == "UTC"
    assert df.height > 0
    assert df.columns == columns


def _check_fuel_type(df):
    assert isinstance(df.schema["Interval Start"], pl.Datetime)
    assert df.schema["Interval Start"].time_zone == "UTC"
    assert isinstance(df.schema["Interval End"], pl.Datetime)
    assert df.schema["Interval End"].time_zone == "UTC"
    assert df.height > 0
    assert df.columns == EIA_FUEL_MIX_COLUMNS


@pytest.mark.integration
def test_list_routes():
    eia = gridstatus.EIA()

    routes = eia.list_routes("electricity/rto/")

    assert "interchange-data" in [r["id"] for r in routes["routes"]]


@pytest.mark.integration
def test_list_facets():
    eia = gridstatus.EIA()

    facets = eia.list_facets("electricity/rto/region-data")

    assert "type" in facets.keys()


@pytest.mark.integration
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
    assert _null_count(df) == 0

    _check_interchange(df)


@pytest.mark.integration
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
    assert _null_count(df.filter(pl.col("Respondent") == "BPAT")) == 0
    _check_region_data(df)


@pytest.mark.integration
def test_rto_region_subba_data():
    eia = gridstatus.EIA()
    start = "2020-01-01"
    end = "2020-01-04"

    df = eia.get_dataset(
        dataset="electricity/rto/region-sub-ba-data",
        start=start,
        end=end,
        verbose=True,
    )

    assert df["Interval End"].min().date() == pd.Timestamp(start).date()
    assert df["Interval End"].max().date() == pd.Timestamp(end).date()
    assert _null_count(df.filter(pl.col("Subregion") == "PGAE")) == 0
    _check_region_subba_data(df)


@pytest.mark.integration
def test_fuel_type():
    eia = gridstatus.EIA()

    start = (pd.Timestamp.utcnow() - pd.Timedelta(days=2)).normalize()
    end = start + pd.Timedelta(days=1)

    df = eia.get_dataset(
        dataset="electricity/rto/fuel-type-data",
        start=start,
        end=end,
        verbose=True,
    )

    assert df["Interval End"].min() == start
    assert df["Interval End"].max() == end

    _check_fuel_type(df)


@pytest.mark.integration
def test_facets():
    eia = gridstatus.EIA()

    start = "2025-01-01"
    end = "2025-01-04"

    df = eia.get_dataset(
        dataset="electricity/rto/fuel-type-data",
        start=start,
        end=end,
        verbose=True,
        facets={"respondent": ["PACE"]},
    )

    assert (df["Respondent Name"] == "PacifiCorp East").all()

    assert df.columns == EIA_FUEL_MIX_COLUMNS


@pytest.mark.integration
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

    assert d["petroleum"].columns == cols_petrol
    assert d["petroleum"].height > 0
    cols_ng = [
        "date",
        "region",
        "natural_gas_price",
        "natural_gas_percent_change",
        "electricity_price",
        "electricity_percent_change",
        "spark_spread",
    ]

    assert d["natural_gas"].columns == cols_ng
    assert d["natural_gas"].height > 0


@pytest.mark.integration
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

    assert d["weekly_spots"].columns == cols_spot_price
    assert d["weekly_spots"].height > 0

    assert d["coal_exports"].columns == cols_coal
    assert d["coal_exports"].height > 0

    assert d["coke_exports"].columns == cols_coke
    assert d["coke_exports"].height > 0


@pytest.mark.integration
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

    assert df.columns == cols


def _check_henry_hub_natural_gas_spot_prices(df):
    assert df.columns == [
        "Interval Start",
        "Interval End",
        "period",
        "duoarea",
        "area_name",
        "product",
        "fuel_type",
        "process",
        "price_type",
        "series",
        "series_description",
        "price",
        "units",
    ]

    unique_deltas = (
        (df["Interval End"] - df["Interval Start"])
        .unique(maintain_order=True)
        .to_list()
    )
    assert all(delta == pd.Timedelta(days=1) for delta in unique_deltas)

    assert set(df["series"].unique(maintain_order=True).to_list()) == set(
        [
            "RNGWHHD",
            "RNGC1",
            "RNGC2",
            "RNGC3",
            "RNGC4",
        ],
    )

    assert df["area_name"].null_count() > 0
    assert df["price"].null_count() == 0
    assert df["series"].null_count() == 0

    assert df.schema["price"] == pl.Float64


@pytest.mark.integration
def test_get_henry_hub_natural_gas_spot_prices_historical_date():
    df = EIA().get_henry_hub_natural_gas_spot_prices(
        "2024-01-02",
        "2024-01-02",
    )

    _check_henry_hub_natural_gas_spot_prices(df)

    assert df["Interval Start"].min() == pd.Timestamp(
        "2024-01-02",
        tz=HENRY_HUB_TIMEZONE,
    )
    assert df["Interval End"].max() == pd.Timestamp("2024-01-03", tz=HENRY_HUB_TIMEZONE)


@pytest.mark.integration
def test_get_henry_hub_natural_gas_spot_prices_historical_date_range():
    df = EIA().get_henry_hub_natural_gas_spot_prices(
        "2023-12-04",
        "2024-01-02",
    )

    _check_henry_hub_natural_gas_spot_prices(df)

    assert df["Interval Start"].min() == pd.Timestamp(
        "2023-12-04",
        tz=HENRY_HUB_TIMEZONE,
    )
    assert df["Interval End"].max() == pd.Timestamp("2024-01-03", tz=HENRY_HUB_TIMEZONE)


def _check_generators_data(
    df: pl.DataFrame,
    generator_status: str,
    columns: List[str] = None,
    expected_rows: int = None,
    expected_period: datetime.date = None,
    expected_updated_at: pd.Timestamp = None,
    expected_all_na_columns: List[str] = None,
):
    assert df.columns == columns
    if expected_rows is not None:
        assert df.height == expected_rows

    assert df["Period"].unique(maintain_order=True).to_list() == [expected_period]
    assert df["Entity ID"].null_count() == 0
    assert df["Plant ID"].null_count() == 0
    assert df["Generator ID"].null_count() == 0

    if expected_updated_at is not None:
        assert df["Updated At"].unique(maintain_order=True).to_list() == [
            expected_updated_at.to_pydatetime(),
        ]

    # These columns should not be all empty
    if generator_status in ["operating", "retired"]:
        for col in [
            "Balancing Authority Code",
            "DC Net Capacity",
            "Nameplate Capacity",
            "Nameplate Energy Capacity",
            "Net Winter Capacity",
            "Unit Code",
        ]:
            if expected_all_na_columns is not None and col in expected_all_na_columns:
                assert df[col].null_count() == df.height
            else:
                assert df[col].null_count() < df.height

    # Different generator_status datasets have different columns
    elif generator_status in ["planned", "canceled_or_postponed"]:
        for col in [
            "Balancing Authority Code",
            "Nameplate Capacity",
            "Net Winter Capacity",
            "Unit Code",
        ]:
            if expected_all_na_columns is not None and col in expected_all_na_columns:
                assert df[col].null_count() == df.height
            else:
                assert df[col].null_count() < df.height

    for col in GENERATOR_FLOAT_COLUMNS:
        if col in df.columns:
            assert df.schema[col] == pl.Float64

    for col in GENERATOR_INT_COLUMNS:
        if col in df.columns:
            assert df.schema[col] == pl.Int64


def test_get_generators_relative_date():
    # The files for the most recent month are generally available 24-26 days
    # after the end of the month.
    date = pd.Timestamp.utcnow() - pd.DateOffset(days=60)

    with api_vcr.use_cassette(f"test_get_generators_relative_date_{date.date()}"):
        data = EIA().get_generators(date)

    for generator_status, columns in [
        ("operating", OPERATING_GENERATOR_COLUMNS),
        ("planned", PLANNED_GENERATOR_COLUMNS),
        ("retired", RETIRED_GENERATOR_COLUMNS),
        ("canceled_or_postponed", CANCELED_OR_POSTPONED_GENERATOR_COLUMNS),
    ]:
        dataset = data[generator_status]
        _check_generators_data(
            df=dataset,
            generator_status=generator_status,
            columns=columns,
            expected_period=date.replace(day=1).date(),
        )


def test_get_generators_absolute_date():
    # This is a relatively recent file with all columns filled in
    date = pd.Timestamp("2024-10-01")

    with api_vcr.use_cassette(f"test_get_generators_absolute_date_{date.date()}"):
        data = EIA().get_generators(date)

    for generator_status, columns, expected_rows in [
        # The row values come from inspecting the spreadsheet
        ("operating", OPERATING_GENERATOR_COLUMNS, 26_455),
        ("planned", PLANNED_GENERATOR_COLUMNS, 1_864),
        ("retired", RETIRED_GENERATOR_COLUMNS, 6_715),
        ("canceled_or_postponed", CANCELED_OR_POSTPONED_GENERATOR_COLUMNS, 1_480),
    ]:
        dataset = data[generator_status]
        _check_generators_data(
            df=dataset,
            generator_status=generator_status,
            columns=columns,
            expected_rows=expected_rows,
            expected_period=pd.Timestamp("2024-10-01").date(),
            expected_updated_at=pd.Timestamp(
                "11/21/2024, 20:19:25",
                tz="UTC",
            ),
        )


def test_get_generators_absolute_date_with_missing_columns():
    # Older month where we have to fill in some columns with np.nan. This is also the
    # first month with data that works in our parsing.
    date = pd.Timestamp("2015-12-22")

    with api_vcr.use_cassette(
        f"test_get_generators_absolute_date_with_missing_columns_{date.date()}",
    ):
        data = EIA().get_generators(date)

    for generator_status, columns, expected_rows in [
        # The row values come from inspecting the spreadsheet
        ("operating", OPERATING_GENERATOR_COLUMNS, 20_070),
        ("planned", PLANNED_GENERATOR_COLUMNS, 1_028),
        ("retired", RETIRED_GENERATOR_COLUMNS, 3_053),
        ("canceled_or_postponed", CANCELED_OR_POSTPONED_GENERATOR_COLUMNS, 717),
    ]:
        dataset = data[generator_status]
        _check_generators_data(
            df=dataset,
            generator_status=generator_status,
            columns=columns,
            expected_rows=expected_rows,
            expected_period=pd.Timestamp("2015-12-01").date(),
            expected_updated_at=pd.Timestamp(
                "02/25/2016, 17:09:16",
                tz="UTC",
            ),
            expected_all_na_columns=[
                "Balancing Authority Code",
                "DC Net Capacity",
                "Nameplate Capacity",
                "Nameplate Energy Capacity",
                "Net Winter Capacity",
                "Unit Code",
            ],
        )
