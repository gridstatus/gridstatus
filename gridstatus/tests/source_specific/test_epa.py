import datetime
import os

import pandas as pd
import pytest
import vcr

from gridstatus.base import NoDataFoundException
from gridstatus.epa import EPA
from gridstatus.epa.epa_constants import POWER_PLANT_EMISSIONS_GENERATION_COLUMNS
from gridstatus.tests.vcr_utils import RECORD_MODE, clean_cassettes

cassette_dir = f"{os.path.dirname(__file__)}/../fixtures/epa/vcr_cassettes"
if RECORD_MODE == "all":
    clean_cassettes(cassette_dir)

api_vcr = vcr.VCR(
    cassette_library_dir=cassette_dir,
    record_mode=RECORD_MODE,
    match_on=["uri", "method"],
    filter_query_parameters=["api_key"],
    filter_headers=[
        ("Authorization", "XXXXXX"),
        ("X-Api-Key", "XXXXXX"),
    ],
)


def _check_power_plant_emissions_generation(df: pd.DataFrame) -> None:
    assert df.shape[0] > 0
    assert df.columns.tolist() == POWER_PLANT_EMISSIONS_GENERATION_COLUMNS
    assert df["Interval Start"].dtype == "datetime64[ns, UTC]"
    assert df["Interval End"].dtype == "datetime64[ns, UTC]"
    assert (df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)).all()
    assert df["Facility ID"].notna().all()
    assert df["Unit ID"].notna().all()
    assert df.duplicated(subset=["Interval Start", "Facility ID", "Unit ID"]).sum() == 0


def test_get_power_plant_emissions_generation_single_day() -> None:
    with api_vcr.use_cassette(
        "test_get_power_plant_emissions_generation_single_day_2023-07-01",
    ):
        df = EPA().get_power_plant_emissions_generation(
            "2023-07-01",
            state_codes=["DE"],
        )

    _check_power_plant_emissions_generation(df)
    assert df["State"].eq("DE").all()
    assert df["lat"].notna().any()
    assert df["lon"].notna().any()
    assert (
        df["Interval Start"].min().date()
        <= datetime.date(2023, 7, 1)
        <= df["Interval Start"].max().date()
    )


def test_get_power_plant_emissions_generation_date_range() -> None:
    with api_vcr.use_cassette(
        "test_get_power_plant_emissions_generation_date_range_2023-07-01_2023-07-02",
    ):
        df = EPA().get_power_plant_emissions_generation(
            "2023-07-01",
            end="2023-07-03",
            state_codes=["DE"],
        )

    _check_power_plant_emissions_generation(df)
    dates = sorted(
        df["Interval Start"].dt.tz_convert("America/New_York").dt.date.unique(),
    )
    assert datetime.date(2023, 7, 1) in dates
    assert datetime.date(2023, 7, 2) in dates


def test_get_power_plant_emissions_generation_no_data() -> None:
    with api_vcr.use_cassette(
        "test_get_power_plant_emissions_generation_no_data_1995-01-01",
    ):
        with pytest.raises(NoDataFoundException):
            EPA().get_power_plant_emissions_generation(
                "1995-01-01",
                state_codes=["DE"],
            )
