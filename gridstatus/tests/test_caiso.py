import pytest

import gridstatus


def test_get_gas_prices():
    iso = gridstatus.CAISO()
    date = "Oct 15, 2022"
    # no fuel region
    df = iso.get_gas_prices(date=date)

    n_unique = 153
    assert df["Fuel Region Id"].nunique() == n_unique
    assert len(df) == n_unique * 24

    # single fuel region
    test_region_1 = "FRPGE2GHG"
    df = iso.get_gas_prices(date=date, fuel_region_id=test_region_1)
    assert df["Fuel Region Id"].unique()[0] == test_region_1
    assert len(df) == 24

    # list of fuel regions
    test_region_2 = "FRSCE8GHG"
    df = iso.get_gas_prices(
        date=date,
        fuel_region_id=[
            test_region_1,
            test_region_2,
        ],
    )
    assert set(df["Fuel Region Id"].unique()) == set(
        [test_region_1, test_region_2],
    )
    assert len(df) == 24 * 2


def test_get_ghg_allowance():
    iso = gridstatus.CAISO()
    date = "Oct 15, 2022"
    df = iso.get_ghg_allowance(date)

    assert len(df) == 1
    assert set(df.columns) == {"Time", "GHG Allowance Price"}
