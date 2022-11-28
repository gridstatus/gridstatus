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


def test_get_curtailment():
    iso = gridstatus.CAISO()
    date = "Oct 15, 2022"
    df = iso.get_curtailment(date)
    assert df.shape == (31, 6)


def test_get_curtailment_2_pages():
    # test that the function can handle 3 pages of data
    iso = gridstatus.CAISO()
    date = "March 15, 2022"
    df = iso.get_curtailment(date)
    assert df.shape == (55, 6)


def test_get_curtailment_3_pages():
    # test that the function can handle 3 pages of data
    iso = gridstatus.CAISO()
    date = "March 16, 2022"
    df = iso.get_curtailment(date)
    assert df.shape == (76, 6)


def check_as_data(df, market):
    columns = [
        "Time",
        "Region",
        "Market",
        "Non-Spinning Reserves Procured (MW)",
        "Non-Spinning Reserves Self-Provided (MW)",
        "Non-Spinning Reserves Total (MW)",
        "Non-Spinning Reserves Total Cost",
        "Regulation Down Procured (MW)",
        "Regulation Down Self-Provided (MW)",
        "Regulation Down Total (MW)",
        "Regulation Down Total Cost",
        "Regulation Mileage Down Procured (MW)",
        "Regulation Mileage Down Self-Provided (MW)",
        "Regulation Mileage Down Total (MW)",
        "Regulation Mileage Down Total Cost",
        "Regulation Mileage Up Procured (MW)",
        "Regulation Mileage Up Self-Provided (MW)",
        "Regulation Mileage Up Total (MW)",
        "Regulation Mileage Up Total Cost",
        "Regulation Up Procured (MW)",
        "Regulation Up Self-Provided (MW)",
        "Regulation Up Total (MW)",
        "Regulation Up Total Cost",
        "Spinning Reserves Procured (MW)",
        "Spinning Reserves Self-Provided (MW)",
        "Spinning Reserves Total (MW)",
        "Spinning Reserves Total Cost",
    ]
    assert df.columns.tolist() == columns
    assert df["Market"].unique()[0] == market
    assert df.shape[0] > 0


def test_caiso_get_as_procurement():
    iso = gridstatus.CAISO()
    date = "Oct 15, 2022"
    for market in ["DAM", "RTM"]:
        df = iso.get_as_procurement(date, market=market)
        check_as_data(df, market)


def test_caiso_get_as_prices():
    iso = gridstatus.CAISO()
    date = "Oct 15, 2022"
    df = iso.get_as_prices(date)

    assert df.shape[0] > 0

    assert df.columns.tolist() == [
        "Time",
        "Region",
        "Market",
        "Non-Spinning Reserves",
        "Regulation Down",
        "Regulation Mileage Down",
        "Regulation Mileage Up",
        "Regulation Up",
        "Spinning Reserves",
    ]
