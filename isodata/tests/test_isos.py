from isodata import *
import isodata
from isodata.base import FuelMix, ISOBase
import pandas as pd
import pytest

all_isos = [MISO(), CAISO(), PJM(), Ercot(), SPP(), NYISO(), ISONE()]


@pytest.mark.parametrize('iso', all_isos)
def test_all_isos(iso):
    print(iso)
    mix = iso.get_fuel_mix()
    assert isinstance(mix, FuelMix)
    assert isinstance(mix.time, pd.Timestamp)
    assert isinstance(mix.mix, pd.DataFrame)
    assert isinstance(repr(mix), str)


def test_list_isos():
    assert len(isodata.list_isos()) == 7


def test_get_iso():
    for iso in isodata.list_isos()["Id"].values:
        assert issubclass(isodata.get_iso(iso), ISOBase)


def test_latest_demand():
    iso = CAISO()
    demand = iso.get_latest_demand()

    assert set(["time", "demand"]) == demand.keys()


def test_latest_supply():
    iso = CAISO()
    demand = iso.get_latest_supply()

    assert set(["time", "supply"]) == demand.keys()


def test_get_historical_fuel_mixmak():
    iso = CAISO()

    # date string works
    date_str = "20220322"
    df = iso.get_historical_fuel_mix(date_str)
    assert isinstance(df, pd.DataFrame)
    assert df.loc[0]["Time"].strftime('%Y%m%d') == date_str

    # timestamp object works
    date_obj = pd.to_datetime("2019/11/19")
    df = iso.get_historical_fuel_mix(date_obj)
    assert isinstance(df, pd.DataFrame)
    assert df.loc[0]["Time"].strftime('%Y%m%d') == date_obj.strftime('%Y%m%d')

    # datetime object works
    date_obj = pd.to_datetime("2021/05/09").date()
    df = iso.get_historical_fuel_mix(date_obj)
    assert isinstance(df, pd.DataFrame)
    assert df.loc[0]["Time"].strftime('%Y%m%d') == date_obj.strftime('%Y%m%d')


def test_get_historical_demand():
    iso = CAISO()

    # date string works
    date_str = "20220322"
    df = iso.get_historical_demand(date_str)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Demand"]) == set(df.columns)
    assert df.loc[0]["Time"].strftime('%Y%m%d') == date_str

    # timestamp object works
    date_obj = pd.to_datetime("2019/11/19")
    df = iso.get_historical_demand(date_obj)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Demand"]) == set(df.columns)
    assert df.loc[0]["Time"].strftime('%Y%m%d') == date_obj.strftime('%Y%m%d')

    # datetime object works
    date_obj = pd.to_datetime("2021/05/09").date()
    df = iso.get_historical_demand(date_obj)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Demand"]) == set(df.columns)
    assert df.loc[0]["Time"].strftime('%Y%m%d') == date_obj.strftime('%Y%m%d')


def test_get_historical_supply():
    iso = CAISO()

    # date string works
    # todo abstract the testing of all way to add dates
    date_str = "20220322"
    df = iso.get_historical_supply(date_str)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Supply"]) == set(df.columns)
    assert df.loc[0]["Time"].strftime('%Y%m%d') == date_str
