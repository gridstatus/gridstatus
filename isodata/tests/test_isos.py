from isodata import *
import isodata
from isodata.base import FuelMix, ISOBase, GridStatus
import pandas as pd
import pytest
from pandas.api.types import is_numeric_dtype


all_isos = [MISO(), CAISO(), PJM(), Ercot(), SPP(), NYISO(), ISONE()]


@pytest.mark.parametrize('iso', all_isos)
def test_get_latest_fuel_mix(iso):
    print(iso)
    mix = iso.get_latest_fuel_mix()
    assert isinstance(mix, FuelMix)
    assert isinstance(mix.time, pd.Timestamp)
    assert isinstance(mix.mix, pd.DataFrame)
    assert len(mix.mix) > 0
    assert mix.iso == iso.name
    assert isinstance(repr(mix), str)


@pytest.mark.parametrize('iso', [ISONE(), NYISO(), CAISO(), PJM()])
def test_get_fuel_mix(iso):
    df = iso.get_fuel_mix_today()
    assert isinstance(df, pd.DataFrame)

    df = iso.get_fuel_mix_yesterday()
    assert isinstance(df, pd.DataFrame)


def test_list_isos():
    assert len(isodata.list_isos()) == 7


def test_get_iso():
    for iso in isodata.list_isos()["Id"].values:
        assert issubclass(isodata.get_iso(iso), ISOBase)


def test_get_iso_invalid():
    with pytest.raises(Exception) as e_info:
        isodata.get_iso("ISO DOESNT EXIST")


@pytest.mark.parametrize('iso', [CAISO(), Ercot()])
def test_get_latest_status(iso):
    status = iso.get_latest_status()
    assert isinstance(status, GridStatus)


@pytest.mark.parametrize('iso', [ISONE(), NYISO(), PJM(), CAISO()])
def test_get_historical_fuel_mix(iso):
    # date string works
    date_str = "04/03/2022"
    df = iso.get_historical_fuel_mix(date_str)
    assert isinstance(df, pd.DataFrame)
    assert df.loc[0]["Time"].strftime('%m/%d/%Y') == date_str

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


@pytest.mark.parametrize('iso', all_isos)
def test_get_latest_supply(iso):
    supply = iso.get_latest_supply()
    set(["time", "supply"]) == supply.keys()
    assert is_numeric_dtype(type(supply["supply"]))


@pytest.mark.parametrize('iso', [ISONE(), Ercot(), NYISO(), PJM(), CAISO()])
def test_get_supply_today(iso):
    # todo check that the date is right
    df = iso.get_supply_today()
    assert isinstance(df, pd.DataFrame)
    set(["Time", "Supply"]) == set(df.columns)
    assert is_numeric_dtype(df['Supply'])


@pytest.mark.parametrize('iso', [ISONE(), NYISO(), PJM(), CAISO()])
def test_get_supply(iso):
    df = iso.get_supply_yesterday()
    assert isinstance(df, pd.DataFrame)

    date_str = "March 3, 2022"
    df = iso.get_historical_supply(date_str)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Supply"]) == set(df.columns)
    assert df.loc[0]["Time"].date(
    ) == isodata.utils._handle_date(date_str).date()
    assert is_numeric_dtype(df['Supply'])


@pytest.mark.parametrize('iso', all_isos)
def test_get_demand_today(iso):
    df = iso.get_demand_today()
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Demand"]) == set(df.columns)
    assert is_numeric_dtype(df['Demand'])
    assert isinstance(df.loc[0]["Time"], pd.Timestamp)
    assert df.loc[0]["Time"].tz is not None


@pytest.mark.parametrize('iso', [PJM(), NYISO(), Ercot(), ISONE(), CAISO()])
def test_get_demand_yesterday(iso):
    # todo check that the date is right
    df = iso.get_demand_yesterday()
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Demand"]) == set(df.columns)
    assert is_numeric_dtype(df['Demand'])
    assert isinstance(df.loc[0]["Time"], pd.Timestamp)
    assert df.loc[0]["Time"].tz is not None


@pytest.mark.parametrize('iso', all_isos)
def test_get_latest_demand(iso):
    demand = iso.get_latest_demand()
    set(["time", "demand"]) == demand.keys()
    assert is_numeric_dtype(type(demand["demand"]))


@pytest.mark.parametrize('iso', [PJM(), NYISO(), ISONE(), CAISO()])
def test_get_historical_demand(iso):
    # pick a test date 2 weeks back
    test_date = (pd.Timestamp.now() - pd.Timedelta(days=14)).date()

    # date string works
    date_str = test_date.strftime('%Y%m%d')
    df = iso.get_historical_demand(date_str)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Demand"]) == set(df.columns)
    assert df.loc[0]["Time"].strftime('%Y%m%d') == date_str
    assert is_numeric_dtype(df['Demand'])

    # timestamp object works
    df = iso.get_historical_demand(test_date)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Demand"]) == set(df.columns)
    assert df.loc[0]["Time"].strftime('%Y%m%d') == test_date.strftime('%Y%m%d')
    assert is_numeric_dtype(df['Demand'])

    # datetime object works
    df = iso.get_historical_demand(test_date)
    assert isinstance(df, pd.DataFrame)
    assert set(["Time", "Demand"]) == set(df.columns)
    assert df.loc[0]["Time"].strftime('%Y%m%d') == test_date.strftime('%Y%m%d')
    assert is_numeric_dtype(df['Demand'])
