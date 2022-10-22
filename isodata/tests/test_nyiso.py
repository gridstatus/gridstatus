import isodata


def test_date_range():
    iso = isodata.NYISO()
    df = iso.get_historical_fuel_mix(start="Aug 1, 2022", end="Oct 22, 2022")
    assert df.shape[0] >= 0
