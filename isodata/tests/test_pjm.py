import pytest

import isodata


def test_pjm_handle_error():
    iso = isodata.PJM()

    # TODO this should stop raising erros in the future when archived data is supported
    with pytest.raises(RuntimeError):
        iso.get_historical_lmp(
            date="4/15/2022",
            market="REAL_TIME_5_MIN",
            locations=None,
        )


def test_dst_shift_back():
    date = "2021-11-07"
    iso = isodata.PJM()
    df = iso.get_historical_fuel_mix(start=date)

    assert len(df["Time"]) == 25  # 25 hours due to shift backwards in time
    assert (df["Time"].dt.strftime("%Y-%m-%d") == date).all()


def test_dst_shift_back():
    date = "2021-03-14"
    iso = isodata.PJM()
    df = iso.get_historical_fuel_mix(start=date)

    assert len(df["Time"]) == 25  # 25 hours due to shift backwards in time
    assert (df["Time"].dt.strftime("%Y-%m-%d") == date).all()
