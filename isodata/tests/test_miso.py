import isodata
from isodata.base import Markets


def test_miso_locations():
    iso = isodata.MISO()
    data = iso.get_latest_lmp(Markets.REAL_TIME_5_MIN, locations=iso.hubs)
    assert set(data["Location"].unique()) == set(iso.hubs)
