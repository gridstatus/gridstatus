import gridstatus
from gridstatus.base import Markets


def test_miso_locations():
    iso = gridstatus.MISO()
    data = iso.get_lmp(
        date="latest",
        market=Markets.REAL_TIME_5_MIN,
        locations=iso.hubs,
    )
    assert set(data["Location"].unique()) == set(iso.hubs)
