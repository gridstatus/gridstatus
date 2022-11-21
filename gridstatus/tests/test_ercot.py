import pandas as pd
import pytest

import gridstatus


@pytest.mark.skip(reason="takes too long to run")
def test_ercot_get_historical_rtm_spp():
    rtm = gridstatus.Ercot().get_historical_rtm_spp(2020)
    assert isinstance(rtm, pd.DataFrame)
    assert len(rtm) > 0


def test_ercot_get_fuel_mix():

    # today
    iso = gridstatus.Ercot()
    df = iso.get_fuel_mix("today")
    assert df.shape[0] >= 0
    assert df.columns.tolist() == ["Time", "Solar", "Wind", "Other"]

    # latest
    df = iso.get_fuel_mix("latest").mix
    assert df.shape[0] >= 0
    assert df.columns.tolist() == ["Time", "Solar", "Wind", "Other"]
