import pandas as pd
import pytest

import isodata


@pytest.mark.skip(reason="takes too long to run")
def test_ercot_get_historical_rtm_spp():
    rtm = isodata.Ercot().get_historical_rtm_spp(2020)
    assert isinstance(rtm, pd.DataFrame)
    assert len(rtm) > 0
