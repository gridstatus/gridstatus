import pytest

import isodata


def test_pjm_handle_error():
    o = isodata.PJM()

    # TODO this should stop raising erros in the future when archived data is supported
    with pytest.raises(RuntimeError):
        o.get_historical_lmp(
            date="4/15/2022",
            market="REAL_TIME_5_MIN",
            locations=None,
        )
