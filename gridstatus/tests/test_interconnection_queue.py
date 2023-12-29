import pytest

import gridstatus


# todo implement this
@pytest.mark.slow
@pytest.mark.parametrize("iso_class", gridstatus.utils.all_isos)
def test_get_interconnection_queue_all(iso_class):
    """Get interconnection queue data for all ISOs"""
    iso = iso_class()
    queue = iso.get_interconnection_queue()
    assert not queue.empty
