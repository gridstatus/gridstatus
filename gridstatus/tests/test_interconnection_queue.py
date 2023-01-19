import pytest

import gridstatus


# todo implement this
@pytest.mark.slow
def test_get_interconnection_queue_all():
    """Get interconnection queue data for all ISOs"""
    gridstatus.get_interconnection_queues()
