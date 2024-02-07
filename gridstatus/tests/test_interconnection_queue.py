import pytest

import gridstatus

interconnection_queues = [
    pytest.param(iso_class)
    for iso_class in gridstatus.utils.all_isos
    if iso_class != gridstatus.IESO
] + [
    pytest.param(
        gridstatus.IESO,
        marks=pytest.mark.xfail(
            reason="IESO has no interconnection queue implementation yet",
            strict=True,
        ),
    ),
]


# todo implement this
@pytest.mark.slow
@pytest.mark.parametrize(
    "iso_class",
    interconnection_queues,
)
def test_get_interconnection_queue_all(iso_class):
    """Get interconnection queue data for all ISOs"""
    iso = iso_class()
    queue = iso.get_interconnection_queue()
    assert not queue.empty
