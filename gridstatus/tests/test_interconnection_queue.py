import pandas as pd
import pytest
import tqdm

from gridstatus import all_isos
from gridstatus.base import _interconnection_columns


# todo implement this
@pytest.mark.slow
def test_get_interconnection_queue_all():
    """Get interconnection queue data for all ISOs"""
    all_queues = []
    for iso in tqdm.tqdm(all_isos):
        iso = iso()
        # only shared columns
        queue = iso.get_interconnection_queue()[_interconnection_columns]
        queue.insert(0, "ISO", iso.name)
        all_queues.append(queue)
        pd.concat(all_queues)

    all_queues = pd.concat(all_queues).reset_index(drop=True)
    return all_queues
