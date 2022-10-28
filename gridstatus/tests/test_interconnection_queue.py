from gridstatus import CAISO, ISONE, MISO, NYISO, PJM, SPP, Ercot
from gridstatus.base import _interconnection_columns


def check_queue(queue):
    # todo make sure datetime columns are right type
    assert isinstance(queue, pd.DataFrame)
    assert queue.shape[0] > 0
    assert set(_interconnection_columns).issubset(queue.columns)


@pytest.mark.parametrize(
    "iso",
    [PJM(), NYISO()],  # , CAISO(), MISO(), Ercot(), ISONE(),  SPP()],
)
def test_get_interconnection_queue(iso):
    queue = iso.get_interconnection_queue()
    check_queue(queue)
