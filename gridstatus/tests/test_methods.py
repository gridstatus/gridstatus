import pytest
import pandas as pd
import gridstatus

@pytest.mark.parametrize(
    "date, iso_class_name",
    [( "01/01/2022", iso_class_name) for iso_class_name in gridstatus.list_isos()['Class'].values]
)
def test_historical_load(date, iso_class_name):
    iso = getattr(gridstatus, iso_class_name)()
    df = iso.get_historical_load(date=date)
    assert not df.empty, "Dataframe came back empty"
    
    
# @pytest.mark.parametrize(
#     "start, end",
#     [
#     (
#         pd.Timestamp("01/01/2022").normalize(),
#         pd.Timestamp("01/03/2022").normalize()
#     ),
#     (
#         "01/01/2022",
#         "01/03/2022"
#     )
# ])
# def test_historical_load_range(start, end):
#     caiso = gridstatus.CAISO()
#     df = caiso.get_historical_load(start=start, end=end)
#     assert not df.empty, "Dataframe came back empty"