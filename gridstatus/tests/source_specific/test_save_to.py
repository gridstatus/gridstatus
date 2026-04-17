import os

import gridstatus
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

vcr = setup_vcr(
    source="caiso_save_to",
    record_mode=RECORD_MODE,
)


def test_save_to_one_day_per_request(tmp_path):
    with vcr.use_cassette("caiso_save_to_one_day_per_request.yaml"):
        iso = gridstatus.CAISO()
        df = iso.get_fuel_mix(
            start="Jan 1, 2022",
            end="Jan 4, 2022",
            save_to=tmp_path,
        )

    files = set(os.listdir(tmp_path))

    assert len(files) == 3
    assert files == set(
        [
            "CAISO_get_fuel_mix_20220101.csv",
            "CAISO_get_fuel_mix_20220102.csv",
            "CAISO_get_fuel_mix_20220103.csv",
        ],
    )

    df_2 = gridstatus.load_folder(tmp_path)

    assert (df == df_2).all().all()


def test_save_to_with_date_range_requests(tmp_path):
    with vcr.use_cassette("nyiso_save_to_with_date_range_requests.yaml"):
        iso = gridstatus.NYISO()

        df = iso.get_fuel_mix(
            start="Jan 30, 2022",
            end="Feb 2, 2022",
            save_to=tmp_path,
        )

    files = set(os.listdir(tmp_path))

    assert len(files) == 2
    assert files == set(
        [
            "NYISO_get_fuel_mix_20220130_20220201.csv",
            "NYISO_get_fuel_mix_20220201_20220202.csv",
        ],
    )

    df_2 = gridstatus.load_folder(tmp_path)
    assert (df == df_2).all().all()
