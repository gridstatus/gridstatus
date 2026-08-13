import io
import struct
import zipfile

import pandas as pd
import pytest
import time_machine

import gridstatus
from gridstatus.utils import (
    is_dst_end,
    is_today,
    is_yesterday,
    read_zip_member_with_local_header_fallback,
)


def test_is_dst_end():
    date = pd.Timestamp("Nov 6, 2022", tz=gridstatus.NYISO.default_timezone)

    assert is_dst_end(date)
    assert not is_dst_end(date - pd.Timedelta("1 day"))
    assert not is_dst_end(date + pd.Timedelta("1 day"))

    # test start
    dst_start = pd.Timestamp(
        "Mar 13, 2022",
        tz=gridstatus.NYISO.default_timezone,
    )
    assert not is_dst_end(dst_start)


eastern_timezone = "America/New_York"
central_timezone = "America/Chicago"


def test_is_today():
    # Mock a time where the EST date is different from the UTC date
    with time_machine.travel("2024-01-01T02:00:00Z", tick=False):
        utc_start_of_day = pd.Timestamp.utcnow().normalize()

        assert (
            pd.Timestamp.utcnow().date() != pd.Timestamp.now(tz=eastern_timezone).date()
        )

        # Because is_today converts the timestamp into the timezone provided,
        # this should be true
        assert is_today(utc_start_of_day, tz=eastern_timezone)

        # EST offset is 5 hours so this timestamp is tomorrow in EST
        assert not is_today(
            utc_start_of_day + pd.Timedelta(hours=5),
            tz=eastern_timezone,
        )

        # Yesterday in EST
        assert not is_today(
            utc_start_of_day - pd.Timedelta(hours=19, seconds=1),
            tz=eastern_timezone,
        )

        # CST offset is 6 hours so this timestamp is today in CST
        assert is_today(utc_start_of_day + pd.Timedelta(hours=5), tz=central_timezone)

        # Yesterday in CST
        assert not is_today(
            utc_start_of_day - pd.Timedelta(hours=18, seconds=1),
            tz=central_timezone,
        )

        # Tomorrow in CST
        assert not is_today(
            utc_start_of_day + pd.Timedelta(hours=6),
            tz=central_timezone,
        )

    # Mock a time where the EST date is the same as the UTC date
    with time_machine.travel("2024-01-01T12:00:00Z", tick=False):
        utc_start_of_day = pd.Timestamp.utcnow().normalize()

        assert (
            pd.Timestamp.utcnow().date() == pd.Timestamp.now(tz=eastern_timezone).date()
        )

        # Yesterday in EST
        assert not is_today(utc_start_of_day, tz=eastern_timezone)

        assert is_today(utc_start_of_day + pd.Timedelta(hours=5), tz=eastern_timezone)
        assert is_today(utc_start_of_day + pd.DateOffset(days=1), tz=eastern_timezone)

        # Tomorrow in EST
        assert not is_today(
            utc_start_of_day + pd.DateOffset(days=1) + pd.Timedelta(hours=5),
            tz=eastern_timezone,
        )

        # Yesterday in CST
        assert not is_today(utc_start_of_day, tz=central_timezone)

        assert is_today(utc_start_of_day + pd.Timedelta(hours=6), tz=central_timezone)

        # Tomorrow in CST
        assert not is_today(
            utc_start_of_day + pd.DateOffset(days=1) + pd.Timedelta(hours=6),
            tz=central_timezone,
        )


def test_is_yesterday():
    with time_machine.travel("2024-01-01T02:00:00Z", tick=False):
        start_of_utc_yesterday = pd.Timestamp.utcnow().normalize() - pd.DateOffset(
            days=1,
        )

        # Because is_yesterday converts the timestamp into the given timezone,
        # this should be true
        assert is_yesterday(start_of_utc_yesterday, tz=eastern_timezone)

        # EST offset is 5 hours so this timestamp is today in EST
        assert not is_yesterday(
            start_of_utc_yesterday + pd.Timedelta(hours=5),
            tz=eastern_timezone,
        )

        assert is_yesterday(start_of_utc_yesterday, tz=central_timezone)
        assert is_yesterday(
            start_of_utc_yesterday + pd.Timedelta(hours=5),
            tz=central_timezone,
        )

        # CST offset is 6 hours so this timestamp is today in CST
        assert not is_yesterday(
            start_of_utc_yesterday + pd.Timedelta(hours=6),
            tz=central_timezone,
        )

    with time_machine.travel("2024-01-01T12:00:00Z", tick=False):
        start_of_utc_yesterday = pd.Timestamp.utcnow().normalize() - pd.DateOffset(
            days=1,
        )

        # This is the day before yesterday in EST
        assert not is_yesterday(start_of_utc_yesterday, tz=eastern_timezone)
        assert is_yesterday(
            start_of_utc_yesterday + pd.Timedelta(hours=5),
            tz=eastern_timezone,
        )

        # This is today in EST
        assert not is_yesterday(
            start_of_utc_yesterday + pd.DateOffset(days=1) + pd.Timedelta(hours=6),
            tz=eastern_timezone,
        )

        # This is the day before yesterday in CST
        assert not is_yesterday(start_of_utc_yesterday, tz=central_timezone)
        assert is_yesterday(
            start_of_utc_yesterday + pd.Timedelta(hours=6),
            tz=central_timezone,
        )

        # This is today in CST
        assert not is_yesterday(
            start_of_utc_yesterday + pd.DateOffset(days=1) + pd.Timedelta(hours=6),
            tz=central_timezone,
        )


ZIP_MEMBER_NAME = "Native_Load_2026.xlsx"
ZIP_MEMBER_CONTENT = b"hour_ending,coast,east,far_west\n" * 500
ZIP_CENTRAL_DIRECTORY_SIGNATURE = b"PK\x01\x02"
ZIP_CENTRAL_DIRECTORY_CRC_OFFSET = 16
ZIP_LOCAL_HEADER_CRC_OFFSET = 14


def _build_zip_with_corrupt_central_directory() -> bytearray:
    """Build a zip whose central directory CRC and sizes disagree with the
    local file header, mimicking ERCOT's corrupt Native_Load archives."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr(ZIP_MEMBER_NAME, ZIP_MEMBER_CONTENT)
    zip_bytes = bytearray(buffer.getvalue())
    central_directory_offset = zip_bytes.find(ZIP_CENTRAL_DIRECTORY_SIGNATURE)
    correct_crc, correct_compressed_size, correct_uncompressed_size = (
        struct.unpack_from(
            "<3I",
            zip_bytes,
            central_directory_offset + ZIP_CENTRAL_DIRECTORY_CRC_OFFSET,
        )
    )
    struct.pack_into(
        "<3I",
        zip_bytes,
        central_directory_offset + ZIP_CENTRAL_DIRECTORY_CRC_OFFSET,
        correct_crc ^ 0xFFFFFFFF,
        correct_compressed_size + 40,
        correct_uncompressed_size + 100,
    )
    return zip_bytes


def test_read_zip_member_with_local_header_fallback_recovers_member():
    zip_bytes = bytes(_build_zip_with_corrupt_central_directory())

    # Precondition: a plain read rejects the corrupt central directory
    with pytest.raises(zipfile.BadZipFile):
        zipfile.ZipFile(io.BytesIO(zip_bytes)).read(ZIP_MEMBER_NAME)

    zip_file = zipfile.ZipFile(io.BytesIO(zip_bytes))
    recovered = read_zip_member_with_local_header_fallback(zip_file, ZIP_MEMBER_NAME)
    assert recovered == ZIP_MEMBER_CONTENT


def test_read_zip_member_with_local_header_fallback_intact_zip():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr(ZIP_MEMBER_NAME, ZIP_MEMBER_CONTENT)

    zip_file = zipfile.ZipFile(io.BytesIO(buffer.getvalue()))
    recovered = read_zip_member_with_local_header_fallback(zip_file, ZIP_MEMBER_NAME)
    assert recovered == ZIP_MEMBER_CONTENT


def test_read_zip_member_with_local_header_fallback_corrupt_data_still_raises():
    zip_bytes = _build_zip_with_corrupt_central_directory()
    # Corrupt the local file header CRC as well so neither header matches the
    # data and the fallback cannot validate it
    (local_crc,) = struct.unpack_from("<I", zip_bytes, ZIP_LOCAL_HEADER_CRC_OFFSET)
    struct.pack_into("<I", zip_bytes, ZIP_LOCAL_HEADER_CRC_OFFSET, local_crc ^ 0xFFFF)

    zip_file = zipfile.ZipFile(io.BytesIO(bytes(zip_bytes)))
    with pytest.raises(zipfile.BadZipFile):
        read_zip_member_with_local_header_fallback(zip_file, ZIP_MEMBER_NAME)
