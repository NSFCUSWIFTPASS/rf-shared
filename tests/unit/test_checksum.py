import pytest
from pathlib import Path

from rf_shared.checksum import get_checksum, get_file_checksum


@pytest.fixture
def sample_data() -> bytes:
    return b"Hello, this is a test of the checksum functions."


@pytest.fixture
def sample_file(tmp_path: Path, sample_data: bytes) -> Path:
    file_path = tmp_path / "test_file.txt"
    file_path.write_bytes(sample_data)
    return file_path


def test_get_checksum_of_bytes(sample_data: bytes):
    # ARRANGE: We have our sample_data.
    expected_checksum = "2ea87543227119431029c424e10602e4"

    # ACT: Calculate the checksum of the byte string.
    actual_checksum = get_checksum(sample_data)

    # ASSERT: Check if the calculated checksum matches the known correct one.
    assert actual_checksum == expected_checksum


def test_get_file_checksum(sample_file: Path, sample_data: bytes):
    # ARRANGE: The sample_file fixture has created a file on disk.
    expected_checksum = get_checksum(sample_data)

    # ACT: Calculate the checksum directly from the file.
    actual_checksum = get_file_checksum(sample_file)

    # ASSERT: The checksum from the file should be identical to the one from memory.
    assert actual_checksum == expected_checksum
    assert actual_checksum == "2ea87543227119431029c424e10602e4"


def test_get_file_checksum_on_empty_file(tmp_path: Path):
    # ARRANGE: Create an empty file.
    empty_file = tmp_path / "empty.txt"
    empty_file.touch()

    expected_checksum_of_empty = "d41d8cd98f00b204e9800998ecf8427e"

    # ACT: Calculate the checksum.
    actual_checksum = get_file_checksum(empty_file)

    # ASSERT: Check against the known value.
    assert actual_checksum == expected_checksum_of_empty
