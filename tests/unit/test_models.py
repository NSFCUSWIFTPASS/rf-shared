import pytest
import json
import datetime
from pathlib import Path

from rf_shared.models import MetadataRecord
from rf_shared.exceptions import ChecksumMismatchError


def test_metadata_record_to_dict_serialization(mock_metadata: MetadataRecord):
    """
    Tests that the to_dict() method correctly serializes datetime and Path objects to strings.
    """
    # ACT
    data_dict = mock_metadata.to_dict()

    # ASSERT
    # Verify that the special types were converted to their string representations
    assert isinstance(data_dict["timestamp"], str)
    assert isinstance(data_dict["source_sc16_path"], str)

    # Check that the timestamp string is in the standard ISO 8601 format and includes timezone
    assert data_dict["timestamp"] == "2024-04-02T23:14:50.009919+00:00"


def test_metadata_record_from_dict_deserialization(mock_metadata: MetadataRecord):
    """
    Tests the full "round-trip" capability: converting a record to a dict and back again.
    """
    # ARRANGE: Create the dictionary representation of our golden master object
    original_dict = mock_metadata.to_dict()

    # ACT: Create a new instance from that dictionary
    recreated_record = MetadataRecord.from_dict(original_dict)

    # ASSERT: The recreated object must be exactly equal to the original fixture object
    assert isinstance(recreated_record, MetadataRecord)
    assert recreated_record == mock_metadata


def test_metadata_record_file_io_round_trip(
    mock_metadata: MetadataRecord, tmp_path: Path
):
    """
    Tests the file I/O methods by writing to a JSON file and loading it back,
    ensuring the object is perfectly preserved.
    """
    # ARRANGE: Define a path for our temporary test file
    test_file_path = tmp_path / "metadata_io_test.json"

    # --- Test the WRITE operation ---
    # ACT
    mock_metadata.write_to_json_file(test_file_path)

    # ASSERT
    assert test_file_path.exists(), "The JSON file was not created"

    # Verify the content written to the file matches our expectations
    with test_file_path.open("r") as f:
        data_from_file = json.load(f)

    # Assert against known values from the fixture
    assert data_from_file["hostname"] == "hcro-rpi-001"
    assert data_from_file["frequency"] == 915000000
    assert data_from_file["timestamp"] == "2024-04-02T23:14:50.009919+00:00"

    # --- Test the LOAD operation ---
    # ACT
    loaded_record = MetadataRecord.load_from_json_file(test_file_path)

    # ASSERT: The object loaded from the file must be identical to the original fixture object
    assert isinstance(loaded_record, MetadataRecord)
    assert loaded_record == mock_metadata


def test_from_dict_raises_error_on_incomplete_data():
    """
    Tests that the class constructor (via from_dict) correctly raises a TypeError
    if required fields are missing from the input dictionary.
    """
    # ARRANGE: Create a dictionary that is missing required fields like 'timestamp'
    incomplete_dict = {
        "hostname": "hcro-rpi-001",
        "frequency": 915000000,
        "source_sc16_path": "/tmp/dummy.sc16",  # Still needs a value for the Path conversion
    }

    # ACT & ASSERT: Use pytest.raises to confirm a TypeError is thrown
    with pytest.raises(KeyError):
        MetadataRecord.from_dict(incomplete_dict)


VALID_CHECKSUM = "abc"
INVALID_CHECKSUM = "ffffffffffffffff"


def test_validate_checksum_success(mock_metadata: MetadataRecord):
    try:
        mock_metadata.validate_checksum(VALID_CHECKSUM)
    except ChecksumMismatchError:
        pytest.fail("validate_checksum() raised ChecksumMismatchError unexpectedly!")


def test_validate_checksum_raises_exception_on_mismatch(mock_metadata: MetadataRecord):
    with pytest.raises(ChecksumMismatchError):
        mock_metadata.validate_checksum(INVALID_CHECKSUM)


def test_validate_checksum_mismatch_exception_message(mock_metadata: MetadataRecord):
    with pytest.raises(ChecksumMismatchError) as excinfo:
        mock_metadata.validate_checksum(INVALID_CHECKSUM)

    error_message = str(excinfo.value)

    assert "Checksum mismatch" in error_message
    assert f"Expected: '{VALID_CHECKSUM}'" in error_message
    assert f"Got: '{INVALID_CHECKSUM}'" in error_message


@pytest.fixture
def mock_metadata():
    return MetadataRecord(
        hostname="hcro-rpi-001",
        timestamp=datetime.datetime(
            2024, 4, 2, 23, 14, 50, 9919, tzinfo=datetime.timezone.utc
        ),
        source_sc16_path=Path("dummy_file_path.sc16"),
        serial="3227508",
        organization="hcro_db_test",
        gcs="43.1534N77.6044W",
        frequency=915000000,
        interval=10,
        length=1.0,
        gain=35,
        sampling_rate=26000000,
        bit_depth=16,
        group="snzfqW",
        checksum=VALID_CHECKSUM,
    )
