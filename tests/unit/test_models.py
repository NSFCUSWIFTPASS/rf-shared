import pytest
import json
import datetime
import uuid
from pathlib import Path

# Pydantic's specific validation error
from pydantic import ValidationError

# Import your new Pydantic models
from rf_shared.models import (
    MetadataRecord,
    Envelope,
    ChecksumMismatchError,
)

# --- Constants ---
VALID_CHECKSUM = "abc"
INVALID_CHECKSUM = "ffffffffffffffff"


# --- Fixtures  ---


@pytest.fixture
def mock_metadata() -> MetadataRecord:
    """Returns a Pydantic MetadataRecord instance for testing."""
    return MetadataRecord(
        hostname="hcro-rpi-001",
        timestamp=datetime.datetime(
            2024, 4, 2, 23, 14, 50, 9919, tzinfo=datetime.timezone.utc
        ),
        source_path=Path("dummy_file_path.sc16"),
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


@pytest.fixture
def mock_envelope(mock_metadata: MetadataRecord) -> Envelope:
    """Returns a Pydantic Envelope instance for testing."""
    # The factory method is part of the model, so this works seamlessly
    return Envelope.from_metadata(mock_metadata)


# --- MetadataRecord Tests  ---


def test_metadata_record_pydantic_serialization(mock_metadata: MetadataRecord):
    """
    Tests that model_dump(mode='json') correctly serializes datetime and Path objects to strings.
    """
    # ACT: Use Pydantic's built-in serialization method
    data_dict = mock_metadata.model_dump(mode="json")

    # ASSERT
    assert isinstance(data_dict["timestamp"], str)
    assert isinstance(data_dict["source_path"], str)
    assert data_dict["timestamp"] == "2024-04-02T23:14:50.009919Z"


def test_metadata_record_pydantic_round_trip(mock_metadata: MetadataRecord):
    """
    Tests the full "round-trip" capability: serializing to a dict and deserializing back.
    """
    # ARRANGE: Create the dictionary using Pydantic's method
    original_dict = mock_metadata.model_dump(mode="json")

    # ACT: Create a new instance using Pydantic's validation/parsing method
    recreated_record = MetadataRecord.model_validate(original_dict)

    # ASSERT: Pydantic models have automatic equality checks
    assert isinstance(recreated_record, MetadataRecord)
    assert recreated_record == mock_metadata


def test_metadata_record_file_io_with_pydantic(
    mock_metadata: MetadataRecord, tmp_path: Path
):
    """
    Tests file I/O using Pydantic's JSON methods, replacing the old custom methods.
    """
    test_file_path = tmp_path / "metadata_io_test.json"

    # --- Test the WRITE operation ---
    # ACT: Use model_dump_json for direct serialization to a JSON string
    json_string_to_write = mock_metadata.model_dump_json(indent=4)
    test_file_path.write_text(json_string_to_write)

    # ASSERT
    assert test_file_path.exists()
    data_from_file = json.loads(test_file_path.read_text())
    assert data_from_file["hostname"] == "hcro-rpi-001"
    assert data_from_file["timestamp"] == "2024-04-02T23:14:50.009919Z"

    # --- Test the LOAD operation ---
    # ACT: Pydantic can validate directly from a JSON string
    loaded_record = MetadataRecord.model_validate_json(test_file_path.read_text())

    # ASSERT
    assert isinstance(loaded_record, MetadataRecord)
    assert loaded_record == mock_metadata


def test_metadata_record_accepts_subsecond_interval():
    """``interval`` is float, so values like 0.25/0.5 (continuous-stream sensors) are valid.

    iq-feeder traffic sends integer seconds (e.g. 10) which still validates as float.
    """
    record = MetadataRecord(
        hostname="rfobs-01",
        timestamp=datetime.datetime(
            2026, 4, 27, 12, 0, 0, tzinfo=datetime.timezone.utc
        ),
        source_path=Path("dummy.sc16"),
        serial="X1",
        organization="Org",
        gcs="0,0",
        frequency=2_437_000_000,
        interval=0.25,
        length=0.5,
        gain=40,
        sampling_rate=25_000_000,
        bit_depth=16,
        group="g",
        checksum="c",
    )
    assert record.interval == 0.25
    # Round-trips through JSON unchanged.
    parsed = MetadataRecord.model_validate_json(record.model_dump_json())
    assert parsed.interval == 0.25


def test_pydantic_raises_validation_error_on_incomplete_data():
    """
    Tests that Pydantic's validation correctly raises a ValidationError
    if required fields are missing from the input dictionary.
    """
    # ARRANGE: A dictionary missing many required fields
    incomplete_dict = {"hostname": "hcro-rpi-001", "frequency": 915000000}

    # ACT & ASSERT: Check for Pydantic's specific, detailed error
    with pytest.raises(ValidationError) as excinfo:
        MetadataRecord.model_validate(incomplete_dict)

    # Optional: Assert that the error message is helpful
    error_str = str(excinfo.value)
    assert "timestamp" in error_str
    assert "Field required" in error_str


# --- Checksum Business Logic Tests (NO CHANGES NEEDED) ---


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


# --- Envelope Tests  ---


def test_envelope_pydantic_serialization(mock_envelope: Envelope):
    """
    Tests that model_dump correctly serializes Path and UUID objects to strings.
    """
    # ACT
    data_dict = mock_envelope.model_dump(mode="json")

    # ASSERT
    assert isinstance(data_dict["source_path"], str)
    assert isinstance(data_dict["message_id"], str)
    assert isinstance(data_dict["payload"], dict)
    # Check that the UUID string is valid
    uuid.UUID(data_dict["message_id"])


def test_envelope_pydantic_round_trip(mock_envelope: Envelope):
    """
    Tests the full "round-trip" capability for the Envelope model.
    """
    # ARRANGE
    original_dict = mock_envelope.model_dump(mode="json")

    # ACT
    recreated_envelope = Envelope.model_validate(original_dict)

    # ASSERT
    assert isinstance(recreated_envelope, Envelope)
    assert recreated_envelope == mock_envelope


def test_envelope_from_metadata_factory(mock_metadata: MetadataRecord):
    """
    Tests the from_metadata() factory method with the new Pydantic implementation.
    """
    # ACT
    envelope = Envelope.from_metadata(mock_metadata)

    # ASSERT
    assert isinstance(envelope, Envelope)
    assert envelope.source_path == mock_metadata.source_path
    assert isinstance(envelope.message_id, uuid.UUID)
    # The payload should be the Pydantic-serialized dictionary of the metadata
    assert envelope.payload == mock_metadata.model_dump(mode="json")


def test_envelope_raises_validation_error_on_incomplete_data():
    """
    Tests that Pydantic validation for Envelope fails on missing data.
    """
    # ARRANGE
    incomplete_dict = {"message_id": str(uuid.uuid4())}

    # ACT & ASSERT
    with pytest.raises(ValidationError) as excinfo:
        Envelope.model_validate(incomplete_dict)

    error_str = str(excinfo.value)
    assert "source_path" in error_str
    assert "payload" in error_str
    assert "Field required" in error_str
