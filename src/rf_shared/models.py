import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
import uuid
from typing import Any, Dict, Awaitable, Callable

from rf_shared.exceptions import (
    ChecksumMismatchError,
    MetadataRecordParsingError,
    EnvelopeParsingError,
)


@dataclass(frozen=True)
class MetadataRecord:
    """Represents the metadata for a single IQ data recording."""

    # Core identifying information
    hostname: str
    timestamp: datetime
    source_path: Path
    serial: str

    # Grouping and location info
    organization: str
    gcs: str
    group: str

    # Radio settings
    frequency: int
    interval: int
    length: float
    gain: int

    sampling_rate: int
    bit_depth: int

    checksum: str

    def validate_checksum(self, calculated_checksum: str):
        if self.checksum != calculated_checksum:
            raise ChecksumMismatchError(
                f"Checksum mismatch for file. "
                f"Expected: '{self.checksum}', Got: '{calculated_checksum}'"
            )

    def to_dict(self) -> dict:
        """Converts the dataclass instance to a JSON-serializable dictionary."""
        data = asdict(self)
        # Convert datetime to ISO 8601 string format
        data["timestamp"] = self.timestamp.isoformat()
        # Convert Path object to a string
        data["source_path"] = str(self.source_path)
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "MetadataRecord":
        """Creates a MetadataRecord instance from a dictionary."""
        try:
            data_copy = data.copy()
            data_copy["timestamp"] = datetime.fromisoformat(data_copy["timestamp"])
            data_copy["source_path"] = Path(data_copy["source_path"])
            return cls(**data_copy)
        except (KeyError, TypeError, ValueError) as e:
            raise MetadataRecordParsingError(
                f"Invalid metadata payload structure: {e}"
            ) from e

    def write_to_json_file(self, file_path: Path):
        """Serializes this record and writes it to a JSON file."""
        with file_path.open("w") as f:
            json.dump(self.to_dict(), f, indent=4)

    @classmethod
    def load_from_json_file(cls, file_path: Path) -> "MetadataRecord":
        """Loads a record from a JSON file and creates a MetadataRecord instance."""
        with file_path.open("r") as f:
            data_dict = json.load(f)
        return cls.from_dict(data_dict)


@dataclass(frozen=True)
class Envelope:
    """
    Defines the message structure for an RF data message.
    """

    source_path: Path
    payload: Dict[str, Any]
    message_id: uuid.UUID

    def to_dict(self) -> dict:
        """Converts the envelope instance to a JSON-serializable dictionary."""
        return {
            "source_path": str(self.source_path),
            "payload": self.payload,
            "message_id": str(self.message_id),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Envelope":
        """Creates an Envelope instance from a dictionary."""
        try:
            return cls(
                source_path=Path(data["source_path"]),
                payload=data["payload"],
                message_id=uuid.UUID(data["message_id"]),
            )
        except (KeyError, TypeError, ValueError) as e:
            raise EnvelopeParsingError(f"Invalid envelope structure: {e}") from e

    @classmethod
    def from_metadata(cls, metadata: MetadataRecord) -> "Envelope":
        """Factory method to create an Envelope from a MetadataRecord instance."""
        return cls(
            source_path=metadata.source_path,
            payload=metadata.to_dict(),
            message_id=uuid.uuid4(),
        )


@dataclass(frozen=True)
class IQStatistics:
    """Represents the calculated power statistics from an IQ data file."""

    average: float
    max: float
    median: float
    std: float
    kurtosis: float


async def no_op_ack():
    """An awaitable function that does nothing."""
    pass


@dataclass(frozen=True)
class ReceivedMessage:
    """
    A transport-agnostic representation of a message.
    It contains the data and an optional callback to acknowledge it.
    """

    data: bytes
    ack: Callable[[], Awaitable[None]] = no_op_ack
