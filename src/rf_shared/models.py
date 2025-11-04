from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import uuid
from typing import Any, Dict, Awaitable, Callable, List, Self
from pydantic import BaseModel, model_validator, ConfigDict, ValidationError

from rf_shared.exceptions import ChecksumMismatchError

__all__ = ["ValidationError"]


class IQStatistics(BaseModel):
    """Represents the calculated power statistics from an IQ data file."""

    model_config = ConfigDict(frozen=True)

    average: float
    max: float
    median: float
    std: float
    kurtosis: float


class PSDData(BaseModel):
    model_config = ConfigDict(frozen=True)

    num_bins: int
    center_freq: float
    sample_rate: int
    powers: List[float]
    frequencies: List[float]

    @model_validator(mode="after")
    def check_lengths(self):
        if len(self.powers) != self.num_bins:
            raise ValueError(
                f"powers length ({len(self.powers)}) != num_bins ({self.num_bins})"
            )

        if len(self.frequencies) != self.num_bins:
            raise ValueError(
                f"frequencies length ({len(self.frequencies)}) != num_bins ({self.num_bins})"
            )

        return self


class MetadataRecord(BaseModel):
    """Represents the metadata for a single IQ data recording."""

    model_config = ConfigDict(frozen=True)

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
        """This business logic method can remain exactly as it is."""
        if self.checksum != calculated_checksum:
            raise ChecksumMismatchError(
                f"Checksum mismatch for file. Expected: '{self.checksum}', Got: '{calculated_checksum}'"
            )


class Envelope(BaseModel):
    """Defines the message structure for an RF data message."""

    model_config = ConfigDict(frozen=True)

    source_path: Path
    payload: Dict[str, Any]
    message_id: uuid.UUID

    @classmethod
    def from_metadata(cls, metadata: MetadataRecord) -> Self:
        """Factory method to create an Envelope from a MetadataRecord instance."""
        return cls(
            source_path=metadata.source_path,
            payload=metadata.model_dump(mode="json"),
            message_id=uuid.uuid4(),
        )


class ProcessedDataEnvelope(BaseModel):
    """Defines the message structure for a processed data message."""

    model_config = ConfigDict(frozen=True)

    metadata: MetadataRecord
    statistics: IQStatistics
    psd_data: PSDData
    message_id: uuid.UUID


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
