import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class MetadataRecord:
    """Represents the metadata for a single IQ data recording."""

    # Core identifying information
    hostname: str
    timestamp: datetime
    source_sc16_path: Path
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

    def to_dict(self) -> dict:
        """Converts the dataclass instance to a JSON-serializable dictionary."""
        data = asdict(self)
        # Convert datetime to ISO 8601 string format
        data["timestamp"] = self.timestamp.isoformat()
        # Convert Path object to a string
        data["source_sc16_path"] = str(self.source_sc16_path)
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "MetadataRecord":
        """Creates a MetadataRecord instance from a dictionary."""
        # Convert the ISO 8601 string back to a datetime object
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        # Convert the string path back to a Path object
        data["source_sc16_path"] = Path(data["source_sc16_path"])
        return cls(**data)

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
class IQStatistics:
    """Represents the calculated power statistics from an IQ data file."""

    average: float
    max: float
    median: float
    std: float
    kurtosis: float
