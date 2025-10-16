import hashlib
from pathlib import Path


def get_file_checksum(filepath: Path):
    hasher = hashlib.md5()
    with open(filepath, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_checksum(data: bytes):
    hasher = hashlib.md5()
    hasher.update(data)
    return hasher.hexdigest()
