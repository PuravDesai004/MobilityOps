"""Stable source-file and physical-row identity helpers."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def sha256_file(path: str | Path, chunk_bytes: int = 1024 * 1024) -> str:
    """Return the lowercase SHA-256 of the actual file bytes."""

    if chunk_bytes < 1:
        raise ValueError("chunk_bytes must be positive")

    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_bytes), b""):
            digest.update(chunk)
    return digest.hexdigest()


def trip_record_id(source_artifact_id: str, source_row_number: int) -> str:
    """Build a stable key from artifact SHA-256 and one-based physical row number.

    Run IDs, attempt IDs, and paths are deliberately not inputs to this function.
    """

    if not _SHA256_PATTERN.fullmatch(source_artifact_id):
        raise ValueError("source_artifact_id must be 64 lowercase hexadecimal characters")
    if isinstance(source_row_number, bool) or not isinstance(source_row_number, int):
        raise TypeError("source_row_number must be an integer")
    if source_row_number < 1:
        raise ValueError("source_row_number must be a one-based positive integer")

    identity_text = f"{source_artifact_id}:{source_row_number}".encode("utf-8")
    return hashlib.sha256(identity_text).hexdigest()
