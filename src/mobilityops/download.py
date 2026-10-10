"""Streaming, validated, atomic downloads for official local source artifacts."""

from __future__ import annotations

import csv
import hashlib
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import requests

from mobilityops.config import YellowTaxiContract
from mobilityops.contract import validate_required_columns

_RETRYABLE_STATUSES = {408, 425, 429, 500, 502, 503, 504}
_RETRYABLE_EXCEPTIONS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)


class DownloadError(RuntimeError):
    """Base class for source retrieval and validation failures."""


class HttpStatusError(DownloadError):
    """An HTTP status that should not be retried or was exhausted."""


class InvalidArtifactError(DownloadError):
    """Downloaded bytes did not satisfy the configured artifact validator."""


class SourceRevisionRequiresReview(DownloadError):
    """A valid active monthly artifact differs from the newly verified source."""


@dataclass(frozen=True)
class DownloadResult:
    url: str
    path: str
    outcome: str
    source_artifact_id: str
    file_size_bytes: int
    request_attempts: int
    downloaded_at: str
    etag: str | None
    last_modified: str | None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_parquet_file(
    path: str | Path,
    contract: YellowTaxiContract | None = None,
    *,
    batch_rows: int = 10_000,
) -> None:
    """Read Parquet metadata and every data page in bounded Arrow batches."""

    if batch_rows < 1:
        raise ValueError("batch_rows must be positive")
    parquet_path = Path(path)
    try:
        file_size = parquet_path.stat().st_size
        if file_size < 12:
            raise InvalidArtifactError("not readable Parquet: file is too small")
        with parquet_path.open("rb") as stream:
            header_magic = stream.read(4)
            stream.seek(-4, os.SEEK_END)
            footer_magic = stream.read(4)
    except OSError as error:
        raise InvalidArtifactError(f"not readable Parquet: {error}") from error
    if header_magic != b"PAR1" or footer_magic != b"PAR1":
        raise InvalidArtifactError("not readable Parquet: PAR1 header/footer markers are missing")

    parquet_file = None
    try:
        try:
            parquet_file = pq.ParquetFile(parquet_path)
            metadata = parquet_file.metadata
            schema = parquet_file.schema_arrow
        except Exception as error:
            raise InvalidArtifactError(f"not readable Parquet: {error}") from error
        if metadata.num_rows < 1:
            raise InvalidArtifactError("Parquet artifact contains no source rows")
        if contract is not None:
            validate_required_columns(schema.names, contract)
        observed_rows = 0
        try:
            for batch in parquet_file.iter_batches(batch_size=batch_rows, use_threads=False):
                observed_rows += batch.num_rows
        except Exception as error:
            raise InvalidArtifactError(f"Parquet data pages are not readable: {error}") from error
        if observed_rows != metadata.num_rows:
            raise InvalidArtifactError(
                f"Parquet data scan returned {observed_rows} rows; "
                f"metadata reports {metadata.num_rows}"
            )
    finally:
        if parquet_file is not None:
            parquet_file.close()


def validate_zone_lookup_csv(path: str | Path) -> None:
    """Read and validate the official TLC taxi-zone lookup header."""

    try:
        with Path(path).open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.reader(stream)
            header = next(reader, None)
            if header is None:
                raise InvalidArtifactError("zone lookup CSV is empty")
            required = {"LocationID", "Borough", "Zone", "service_zone"}
            if not required.issubset(set(header)):
                observed = ", ".join(header) or "<empty>"
                raise InvalidArtifactError(
                    "zone lookup CSV is missing required header fields; "
                    f"expected {', '.join(sorted(required))}; observed {observed}"
                )
            row_count = 0
            for row_count, row in enumerate(reader, start=1):
                if len(row) != len(header):
                    raise InvalidArtifactError(
                        f"zone lookup CSV row {row_count + 1} has {len(row)} fields; "
                        f"expected {len(header)}"
                    )
    except (OSError, UnicodeError, csv.Error) as error:
        raise InvalidArtifactError(f"not readable zone lookup CSV: {error}") from error
    if row_count == 0:
        raise InvalidArtifactError("zone lookup CSV has no data rows")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _prior_value(receipt: dict[str, Any] | None, *keys: str) -> Any:
    if receipt is None:
        return None
    for key in keys:
        value = receipt.get(key)
        if value is not None:
            return value
    return None


def _validator_succeeds(path: Path, validator: Callable[[Path], None]) -> bool:
    try:
        validator(path)
    except Exception:
        return False
    return True


def download_artifact(
    *,
    url: str,
    destination: str | Path,
    validator: Callable[[Path], None],
    retry_attempts: int = 3,
    connect_timeout_seconds: float = 10,
    read_timeout_seconds: float = 60,
    retry_backoff_seconds: float = 0.5,
    session: Any | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    immutable_existing: bool = False,
    prior_receipt: dict[str, Any] | None = None,
) -> DownloadResult:
    """Stream a source into a sibling temporary file, validate, hash, then publish.

    Conditional requests reuse a previously verified artifact when the official host
    supports ETag or Last-Modified. Without validators, bytes are streamed and compared
    by checksum before the active file can be reused or a source revision is rejected.
    """

    if retry_attempts < 1:
        raise ValueError("retry_attempts must be at least one")
    if connect_timeout_seconds <= 0 or read_timeout_seconds <= 0:
        raise ValueError("download timeouts must be positive")
    if retry_backoff_seconds < 0:
        raise ValueError("retry_backoff_seconds must not be negative")

    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    http = session if session is not None else requests.Session()

    existing_valid = target.is_file() and _validator_succeeds(target, validator)
    existing_sha = _sha256_file(target) if existing_valid else None
    receipt_sha = _prior_value(prior_receipt, "source_artifact_id", "artifact_sha256")
    if (
        immutable_existing
        and existing_valid
        and prior_receipt is not None
        and receipt_sha is not None
        and receipt_sha != existing_sha
    ):
        raise SourceRevisionRequiresReview(
            "SOURCE_REVISION_REQUIRES_REVIEW: the local monthly artifact checksum differs "
            "from its last successful manifest; the prior artifact was preserved"
        )
    validators_match_existing = existing_valid and receipt_sha == existing_sha
    request_headers = {"Accept-Encoding": "identity"}
    if validators_match_existing:
        etag = _prior_value(prior_receipt, "etag")
        last_modified = _prior_value(prior_receipt, "last_modified")
        if etag:
            request_headers["If-None-Match"] = str(etag)
        elif last_modified:
            request_headers["If-Modified-Since"] = str(last_modified)

    last_error: Exception | None = None
    for attempt in range(1, retry_attempts + 1):
        temp_path = target.with_name(f".{target.name}.{uuid.uuid4().hex}.part")
        response = None
        try:
            response = http.get(
                url,
                headers=request_headers,
                stream=True,
                timeout=(connect_timeout_seconds, read_timeout_seconds),
            )
            status = int(response.status_code)
            if status == 304:
                if not validators_match_existing or not existing_valid or existing_sha is None:
                    raise InvalidArtifactError(
                        "server returned 304 without a matching verified local artifact"
                    )
                return DownloadResult(
                    url=url,
                    path=str(target),
                    outcome="REUSED_VERIFIED_UNCHANGED",
                    source_artifact_id=existing_sha,
                    file_size_bytes=target.stat().st_size,
                    request_attempts=attempt,
                    downloaded_at=str(
                        _prior_value(prior_receipt, "downloaded_at", "source_downloaded_at")
                        or _utc_now()
                    ),
                    etag=response.headers.get("ETag") or _prior_value(prior_receipt, "etag"),
                    last_modified=response.headers.get("Last-Modified")
                    or _prior_value(prior_receipt, "last_modified"),
                )
            if status in _RETRYABLE_STATUSES:
                last_error = HttpStatusError(f"transient HTTP {status} while downloading {url}")
                if attempt < retry_attempts:
                    sleep_fn(retry_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                raise last_error
            if status < 200 or status >= 300:
                raise HttpStatusError(f"HTTP {status} while downloading {url}; not retrying")

            digest = hashlib.sha256()
            byte_count = 0
            with temp_path.open("xb") as stream:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    stream.write(chunk)
                    digest.update(chunk)
                    byte_count += len(chunk)
                stream.flush()
                os.fsync(stream.fileno())

            expected_length = response.headers.get("Content-Length")
            if expected_length is not None:
                try:
                    expected_bytes = int(expected_length)
                except (TypeError, ValueError):
                    expected_bytes = None
                if expected_bytes is not None and expected_bytes != byte_count:
                    raise requests.exceptions.ChunkedEncodingError(
                        "incomplete response: expected "
                        f"{expected_bytes} bytes, received {byte_count}"
                    )
            if byte_count == 0:
                raise InvalidArtifactError("downloaded artifact is empty")

            try:
                validator(temp_path)
            except Exception as error:
                detail = str(error) or type(error).__name__
                raise InvalidArtifactError(
                    f"downloaded content failed artifact validation: {detail}"
                ) from error

            downloaded_sha = digest.hexdigest()
            if existing_valid and existing_sha == downloaded_sha:
                return DownloadResult(
                    url=url,
                    path=str(target),
                    outcome="REUSED_VERIFIED_UNCHANGED",
                    source_artifact_id=existing_sha,
                    file_size_bytes=target.stat().st_size,
                    request_attempts=attempt,
                    downloaded_at=str(
                        _prior_value(prior_receipt, "downloaded_at", "source_downloaded_at")
                        or _utc_now()
                    ),
                    etag=response.headers.get("ETag") or _prior_value(prior_receipt, "etag"),
                    last_modified=response.headers.get("Last-Modified")
                    or _prior_value(prior_receipt, "last_modified"),
                )
            if immutable_existing and existing_valid:
                raise SourceRevisionRequiresReview(
                    "SOURCE_REVISION_REQUIRES_REVIEW: the configured monthly source has a new "
                    "SHA-256; the prior valid artifact was preserved"
                )

            os.replace(temp_path, target)
            return DownloadResult(
                url=url,
                path=str(target),
                outcome="DOWNLOADED",
                source_artifact_id=downloaded_sha,
                file_size_bytes=byte_count,
                request_attempts=attempt,
                downloaded_at=_utc_now(),
                etag=response.headers.get("ETag"),
                last_modified=response.headers.get("Last-Modified"),
            )
        except _RETRYABLE_EXCEPTIONS as error:
            last_error = error
            if attempt >= retry_attempts:
                raise DownloadError(
                    f"download failed after {attempt} attempts ({type(error).__name__})"
                ) from error
            sleep_fn(retry_backoff_seconds * (2 ** (attempt - 1)))
        finally:
            if response is not None:
                close = getattr(response, "close", None)
                if close is not None:
                    close()
            temp_path.unlink(missing_ok=True)

    assert last_error is not None
    raise DownloadError(str(last_error)) from last_error


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
