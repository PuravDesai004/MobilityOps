"""Atomic source manifest and append-only local attempt history."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mobilityops.config import ProjectConfig, YellowTaxiContract
from mobilityops.identity import sha256_file, trip_record_id
from mobilityops.profile import ParquetProfile


def utc_now() -> str:
    """Return an ISO 8601 UTC timestamp."""

    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def write_manifest_atomically(path: str | Path, manifest: dict[str, Any]) -> None:
    """Replace a JSON manifest via a temporary file in the same directory."""

    manifest_path = Path(path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = (
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n"
    )
    temp_path = manifest_path.with_name(f".{manifest_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temp_path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, manifest_path)
    finally:
        temp_path.unlink(missing_ok=True)


def append_attempt_event(path: str | Path, event: dict[str, Any]) -> None:
    """Append one JSON object and newline without rewriting previous attempts."""

    history_path = Path(path)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(event, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n"
    descriptor = os.open(history_path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o666)
    with os.fdopen(descriptor, "a", encoding="utf-8", newline="\n") as stream:
        stream.write(line)
        stream.flush()
        os.fsync(stream.fileno())


def record_profiled_source(
    *,
    config: ProjectConfig,
    contract: YellowTaxiContract,
    profile: ParquetProfile,
    artifact_path: str | Path,
    zone_lookup_path: str | Path,
    run_id: str,
    attempt_id: str,
) -> dict[str, Any]:
    """Write a PROFILED source manifest and append a successful profile event."""

    artifact = Path(artifact_path).resolve()
    zone_path = Path(zone_lookup_path).resolve()
    try:
        local_path = artifact.relative_to(config.project_root).as_posix()
    except ValueError:
        local_path = str(artifact)

    existing_manifest_path = config.resolve_path("manifest_json")
    created_at = utc_now()
    if existing_manifest_path.is_file():
        try:
            prior = json.loads(existing_manifest_path.read_text(encoding="utf-8"))
            created_at = prior.get("created_at", created_at)
        except (OSError, json.JSONDecodeError):
            pass

    fields_present = {field.name for field in profile.fields}
    manifest: dict[str, Any] = {
        "manifest_version": 1,
        "dataset_id": config.dataset_id,
        "source_name": artifact.name,
        "source_url": None,
        "source_partition": config.source_month,
        "source_artifact_id": profile.source_artifact_id,
        "artifact_sha256": profile.source_artifact_id,
        "original_bytes": profile.file_size_bytes,
        "expected_row_count": profile.row_count,
        "observed_schema_hash": profile.schema_sha256,
        "contract_version": contract.contract_version,
        "source_downloaded_at": None,
        "local_path": local_path,
        "zone_artifact_sha256": sha256_file(zone_path),
        "zone_artifact_version": f"sha256:{sha256_file(zone_path)}",
        "known_optional_fields": [
            field_name for field_name in contract.optional_columns if field_name in fields_present
        ],
        "profile": {
            "row_count": profile.row_count,
            "row_group_count": profile.row_group_count,
            "file_size_bytes": profile.file_size_bytes,
            "fields": [
                {"name": field.name, "type": field.arrow_type, "nullable": field.nullable}
                for field in profile.fields
            ],
        },
        "status": "PROFILED",
        "created_at": created_at,
        "profiled_at": utc_now(),
    }
    write_manifest_atomically(existing_manifest_path, manifest)

    first_id = trip_record_id(profile.source_artifact_id, 1) if profile.row_count else None
    last_id = (
        trip_record_id(profile.source_artifact_id, profile.row_count) if profile.row_count else None
    )
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "PROFILE_ATTEMPT",
        "outcome": "SUCCEEDED",
        "run_id": run_id,
        "attempt_id": attempt_id,
        "occurred_at": utc_now(),
        "dataset_id": config.dataset_id,
        "source_artifact_id": profile.source_artifact_id,
        "error_category": None,
        "receipt_references": [],
        "details": {
            "row_count": profile.row_count,
            "row_group_count": profile.row_group_count,
            "schema_sha256": profile.schema_sha256,
            "first_trip_record_id": first_id,
            "last_trip_record_id": last_id,
        },
    }
    append_attempt_event(config.resolve_path("attempt_history_jsonl"), event)
    return manifest


def record_failed_profile_attempt(
    *,
    config: ProjectConfig,
    run_id: str,
    attempt_id: str,
    error_category: str,
) -> None:
    """Append failure history only; a failed attempt never downgrades the manifest."""

    append_attempt_event(
        config.resolve_path("attempt_history_jsonl"),
        {
            "event_id": str(uuid.uuid4()),
            "event_type": "PROFILE_ATTEMPT",
            "outcome": "FAILED",
            "run_id": run_id,
            "attempt_id": attempt_id,
            "occurred_at": utc_now(),
            "dataset_id": config.dataset_id,
            "source_artifact_id": None,
            "error_category": error_category,
            "receipt_references": [],
            "details": {},
        },
    )
