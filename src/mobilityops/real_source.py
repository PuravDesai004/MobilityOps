"""Orchestrate official source download, batch profile, and isolated sampling."""

from __future__ import annotations

import csv
import json
import uuid
from pathlib import Path
from typing import Any

from mobilityops.catalog import verify_official_links
from mobilityops.config import ProjectConfig, YellowTaxiContract
from mobilityops.contract import validate_required_columns
from mobilityops.download import (
    DownloadResult,
    download_artifact,
    validate_parquet_file,
    validate_zone_lookup_csv,
)
from mobilityops.identity import trip_record_id
from mobilityops.manifest import append_attempt_event, utc_now, write_manifest_atomically
from mobilityops.profile import ParquetProfile, profile_parquet
from mobilityops.sample import SampleResult, create_deterministic_sample


def _read_json_if_present(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        content = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return content if isinstance(content, dict) else None


def _artifact_receipt(manifest: dict[str, Any] | None, name: str) -> dict[str, Any] | None:
    if manifest is None:
        return None
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        return None
    value = artifacts.get(name)
    return value if isinstance(value, dict) else None


def _schema_report(profile: ParquetProfile) -> dict[str, Any]:
    return {
        "file_name": profile.file_name,
        "file_size_bytes": profile.file_size_bytes,
        "row_count": profile.row_count,
        "row_group_count": profile.row_group_count,
        "schema_sha256": profile.schema_sha256,
        "fields": [field.__dict__ for field in profile.fields],
        "null_counts": profile.null_counts,
    }


def _zone_row_count(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream)
        next(reader, None)
        return sum(1 for _ in reader)


def _download_source(
    *,
    config: ProjectConfig,
    contract: YellowTaxiContract,
    prior_manifest: dict[str, Any] | None,
) -> tuple[DownloadResult, DownloadResult, ParquetProfile, dict[str, Any]]:
    source = config.real_source
    parquet_path = config.resolve_path("real_parquet")
    zone_path = config.resolve_path("real_zone_lookup_csv")

    parquet_download = download_artifact(
        url=source.parquet_url,
        destination=parquet_path,
        validator=lambda path: validate_parquet_file(path, contract, batch_rows=config.chunk_rows),
        retry_attempts=source.retry_attempts,
        connect_timeout_seconds=source.connect_timeout_seconds,
        read_timeout_seconds=source.read_timeout_seconds,
        retry_backoff_seconds=source.retry_backoff_seconds,
        immutable_existing=True,
        prior_receipt=_artifact_receipt(prior_manifest, "yellow_taxi_parquet"),
    )
    zone_download = download_artifact(
        url=source.zone_lookup_url,
        destination=zone_path,
        validator=validate_zone_lookup_csv,
        retry_attempts=source.retry_attempts,
        connect_timeout_seconds=source.connect_timeout_seconds,
        read_timeout_seconds=source.read_timeout_seconds,
        retry_backoff_seconds=source.retry_backoff_seconds,
        prior_receipt=_artifact_receipt(prior_manifest, "zone_lookup_csv"),
    )

    profile = profile_parquet(parquet_path, batch_rows=config.chunk_rows)
    check = validate_required_columns((field.name for field in profile.fields), contract)
    known_optional = list(check.present_optional_columns)
    created_at = (prior_manifest or {}).get("created_at") or utc_now()
    downloaded_at = parquet_download.downloaded_at
    manifest = {
        "manifest_version": 1,
        "dataset_id": source.dataset_id,
        "source_name": parquet_path.name,
        "source_url": source.parquet_url,
        "source_catalog_url": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
        "source_partition": config.source_month,
        "source_artifact_id": profile.source_artifact_id,
        "artifact_sha256": profile.source_artifact_id,
        "original_bytes": profile.file_size_bytes,
        "expected_row_count": None,
        "observed_row_count": profile.row_count,
        "observed_schema_hash": profile.schema_sha256,
        "contract_version": contract.contract_version,
        "source_downloaded_at": downloaded_at,
        "source_page_verified_on": utc_now()[:10],
        "local_path": parquet_path.resolve().relative_to(config.project_root).as_posix(),
        "zone_artifact_sha256": zone_download.source_artifact_id,
        "zone_artifact_version": f"sha256:{zone_download.source_artifact_id}",
        "known_optional_fields": known_optional,
        "additional_source_fields_preserved": list(check.additional_columns),
        "profile": profile.as_dict(),
        "zone_lookup": {
            "source_url": source.zone_lookup_url,
            "local_path": zone_path.resolve().relative_to(config.project_root).as_posix(),
            "sha256": zone_download.source_artifact_id,
            "file_size_bytes": zone_download.file_size_bytes,
            "row_count": _zone_row_count(zone_path),
        },
        "artifacts": {
            "yellow_taxi_parquet": {
                "source_artifact_id": parquet_download.source_artifact_id,
                "artifact_sha256": parquet_download.source_artifact_id,
                "etag": parquet_download.etag,
                "last_modified": parquet_download.last_modified,
                "downloaded_at": parquet_download.downloaded_at,
                "outcome": parquet_download.outcome,
            },
            "zone_lookup_csv": {
                "source_artifact_id": zone_download.source_artifact_id,
                "artifact_sha256": zone_download.source_artifact_id,
                "etag": zone_download.etag,
                "last_modified": zone_download.last_modified,
                "downloaded_at": zone_download.downloaded_at,
                "outcome": zone_download.outcome,
            },
        },
        "status": "PROFILED",
        "created_at": created_at,
        "profiled_at": utc_now(),
        "classification_run": False,
        "identity": {
            "source_artifact_id": profile.source_artifact_id,
            "source_row_number": "one-based physical source row number",
            "trip_record_id_input": "source_artifact_id:source_row_number",
        },
    }
    return parquet_download, zone_download, profile, manifest


def _sample_manifest(
    *,
    config: ProjectConfig,
    contract: YellowTaxiContract,
    sample: SampleResult,
    source_profile: ParquetProfile,
    prior_manifest: dict[str, Any] | None,
) -> dict[str, Any]:
    source = config.real_source
    path = config.resolve_path("sample_parquet")
    created_at = (prior_manifest or {}).get("created_at") or utc_now()
    first_source_row = sample.selected_source_row_numbers[0] if sample.row_count else None
    last_source_row = sample.selected_source_row_numbers[-1] if sample.row_count else None
    return {
        "manifest_version": 1,
        "dataset_id": source.sample_dataset_id,
        "dataset_scope": "sample",
        "source_name": path.name,
        "source_partition": config.source_month,
        "source_artifact_id": sample.sample_artifact_id,
        "artifact_sha256": sample.sample_artifact_id,
        "original_bytes": sample.profile.file_size_bytes,
        "expected_row_count": sample.row_count,
        "observed_schema_hash": sample.profile.schema_sha256,
        "contract_version": contract.contract_version,
        "local_path": path.resolve().relative_to(config.project_root).as_posix(),
        "parent_dataset_id": source.dataset_id,
        "parent_artifact_id": sample.parent_artifact_id,
        "parent_source_partition": config.source_month,
        "parent_row_count": source_profile.row_count,
        "sampling": {
            "method": sample.sampling_method,
            "limit_rows": source.sample_limit_rows,
            "sampled_rows": sample.row_count,
            "source_row_number_column": sample.source_row_number_column,
            "first_source_row_number": first_source_row,
            "last_source_row_number": last_source_row,
        },
        "profile": sample.profile.as_dict(),
        "identity": {
            "source_artifact_id": sample.sample_artifact_id,
            "source_row_number": "one-based physical row in this sample artifact",
            "trip_record_id_input": "sample_artifact_id:sample_physical_row_number",
            "parent_lineage_column": sample.source_row_number_column,
        },
        "status": "SAMPLED",
        "created_at": created_at,
        "sampled_at": utc_now(),
        "classification_run": False,
    }


def _sample_source(
    *,
    config: ProjectConfig,
    contract: YellowTaxiContract,
    source_profile: ParquetProfile,
    prior_sample_manifest: dict[str, Any] | None,
    run_id: str,
    attempt_id: str,
) -> tuple[SampleResult, dict[str, Any]]:
    sample = create_deterministic_sample(
        source_path=config.resolve_path("real_parquet"),
        sample_path=config.resolve_path("sample_parquet"),
        contract=contract,
        parent_artifact_id=source_profile.source_artifact_id,
        limit_rows=config.real_source.sample_limit_rows,
        batch_rows=config.chunk_rows,
    )
    manifest = _sample_manifest(
        config=config,
        contract=contract,
        sample=sample,
        source_profile=source_profile,
        prior_manifest=prior_sample_manifest,
    )
    write_manifest_atomically(config.resolve_path("sample_profile_json"), sample.profile.as_dict())
    write_manifest_atomically(config.resolve_path("sample_manifest_json"), manifest)
    first_sample_id = trip_record_id(sample.sample_artifact_id, 1) if sample.row_count else None
    last_sample_id = (
        trip_record_id(sample.sample_artifact_id, sample.row_count) if sample.row_count else None
    )
    append_attempt_event(
        config.resolve_path("sample_attempt_history_jsonl"),
        {
            "event_id": str(uuid.uuid4()),
            "event_type": "SAMPLE_ATTEMPT",
            "outcome": "SUCCEEDED",
            "run_id": run_id,
            "attempt_id": attempt_id,
            "occurred_at": utc_now(),
            "dataset_id": config.real_source.sample_dataset_id,
            "source_artifact_id": sample.sample_artifact_id,
            "parent_artifact_id": sample.parent_artifact_id,
            "error_category": None,
            "receipt_references": [
                config.resolve_path("sample_manifest_json").as_posix(),
                config.resolve_path("sample_profile_json").as_posix(),
            ],
            "details": {
                "row_count": sample.row_count,
                "parent_row_count": source_profile.row_count,
                "sampling_method": sample.sampling_method,
                "source_row_number_column": sample.source_row_number_column,
                "first_sample_trip_record_id": first_sample_id,
                "last_sample_trip_record_id": last_sample_id,
                "first_parent_trip_record_id": trip_record_id(
                    sample.parent_artifact_id, sample.selected_source_row_numbers[0]
                )
                if sample.row_count
                else None,
                "last_parent_trip_record_id": trip_record_id(
                    sample.parent_artifact_id, sample.selected_source_row_numbers[-1]
                )
                if sample.row_count
                else None,
            },
        },
    )
    return sample, manifest


def prepare_real_source(
    *,
    config: ProjectConfig,
    contract: YellowTaxiContract,
    run_id: str | None = None,
    attempt_id: str | None = None,
) -> dict[str, Any]:
    """Download/profile the active month, then create its separate deterministic sample."""

    resolved_run_id = run_id or str(uuid.uuid4())
    resolved_attempt_id = attempt_id or str(uuid.uuid4())
    started_at = utc_now()
    real_manifest_path = config.resolve_path("real_manifest_json")
    prior_real_manifest = _read_json_if_present(real_manifest_path)
    try:
        catalog_verified_on = verify_official_links(
            config.real_source.parquet_url, config.real_source.zone_lookup_url
        )
        parquet_result, zone_result, profile, real_manifest = _download_source(
            config=config,
            contract=contract,
            prior_manifest=prior_real_manifest,
        )
        real_manifest["source_page_verified_on"] = catalog_verified_on
        source_schema = _schema_report(profile)
        source_schema.update(
            {
                "dataset_id": config.real_source.dataset_id,
                "source_partition": config.source_month,
                "source_url": config.real_source.parquet_url,
                "source_artifact_id": profile.source_artifact_id,
                "contract_version": contract.contract_version,
                "contract_required_columns": contract.required_columns,
                "contract_present_optional_columns": real_manifest["known_optional_fields"],
                "additional_source_fields_preserved": real_manifest[
                    "additional_source_fields_preserved"
                ],
            }
        )
        write_manifest_atomically(config.resolve_path("source_schema_json"), source_schema)
        write_manifest_atomically(config.resolve_path("real_profile_json"), profile.as_dict())
        write_manifest_atomically(real_manifest_path, real_manifest)
        append_attempt_event(
            config.resolve_path("real_attempt_history_jsonl"),
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "SOURCE_PREPARATION_ATTEMPT",
                "outcome": "SUCCEEDED",
                "run_id": resolved_run_id,
                "attempt_id": resolved_attempt_id,
                "started_at": started_at,
                "ended_at": utc_now(),
                "dataset_id": config.real_source.dataset_id,
                "source_artifact_id": profile.source_artifact_id,
                "error_category": None,
                "receipt_references": [
                    str(real_manifest_path),
                    str(config.resolve_path("real_profile_json")),
                    str(config.resolve_path("source_schema_json")),
                ],
                "details": {
                    "parquet_outcome": parquet_result.outcome,
                    "parquet_request_attempts": parquet_result.request_attempts,
                    "zone_lookup_outcome": zone_result.outcome,
                    "zone_lookup_request_attempts": zone_result.request_attempts,
                    "row_count": profile.row_count,
                    "row_group_count": profile.row_group_count,
                    "file_size_bytes": profile.file_size_bytes,
                    "null_counts": profile.null_counts,
                    "first_trip_record_id": trip_record_id(profile.source_artifact_id, 1)
                    if profile.row_count
                    else None,
                    "last_trip_record_id": trip_record_id(
                        profile.source_artifact_id, profile.row_count
                    )
                    if profile.row_count
                    else None,
                },
            },
        )
    except Exception as error:
        append_attempt_event(
            config.resolve_path("real_attempt_history_jsonl"),
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "SOURCE_PREPARATION_ATTEMPT",
                "outcome": "FAILED",
                "run_id": resolved_run_id,
                "attempt_id": resolved_attempt_id,
                "started_at": started_at,
                "ended_at": utc_now(),
                "dataset_id": config.real_source.dataset_id,
                "source_artifact_id": None,
                "error_category": type(error).__name__,
                "error": str(error),
                "receipt_references": [],
                "details": {},
            },
        )
        raise

    prior_sample_manifest = _read_json_if_present(config.resolve_path("sample_manifest_json"))
    sample_started_at = utc_now()
    sample_attempt_id = str(uuid.uuid4())
    try:
        sample, sample_manifest = _sample_source(
            config=config,
            contract=contract,
            source_profile=profile,
            prior_sample_manifest=prior_sample_manifest,
            run_id=resolved_run_id,
            attempt_id=sample_attempt_id,
        )
    except Exception as error:
        append_attempt_event(
            config.resolve_path("sample_attempt_history_jsonl"),
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "SAMPLE_ATTEMPT",
                "outcome": "FAILED",
                "run_id": resolved_run_id,
                "attempt_id": sample_attempt_id,
                "started_at": sample_started_at,
                "ended_at": utc_now(),
                "dataset_id": config.real_source.sample_dataset_id,
                "source_artifact_id": None,
                "parent_artifact_id": profile.source_artifact_id,
                "error_category": type(error).__name__,
                "error": str(error),
                "receipt_references": [],
                "details": {},
            },
        )
        raise

    return {
        "real_source": {
            "dataset_id": config.real_source.dataset_id,
            "partition": config.source_month,
            "download": parquet_result.as_dict(),
            "zone_lookup_download": zone_result.as_dict(),
            "profile": profile.as_dict(),
            "manifest_path": str(real_manifest_path),
            "profile_path": str(config.resolve_path("real_profile_json")),
            "schema_report_path": str(config.resolve_path("source_schema_json")),
            "attempt_history_path": str(config.resolve_path("real_attempt_history_jsonl")),
            "status": real_manifest["status"],
        },
        "sample": {
            "dataset_id": config.real_source.sample_dataset_id,
            "result": sample.as_dict(),
            "manifest_path": str(config.resolve_path("sample_manifest_json")),
            "profile_path": str(config.resolve_path("sample_profile_json")),
            "attempt_history_path": str(config.resolve_path("sample_attempt_history_jsonl")),
            "status": sample_manifest["status"],
        },
        "classification_run": False,
    }
