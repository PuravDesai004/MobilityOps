"""Single Python classifier; immutable generations published through one JSON pointer."""

from __future__ import annotations

import csv
import json
import shutil
import time
import uuid
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from mobilityops.config import YellowTaxiContract
from mobilityops.contract import validate_required_columns
from mobilityops.identity import sha256_file
from mobilityops.manifest import append_attempt_event, utc_now, write_manifest_atomically
from mobilityops.policy import LONG_DURATION_SECONDS, ProvenanceError, classify_row, integer_value
from mobilityops.policy import decimal_value as decimal_value

POLICY_VERSION = "python_v1_half_up"
OUTPUT_SCHEMA = pa.schema(
    [
        ("dataset_id", pa.string()),
        ("artifact_id", pa.string()),
        ("source_partition", pa.string()),
        ("source_row_number", pa.int64()),
        ("trip_record_id", pa.string()),
        ("parent_artifact_id", pa.string()),
        ("parent_source_row_number", pa.int64()),
        ("pickup_at", pa.timestamp("us")),
        ("dropoff_at", pa.timestamp("us")),
        ("pickup_zone_source_id", pa.int64()),
        ("dropoff_zone_source_id", pa.int64()),
        ("pickup_zone_key", pa.int64()),
        ("dropoff_zone_key", pa.int64()),
        ("passenger_count", pa.int64()),
        ("payment_type", pa.int64()),
        ("distance_miles", pa.decimal128(18, 3)),
        ("recorded_charge", pa.decimal128(18, 2)),
        ("fare_amount", pa.decimal128(18, 2)),
        ("tip_amount", pa.decimal128(18, 2)),
        ("cbd_congestion_fee", pa.decimal128(18, 2)),
        ("duration_seconds", pa.float64()),
        ("warning_codes", pa.list_(pa.string())),
        ("reason_codes", pa.list_(pa.string())),
        ("raw_payload_json", pa.string()),
    ]
)


def read_zones(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    seen = {0}
    result = [
        {
            "zone_key": 0,
            "borough": "Unresolved",
            "zone_name": "Unresolved",
            "service_zone": "Unresolved",
        }
    ]
    for row in rows:
        key = integer_value(row["LocationID"])
        if key is None or key <= 0 or key in seen:
            raise ValueError("Zone lookup has invalid or duplicate LocationID")
        seen.add(key)
        result.append(
            {
                "zone_key": key,
                "borough": row["Borough"],
                "zone_name": row["Zone"],
                "service_zone": row["service_zone"],
            }
        )
    return result


def classify_artifact(
    *,
    source_path: Path,
    source_manifest: dict[str, Any],
    zone_path: Path,
    output_root: Path,
    contract: YellowTaxiContract,
    batch_rows: int = 10000,
    run_id: str | None = None,
    fail_after_batches: int | None = None,
    long_duration_seconds: int = LONG_DURATION_SECONDS,
) -> dict[str, Any]:
    """Only the final pointer makes an entire accepted/quarantine pair visible."""
    if batch_rows < 1 or long_duration_seconds < 1:
        raise ValueError("Batch size and duration threshold must be positive")
    started = time.perf_counter()
    attempt = str(uuid.uuid4())
    output_root.mkdir(parents=True, exist_ok=True)
    pointer = output_root / "classification.json"
    generation = output_root / "generations" / attempt
    history = output_root / "attempts.jsonl"
    event: dict[str, Any] = {
        "attempt_id": attempt,
        "run_id": run_id or attempt,
        "event_type": "CLASSIFICATION",
        "started_at": utc_now(),
    }
    try:
        artifact_id = sha256_file(source_path)
        if artifact_id != source_manifest.get("source_artifact_id"):
            raise ProvenanceError("Source bytes disagree with manifest artifact ID")
        dataset_id = source_manifest["dataset_id"]
        partition = source_manifest["source_partition"]
        zone_sha = sha256_file(zone_path)
        expected_zone_sha = source_manifest.get("zone_artifact_sha256")
        if expected_zone_sha and zone_sha != expected_zone_sha:
            raise ProvenanceError("Zone bytes disagree with source manifest")
        zones = read_zones(zone_path)
        zone_ids = {r["zone_key"] for r in zones if r["zone_key"] != 0}
        event.update(dataset_id=dataset_id, artifact_id=artifact_id)
        expected_rows = source_manifest.get("observed_row_count")
        if expected_rows is None:
            expected_rows = source_manifest.get("expected_row_count")
        if expected_rows is None:
            expected_rows = source_manifest["profile"]["row_count"]
        with pq.ParquetFile(source_path) as candidate:
            validate_required_columns(candidate.schema_arrow.names, contract)
            if candidate.metadata.num_rows != expected_rows:
                raise ProvenanceError("Source count disagrees with manifest")
        if pointer.exists():
            prior = json.loads(pointer.read_text(encoding="utf-8"))
            if prior["artifact_id"] != artifact_id:
                raise ProvenanceError(
                    "SOURCE_REVISION_REQUIRES_REVIEW: active classification differs"
                )
            if (
                prior["dataset_id"] != dataset_id
                or prior["source_partition"] != partition
                or prior["source_rows"] != expected_rows
                or prior.get("parent_artifact_id") != source_manifest.get("parent_artifact_id")
            ):
                raise ProvenanceError("Active classification provenance differs from manifest")
            if (
                prior["zone_sha256"] == zone_sha
                and prior["policy_version"] == POLICY_VERSION
                and prior["long_duration_seconds"] == long_duration_seconds
                and all(
                    sha256_file(Path(prior[k])) == prior[f"{k}_sha256"]
                    for k in ("accepted_path", "quarantined_path")
                )
            ):
                append_attempt_event(
                    history, dict(event, outcome="REUSED_VERIFIED_UNCHANGED", ended_at=utc_now())
                )
                return prior
        with pq.ParquetFile(source_path) as source:
            generation.mkdir(parents=True)
            accepted_path = generation / "accepted.parquet"
            quarantine_path = generation / "quarantined.parquet"
            counts = Counter()
            reasons: Counter[str] = Counter()
            warnings: Counter[str] = Counter()
            charge_sum = Decimal("0.00")
            with (
                pq.ParquetWriter(accepted_path, OUTPUT_SCHEMA, compression="zstd") as accepted,
                pq.ParquetWriter(quarantine_path, OUTPUT_SCHEMA, compression="zstd") as rejected,
            ):
                for batch_number, batch in enumerate(
                    source.iter_batches(batch_size=batch_rows, use_threads=False), 1
                ):
                    outputs: dict[str, list] = {"accepted": [], "quarantined": []}
                    for row in batch.to_pylist():
                        counts["source"] += 1
                        result = classify_row(
                            row,
                            artifact_id=artifact_id,
                            source_row_number=counts["source"],
                            dataset_id=dataset_id,
                            source_partition=partition,
                            zone_ids=zone_ids,
                            parent_artifact_id=source_manifest.get("parent_artifact_id"),
                            long_duration_seconds=long_duration_seconds,
                        )
                        disposition = "quarantined" if result["reason_codes"] else "accepted"
                        outputs[disposition].append(result)
                        counts[disposition] += 1
                        reasons.update(result["reason_codes"])
                        warnings.update(result["warning_codes"])
                        counts["warning_records"] += bool(result["warning_codes"])
                        if disposition == "accepted":
                            charge_sum += result["recorded_charge"]
                    for label, writer in [("accepted", accepted), ("quarantined", rejected)]:
                        if outputs[label]:
                            writer.write_table(pa.Table.from_pylist(outputs[label], OUTPUT_SCHEMA))
                    if fail_after_batches == batch_number:
                        raise RuntimeError("Controlled classification publication failure")
            if counts["source"] != expected_rows or sha256_file(source_path) != artifact_id:
                raise ProvenanceError("Source changed during classification")
        report = {
            "status": "CLASSIFIED",
            "policy_version": POLICY_VERSION,
            "long_duration_seconds": long_duration_seconds,
            "dataset_id": dataset_id,
            "scope": source_manifest.get(
                "dataset_scope", "fixture" if dataset_id == "fixture" else "real"
            ),
            "artifact_id": artifact_id,
            "source_partition": partition,
            "zone_sha256": zone_sha,
            "parent_artifact_id": source_manifest.get("parent_artifact_id"),
            "source_rows": counts["source"],
            "accepted_rows": counts["accepted"],
            "quarantined_rows": counts["quarantined"],
            "warning_records": counts["warning_records"],
            "reason_counts": dict(reasons),
            "warning_counts": dict(warnings),
            "recorded_charge": str(charge_sum),
            "source_path": str(source_path.resolve()),
            "accepted_path": str(accepted_path.resolve()),
            "quarantined_path": str(quarantine_path.resolve()),
            "accepted_path_sha256": sha256_file(accepted_path),
            "quarantined_path_sha256": sha256_file(quarantine_path),
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "classified_at": utc_now(),
        }
        write_manifest_atomically(generation / "report.json", report)
        write_manifest_atomically(pointer, report)
        append_attempt_event(
            history, dict(event, outcome="SUCCEEDED", ended_at=utc_now(), counts=dict(counts))
        )
        return report
    except Exception as error:
        if generation.exists() and not (
            pointer.exists()
            and json.loads(pointer.read_text(encoding="utf-8")).get("accepted_path")
            == str((generation / "accepted.parquet").resolve())
        ):
            if not generation.resolve().is_relative_to((output_root / "generations").resolve()):
                raise ProvenanceError("Unsafe classification cleanup path") from error
            shutil.rmtree(generation)
        append_attempt_event(
            history,
            dict(
                event,
                outcome="FAILED",
                ended_at=utc_now(),
                error=str(error),
                error_category=type(error).__name__,
            ),
        )
        raise
