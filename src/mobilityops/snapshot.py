"""Small portable snapshots: immutable files, atomic manifest, strict offline validation."""

from __future__ import annotations

import json
import math
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from mobilityops.identity import sha256_file
from mobilityops.manifest import utc_now, write_manifest_atomically
from mobilityops.storage import connect_local

SNAPSHOT_VERSION = 1
METRIC_VERSION = "signed_charge_usd_half_up_v1"
ZONE_COLUMNS = {
    "dataset_id",
    "artifact_id",
    "source_partition",
    "pickup_date",
    "pickup_hour",
    "pickup_zone_key",
    "trip_count",
    "recorded_charge_sum",
    "distance_miles_sum",
    "distance_count",
    "duration_seconds_sum",
    "duration_count",
    "warning_trip_count",
    "zone_name",
    "borough",
}


def export_snapshot(
    database: Path, output_root: Path, *, verification_paths: list[Path], versions: dict[str, str]
) -> dict[str, Any]:
    reports = [json.loads(path.read_text(encoding="utf-8")) for path in verification_paths]
    if not reports or any(r["status"] != "PASSED" for r in reports):
        raise ValueError("Snapshots require passed per-artifact reconciliation reports")
    generation = output_root / "generations" / uuid.uuid4().hex
    generation.mkdir(parents=True)
    with connect_local(database, read_only=True) as connection:
        receipts = connection.execute("SELECT * FROM control.source_artifacts ORDER BY dataset_id")
        fields = [c[0] for c in receipts.description]
        artifacts = [dict(zip(fields, row, strict=True)) for row in receipts.fetchall()]
        if {(r["dataset_id"], r["artifact_id"]) for r in reports} != {
            (r["dataset_id"], r["artifact_id"]) for r in artifacts
        }:
            raise ValueError("Every active artifact needs a current verified report")
        zone_table = connection.execute("""SELECT m.*, z.zone_name, z.borough
            FROM analytics.mart_zone_hour m JOIN analytics.dim_zone z
            ON m.pickup_zone_key=z.zone_key ORDER BY dataset_id,pickup_date,pickup_hour,
            pickup_zone_key""").fetch_arrow_table()
        pq.write_table(zone_table, generation / "zone_hour.parquet", compression="zstd")
        daily = connection.execute(
            "SELECT * FROM analytics.mart_daily_reconciliation "
            "ORDER BY dataset_id,artifact_id,pickup_date_bucket"
        ).df()
        daily.to_csv(generation / "daily_reconciliation.csv", index=False)
        audits_cursor = connection.execute(
            "SELECT * FROM control.ingestion_audit ORDER BY committed_at"
        )
        audits_fields = [c[0] for c in audits_cursor.description]
        audits = [dict(zip(audits_fields, r, strict=True)) for r in audits_cursor.fetchall()]
        reasons = connection.execute("""SELECT code, count(*) as n FROM
            (SELECT unnest(reason_codes) as code FROM curated.quarantined_trips)
            GROUP BY code ORDER BY code""").fetchall()
        warnings = connection.execute("""SELECT code, count(*) as n FROM
            (SELECT unnest(warning_codes) as code FROM curated.accepted_trips)
            GROUP BY code ORDER BY code""").fetchall()
        coverage = connection.execute(
            "SELECT min(pickup_date)::VARCHAR, max(pickup_date)::VARCHAR FROM analytics.fct_trip"
        ).fetchone()
    scopes = {r["scope"] for r in artifacts}
    if len(scopes) != 1:
        raise ValueError("Snapshot scope mixes fixture/sample/real")
    scope = scopes.pop()
    summary = {
        "engine": "duckdb",
        "origin_engine": f"duckdb_{scope}",
        "scope": scope,
        "source_months": sorted({r["source_partition"] for r in artifacts}),
        "actual_date_coverage": list(coverage),
        "exported_at": utc_now(),
        "source_rows": sum(r["source_rows"] for r in artifacts),
        "accepted_rows": sum(r["accepted_rows"] for r in artifacts),
        "quarantined_rows": sum(r["quarantined_rows"] for r in artifacts),
        "recorded_charge_usd": str(sum((r["recorded_charge"] for r in artifacts), Decimal(0))),
        "artifacts": [{**r, "recorded_charge": str(r["recorded_charge"])} for r in artifacts],
        "reconciliation": reports,
        "ingestion_audit": audits,
        "reason_counts": dict(reasons),
        "accepted_warning_counts": dict(warnings),
        "versions": versions,
        "metric_version": METRIC_VERSION,
        "sampling_note": "Separate evenly spaced sample; not a full-source estimate"
        if scope == "sample"
        else None,
        "units": {
            "charge": "USD recorded passenger charge",
            "distance": "miles",
            "duration": "seconds",
            "timestamps": "America/New_York wall clock",
        },
    }
    scenario_path = Path(__file__).resolve().parents[2] / "reports/evidence/local_scenarios.json"
    if scenario_path.is_file():
        scenarios = json.loads(scenario_path.read_text(encoding="utf-8"))
        if scenarios.get("status") == "PASSED" and scenarios.get("engine") == "duckdb":
            summary["isolated_fixture_recovery_and_backfill"] = scenarios
    write_manifest_atomically(generation / "run_summary.json", summary)
    manifest = {
        "snapshot_version": SNAPSHOT_VERSION,
        "metric_version": METRIC_VERSION,
        "contract_version": "v1",
        "origin_engine": summary["origin_engine"],
        "scope": scope,
        "exported_at": summary["exported_at"],
        "files": {
            name: {
                "path": (generation / name).relative_to(output_root).as_posix(),
                "sha256": sha256_file(generation / name),
                "bytes": (generation / name).stat().st_size,
            }
            for name in ("zone_hour.parquet", "daily_reconciliation.csv", "run_summary.json")
        },
    }
    # Validate the candidate before publishing; a failure retains the previous visible snapshot.
    _load_validated(output_root, manifest)
    write_manifest_atomically(output_root / "snapshot_manifest.json", manifest)
    return manifest


def _load_validated(root: Path, manifest: dict[str, Any]):
    if (
        manifest.get("snapshot_version") != SNAPSHOT_VERSION
        or manifest.get("metric_version") != METRIC_VERSION
        or manifest.get("contract_version") != "v1"
    ):
        raise ValueError("Unsupported snapshot/metric version")
    paths = {}
    for name in ("zone_hour.parquet", "daily_reconciliation.csv", "run_summary.json"):
        receipt = manifest["files"][name]
        path = (root / receipt["path"]).resolve()
        if not path.is_relative_to(root.resolve()) or not path.is_file():
            raise ValueError(f"Missing or unsafe snapshot file: {name}")
        if path.stat().st_size != receipt["bytes"] or sha256_file(path) != receipt["sha256"]:
            raise ValueError(f"Snapshot checksum mismatch: {name}")
        paths[name] = path
    schema = pq.read_schema(paths["zone_hour.parquet"])
    if set(schema.names) != ZONE_COLUMNS:
        raise ValueError("Snapshot zone/hour schema mismatch")
    for name in ("trip_count", "duration_count", "distance_count", "warning_trip_count"):
        if not pa.types.is_integer(schema.field(name).type):
            raise ValueError("Snapshot count type mismatch")
    for name in ("recorded_charge_sum", "distance_miles_sum"):
        if not pa.types.is_decimal(schema.field(name).type):
            raise ValueError("Snapshot decimal type mismatch")
        expected_scale = 2 if name == "recorded_charge_sum" else 3
        if schema.field(name).type.scale != expected_scale:
            raise ValueError("Snapshot decimal scale mismatch")
    if not pa.types.is_date(schema.field("pickup_date").type):
        raise ValueError("Snapshot pickup date type mismatch")
    zone = pd.read_parquet(paths["zone_hour.parquet"])
    daily = pd.read_csv(paths["daily_reconciliation.csv"], dtype={"recorded_charge_sum": str})
    summary = json.loads(paths["run_summary.json"].read_text(encoding="utf-8"))
    expected_daily = {
        "dataset_id",
        "artifact_id",
        "source_partition",
        "pickup_date_bucket",
        "source_count",
        "accepted_count",
        "quarantined_count",
        "recorded_charge_sum",
    }
    if set(daily.columns) != expected_daily or daily.isna().any().any():
        raise ValueError("Snapshot reconciliation schema mismatch")
    if (
        summary["origin_engine"] != manifest["origin_engine"]
        or summary["scope"] != manifest["scope"]
    ):
        raise ValueError("Snapshot origin mismatch")
    if summary["scope"] not in {"fixture", "sample", "real"}:
        raise ValueError("Unsupported local snapshot scope")
    if (
        summary["engine"] != "duckdb"
        or summary["origin_engine"] != f"duckdb_{summary['scope']}"
        or summary["metric_version"] != METRIC_VERSION
    ):
        raise ValueError("Snapshot engine/metric declaration mismatch")
    if zone.isna().any().any():
        raise ValueError("Snapshot contains unexpected null aggregate fields")
    numeric_counts = ["trip_count", "duration_count", "distance_count", "warning_trip_count"]
    if any((zone[col] < 0).any() for col in numeric_counts):
        raise ValueError("Snapshot has invalid counts")
    for column in ("source_count", "accepted_count", "quarantined_count"):
        if not all(
            math.isfinite(value) and value >= 0 and value == int(value) for value in daily[column]
        ):
            raise ValueError("Snapshot daily count domain mismatch")
    if not all(math.isfinite(value) for value in zone.duration_seconds_sum):
        raise ValueError("Snapshot duration is nonfinite")
    if (
        (zone.warning_trip_count > zone.trip_count).any()
        or (zone.pickup_hour < 0).any()
        or (zone.pickup_hour > 23).any()
        or (zone.duration_seconds_sum < 0).any()
        or (daily.source_count != daily.accepted_count + daily.quarantined_count).any()
    ):
        raise ValueError("Snapshot domain validation failed")
    expected_keys = {(a["dataset_id"], a["artifact_id"]) for a in summary["artifacts"]}
    if len(expected_keys) != len(summary["artifacts"]):
        raise ValueError("Snapshot artifact registration is duplicated")
    if summary["source_months"] != sorted(
        {a["source_partition"] for a in summary["artifacts"]}
    ) or any(a["scope"] != summary["scope"] for a in summary["artifacts"]):
        raise ValueError("Snapshot scope/month coverage mismatch")
    coverage = (
        [str(zone.pickup_date.min()), str(zone.pickup_date.max())]
        if not zone.empty
        else [None, None]
    )
    if coverage != summary["actual_date_coverage"]:
        raise ValueError("Snapshot date coverage mismatch")
    if set(zip(daily.dataset_id, daily.artifact_id, strict=True)) != expected_keys:
        raise ValueError("Snapshot artifact coverage mismatch")
    if not set(zip(zone.dataset_id, zone.artifact_id, strict=True)).issubset(expected_keys):
        raise ValueError("Snapshot contains an unregistered zone artifact")
    if (
        zone.duplicated(
            [
                "dataset_id",
                "artifact_id",
                "source_partition",
                "pickup_date",
                "pickup_hour",
                "pickup_zone_key",
            ]
        ).any()
        or daily.duplicated(
            ["dataset_id", "artifact_id", "source_partition", "pickup_date_bucket"]
        ).any()
    ):
        raise ValueError("Snapshot aggregate grain is duplicated")
    if {(r["dataset_id"], r["artifact_id"]) for r in summary["reconciliation"]} != expected_keys:
        raise ValueError("Snapshot verification artifact coverage mismatch")
    for artifact in summary["artifacts"]:
        selected_zone = zone[
            (zone.dataset_id == artifact["dataset_id"])
            & (zone.artifact_id == artifact["artifact_id"])
        ]
        selected_daily = daily[
            (daily.dataset_id == artifact["dataset_id"])
            & (daily.artifact_id == artifact["artifact_id"])
        ]
        if (
            int(selected_zone.trip_count.sum()) != artifact["accepted_rows"]
            or int(selected_daily.source_count.sum()) != artifact["source_rows"]
            or int(selected_daily.accepted_count.sum()) != artifact["accepted_rows"]
            or int(selected_daily.quarantined_count.sum()) != artifact["quarantined_rows"]
            or filtered_metrics(selected_zone)["charge_usd"] != Decimal(artifact["recorded_charge"])
            or sum((Decimal(v) for v in selected_daily.recorded_charge_sum), Decimal(0))
            != Decimal(artifact["recorded_charge"])
            or not (selected_daily.source_partition == artifact["source_partition"]).all()
            or not (selected_zone.source_partition == artifact["source_partition"]).all()
        ):
            raise ValueError("Snapshot artifact reconciliation failed")
    if (zone["trip_count"] != zone["duration_count"]).any() or (
        zone["trip_count"] != zone["distance_count"]
    ).any():
        raise ValueError("Snapshot metric denominators disagree")
    checks = [
        int(zone.trip_count.sum()) == summary["accepted_rows"],
        int(daily.source_count.sum()) == summary["source_rows"],
        int(daily.accepted_count.sum()) == summary["accepted_rows"],
        int(daily.quarantined_count.sum()) == summary["quarantined_rows"],
        summary["source_rows"] == summary["accepted_rows"] + summary["quarantined_rows"],
        sum((Decimal(str(v)) for v in zone.recorded_charge_sum), Decimal(0))
        == Decimal(summary["recorded_charge_usd"]),
        sum((Decimal(v) for v in daily.recorded_charge_sum), Decimal(0))
        == Decimal(summary["recorded_charge_usd"]),
        all(r["status"] == "PASSED" for r in summary["reconciliation"]),
    ]
    if not all(checks):
        raise ValueError("Snapshot reconciliation failed")
    return zone, daily, summary, manifest


def load_snapshot(root: Path):
    """No network, credential access, database connection or pipeline invocation."""
    manifest_path = root / "snapshot_manifest.json"
    if not manifest_path.is_file():
        raise ValueError("Snapshot missing. Run scripts/run_local.ps1 -Scope fixture first.")
    try:
        return _load_validated(root, json.loads(manifest_path.read_text(encoding="utf-8")))
    except (KeyError, OSError, TypeError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid snapshot: {error}") from error


def filtered_metrics(zone: pd.DataFrame) -> dict[str, Any]:
    """Use additive numerators and denominators after filtering."""
    trips = int(zone.trip_count.sum())
    duration_count, distance_count = int(zone.duration_count.sum()), int(zone.distance_count.sum())
    charge = sum((Decimal(str(v)) for v in zone.recorded_charge_sum), Decimal(0))
    distance = sum((Decimal(str(v)) for v in zone.distance_miles_sum), Decimal(0))
    return {
        "trips": trips,
        "charge_usd": charge,
        "average_distance_miles": float(distance / distance_count) if distance_count else None,
        "average_duration_minutes": float(zone.duration_seconds_sum.sum()) / duration_count / 60
        if duration_count
        else None,
        "warning_trips": int(zone.warning_trip_count.sum()),
    }
