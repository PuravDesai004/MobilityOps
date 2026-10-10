"""Local orchestration of the same functions exposed by CLI and Airflow."""

from __future__ import annotations

import importlib.metadata
import json
import logging
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from mobilityops.classify import classify_artifact
from mobilityops.config import ProjectConfig, load_yellow_taxi_contract
from mobilityops.contract import validate_required_columns
from mobilityops.fixture import generate_fixture
from mobilityops.identity import sha256_file
from mobilityops.manifest import (
    append_attempt_event,
    record_profiled_source,
    utc_now,
    write_manifest_atomically,
)
from mobilityops.profile import profile_parquet
from mobilityops.real_source import prepare_real_source
from mobilityops.reconcile import reconcile
from mobilityops.snapshot import export_snapshot, load_snapshot
from mobilityops.storage import connect_local, ingest_classified

LOGGER = logging.getLogger(__name__)
STAGES = ("prepare", "classify", "ingest", "models", "verify", "snapshot")


def config_for_month(config: ProjectConfig, month: str) -> ProjectConfig:
    """Create a month-specific view; never rewrite the user's base configuration."""
    data = config.model_dump()
    old = config.source_month
    data["source_month"] = month
    for key, value in data["real_source"].items():
        if isinstance(value, str):
            data["real_source"][key] = value.replace(old, month)
    for key, value in data["paths"].items():
        if key.startswith(("real_", "sample_")):
            data["paths"][key] = value.replace(old, month)
    if month != old:
        data["paths"]["source_schema_json"] = f"reports/real/{month}/source_schema.json"
    result = ProjectConfig.model_validate(data)
    result._project_root = config.project_root
    return result


def runtime_versions() -> dict[str, str]:
    names = [
        "requests",
        "pyarrow",
        "duckdb",
        "PyYAML",
        "pydantic",
        "streamlit",
        "pandas",
        "plotly",
        "pytest",
        "ruff",
    ]
    return {
        "python": sys.version.split()[0],
        **{name: importlib.metadata.version(name) for name in names},
    }


def prepare(config: ProjectConfig, scope: str, *, offline: bool, run_id: str):
    contract = load_yellow_taxi_contract(config)
    if scope == "fixture":
        source, zones = (
            config.resolve_path("fixture_parquet"),
            config.resolve_path("zone_lookup_csv"),
        )
        if not source.exists() or not zones.exists():
            generate_fixture(source, zones)
        profile = profile_parquet(source, batch_rows=config.chunk_rows)
        validate_required_columns((f.name for f in profile.fields), contract)
        manifest_path = config.resolve_path("manifest_json")
        if manifest_path.exists():
            prior = json.loads(manifest_path.read_text(encoding="utf-8"))
            if prior["source_artifact_id"] != profile.source_artifact_id:
                raise ValueError("SOURCE_REVISION_REQUIRES_REVIEW: fixture changed")
        manifest = record_profiled_source(
            config=config,
            contract=contract,
            profile=profile,
            artifact_path=source,
            zone_lookup_path=zones,
            run_id=run_id,
            attempt_id=str(uuid.uuid4()),
        )
        manifest["dataset_scope"] = "fixture"
        return source, zones, manifest
    if not offline:
        prepare_real_source(config=config, contract=contract, run_id=run_id)
    prefix = "sample" if scope == "sample" else "real"
    manifest_path = config.resolve_path(f"{prefix}_manifest_json")
    if not manifest_path.exists():
        raise ValueError("Prepared source manifest missing; rerun without --offline")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    source = config.resolve_path(f"{prefix}_parquet")
    zones = config.resolve_path("real_zone_lookup_csv")
    if sha256_file(source) != manifest["source_artifact_id"]:
        raise ValueError("SOURCE_REVISION_REQUIRES_REVIEW: prepared artifact bytes changed")
    validate_required_columns((f.name for f in profile_parquet(source).fields), contract)
    manifest["dataset_scope"] = scope
    if scope == "sample":
        parent = json.loads(config.resolve_path("real_manifest_json").read_text(encoding="utf-8"))
        if parent["source_artifact_id"] != manifest["parent_artifact_id"]:
            raise ValueError("Sample parent identity differs from real-source manifest")
        manifest["zone_artifact_sha256"] = parent["zone_artifact_sha256"]
    return source, zones, manifest


def run_dbt(root: Path, database: Path, report_root: Path, *, docs: bool = True):
    """Subprocess uses dbt's environment and a profile containing only a local target."""
    interpreter = (
        root / ".venv-dbt-local" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    )
    if os.environ.get("MOBILITYOPS_DBT_PYTHON"):
        interpreter = Path(os.environ["MOBILITYOPS_DBT_PYTHON"])
    # CI/clean environments may put application and dbt dependencies together locally.
    if not interpreter.is_file():
        interpreter = Path(sys.executable)
    environment = dict(
        os.environ,
        MOBILITYOPS_DB_PATH=str(database.resolve()),
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
    )
    # dbt's interpreter owns its dependencies; do not leak the application's source metadata.
    environment.pop("PYTHONPATH", None)
    report_root.mkdir(parents=True, exist_ok=True)
    outcomes = []
    for subcommand in (["build"], ["docs", "generate"]) if docs else (["build"],):
        command = [
            str(interpreter),
            str(root / "scripts/dbt_local.py"),
            *subcommand,
            "--project-dir",
            str(root / "dbt"),
            "--profiles-dir",
            str(root / "dbt"),
            "--target",
            "local",
            "--target-path",
            str(report_root / "dbt_target"),
            "--log-path",
            str(report_root / "dbt_logs"),
        ]
        log_path = report_root / ("dbt_" + "_".join(subcommand) + ".log")
        started = time.perf_counter()
        LOGGER.info("dbt %s --target local", " ".join(subcommand))
        with log_path.open("w", encoding="utf-8") as stream:
            process = subprocess.run(
                command,
                cwd=root,
                env=environment,
                stdout=stream,
                stderr=subprocess.STDOUT,
                timeout=1800,
                check=False,
            )
        outcome = {
            "command": command,
            "exit_code": process.returncode,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }
        outcomes.append(outcome)
        if process.returncode:
            raise RuntimeError(f"Local dbt failed. See {log_path}")
    write_manifest_atomically(
        report_root / "dbt_execution.json", {"outcomes": outcomes, "target": "local"}
    )
    return outcomes


def run_workflow(
    config: ProjectConfig,
    *,
    scope: str,
    month: str | None = None,
    offline: bool = False,
    stage: str = "all",
    database: Path | None = None,
    snapshot_root: Path | None = None,
) -> dict[str, Any]:
    """Dependency gates fail closed. Earlier completed generations survive later failures."""
    if scope not in {"fixture", "sample", "real"} or stage not in (*STAGES, "all"):
        raise ValueError("Unknown workflow scope or stage")
    if month:
        config = config_for_month(config, month)
    root = config.project_root
    run_id = str(uuid.uuid4())
    partition = "fixture" if scope == "fixture" else config.source_month
    report_root = root / "reports" / "local" / scope / partition
    output_root = root / "data" / "classified" / scope / partition
    database = database or root / "data" / "db" / f"{scope}.duckdb"
    snapshot_root = snapshot_root or root / "demo" / ("snapshots" if scope == "real" else scope)
    history = root / "reports" / "local" / "workflow_attempts.jsonl"
    started = time.perf_counter()
    event: dict[str, Any] = {
        "run_id": run_id,
        "scope": scope,
        "source_month": config.source_month,
        "stage": stage,
        "started_at": utc_now(),
        "versions": runtime_versions(),
        "command": sys.argv,
        "database": str(database.resolve()),
    }
    results = {}
    try:
        LOGGER.info("Starting %s scope %s", scope, partition)
        source, zones, manifest = prepare(
            config, scope, offline=offline or stage != "all" and stage != "prepare", run_id=run_id
        )
        if stage in {"all", "prepare"}:
            results["prepare"] = {
                "artifact_id": manifest["source_artifact_id"],
                "dataset_id": manifest["dataset_id"],
            }
        if stage in {"all", "classify"}:
            LOGGER.info("Classifying %s", manifest["dataset_id"])
            report = classify_artifact(
                source_path=source,
                source_manifest=manifest,
                zone_path=zones,
                output_root=output_root,
                contract=load_yellow_taxi_contract(config),
                batch_rows=config.chunk_rows,
                run_id=run_id,
            )
            results["classification"] = {
                k: report[k]
                for k in (
                    "source_rows",
                    "accepted_rows",
                    "quarantined_rows",
                    "recorded_charge",
                    "elapsed_seconds",
                )
            }
        elif stage != "prepare":
            report = json.loads((output_root / "classification.json").read_text(encoding="utf-8"))
            if report["artifact_id"] != manifest["source_artifact_id"]:
                raise ValueError("Classification artifact no longer matches prepared source")
        if stage in {"all", "ingest"}:
            LOGGER.info("Ingesting classified outputs transactionally")
            results["ingestion"] = ingest_classified(
                database=database, report=report, zone_path=zones
            )
        if stage in {"all", "models"}:
            results["dbt"] = run_dbt(root, database, report_root)
        if stage in {"all", "verify"}:
            LOGGER.info(
                "Reconciling %s against independent source reference", manifest["dataset_id"]
            )
            results["verification"] = reconcile(database, report, report_root / "verification.json")
        if stage in {"all", "snapshot"}:
            # Refresh each active artifact's evidence against current fact and marts.
            verification_paths = []
            for pointer in sorted(
                (root / "data" / "classified" / scope).glob("*/classification.json")
            ):
                active = json.loads(pointer.read_text(encoding="utf-8"))
                with connect_local(database, read_only=True) as connection:
                    exists = connection.execute(
                        "SELECT count(*) FROM control.source_artifacts "
                        "WHERE dataset_id=? AND artifact_id=?",
                        [active["dataset_id"], active["artifact_id"]],
                    ).fetchone()[0]
                if exists:
                    path = (
                        root
                        / "reports"
                        / "local"
                        / scope
                        / pointer.parent.name
                        / "verification.json"
                    )
                    reconcile(database, active, path)
                    verification_paths.append(path)
            export_snapshot(
                database,
                snapshot_root,
                verification_paths=verification_paths,
                versions=runtime_versions(),
            )
            _, _, summary, _ = load_snapshot(snapshot_root)
            results["snapshot"] = {
                "path": str(snapshot_root),
                "status": "VALIDATED",
                "accepted_rows": summary["accepted_rows"],
            }
        event.update(
            outcome="SUCCEEDED",
            ended_at=utc_now(),
            elapsed_seconds=round(time.perf_counter() - started, 3),
            results=results,
        )
        write_manifest_atomically(report_root / "latest_workflow.json", event)
        append_attempt_event(history, event)
        LOGGER.info("Completed %s: %s", scope, results.get("classification", {}))
        return event
    except Exception as error:
        append_attempt_event(
            history,
            dict(
                event,
                outcome="FAILED",
                ended_at=utc_now(),
                elapsed_seconds=round(time.perf_counter() - started, 3),
                error=str(error),
                results=results,
            ),
        )
        raise
