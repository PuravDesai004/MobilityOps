import json
import shutil
from pathlib import Path

import pytest

import mobilityops.manifest as manifest_module
from mobilityops.config import load_project_config, load_yellow_taxi_contract
from mobilityops.fixture import generate_fixture
from mobilityops.manifest import (
    append_attempt_event,
    record_profiled_source,
    write_manifest_atomically,
)
from mobilityops.profile import profile_parquet


def _temporary_project_config(tmp_path: Path):
    config_dir = tmp_path / "config"
    contract_dir = config_dir / "contracts"
    contract_dir.mkdir(parents=True)
    shutil.copy2("config/project.yaml", config_dir / "project.yaml")
    shutil.copy2(
        "config/contracts/yellow_taxi_v1.yaml",
        contract_dir / "yellow_taxi_v1.yaml",
    )
    return load_project_config(config_dir / "project.yaml")


def test_profile_writes_manifest_and_history_without_claiming_classification(tmp_path):
    config = _temporary_project_config(tmp_path)
    contract = load_yellow_taxi_contract(config)
    parquet_path = tmp_path / "fixture.parquet"
    zone_path = tmp_path / "zones.csv"
    generate_fixture(parquet_path, zone_path)
    profile = profile_parquet(parquet_path)

    manifest = record_profiled_source(
        config=config,
        contract=contract,
        profile=profile,
        artifact_path=parquet_path,
        zone_lookup_path=zone_path,
        run_id="run-one",
        attempt_id="attempt-one",
    )
    saved_manifest = json.loads(config.resolve_path("manifest_json").read_text(encoding="utf-8"))
    history_path = config.resolve_path("attempt_history_jsonl")
    history = [json.loads(line) for line in history_path.read_text(encoding="utf-8").splitlines()]

    assert manifest["status"] == "PROFILED"
    assert saved_manifest["dataset_id"] == "fixture"
    assert saved_manifest["source_partition"] == "2026-01"
    assert saved_manifest["expected_row_count"] == 12
    assert saved_manifest["artifact_sha256"] == profile.source_artifact_id
    assert saved_manifest["observed_schema_hash"] == profile.schema_sha256
    assert saved_manifest["source_downloaded_at"] is None
    assert saved_manifest["zone_artifact_sha256"]
    assert "passenger_count" in saved_manifest["known_optional_fields"]
    assert len(history) == 1
    assert history[0]["run_id"] == "run-one"
    assert history[0]["attempt_id"] == "attempt-one"
    assert history[0]["outcome"] == "SUCCEEDED"
    assert history[0]["details"]["row_count"] == 12
    assert len(history[0]["details"]["first_trip_record_id"]) == 64
    assert len(history[0]["details"]["last_trip_record_id"]) == 64
    assert saved_manifest["status"] != "CLASSIFIED"


def test_failed_later_attempt_appends_history_without_downgrading_manifest(tmp_path):
    config = _temporary_project_config(tmp_path)
    contract = load_yellow_taxi_contract(config)
    parquet_path = tmp_path / "fixture.parquet"
    zone_path = tmp_path / "zones.csv"
    generate_fixture(parquet_path, zone_path)
    profile = profile_parquet(parquet_path)
    record_profiled_source(
        config=config,
        contract=contract,
        profile=profile,
        artifact_path=parquet_path,
        zone_lookup_path=zone_path,
        run_id="run-success",
        attempt_id="attempt-success",
    )
    manifest_path = config.resolve_path("manifest_json")
    before = manifest_path.read_bytes()

    append_attempt_event(
        config.resolve_path("attempt_history_jsonl"),
        {
            "event_type": "PROFILE_ATTEMPT",
            "outcome": "FAILED",
            "run_id": "run-failed",
            "attempt_id": "attempt-failed",
            "error_category": "OSError",
        },
    )

    after = manifest_path.read_bytes()
    events = [
        json.loads(line)
        for line in config.resolve_path("attempt_history_jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert after == before
    assert [event["outcome"] for event in events] == ["SUCCEEDED", "FAILED"]


def test_atomic_manifest_replace_failure_preserves_existing_file(tmp_path, monkeypatch):
    manifest_path = tmp_path / "source.json"
    write_manifest_atomically(manifest_path, {"status": "PROFILED", "revision": 1})
    before = manifest_path.read_bytes()

    def fail_replace(_source, _destination):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(manifest_module.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated replace failure"):
        write_manifest_atomically(manifest_path, {"status": "FAILED", "revision": 2})

    assert manifest_path.read_bytes() == before
    assert list(tmp_path.glob("*.tmp")) == []
    assert list(tmp_path.glob(".*.tmp")) == []
