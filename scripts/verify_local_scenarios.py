"""Run isolated local rollback, replay, new-artifact and older-date backfill demonstrations."""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mobilityops.classify import classify_artifact
from mobilityops.config import load_project_config, load_yellow_taxi_contract
from mobilityops.fixture import FIXTURE_SCHEMA, fixture_rows, generate_fixture
from mobilityops.identity import sha256_file
from mobilityops.manifest import utc_now, write_manifest_atomically
from mobilityops.reconcile import reconcile
from mobilityops.storage import connect_local, ingest_classified
from mobilityops.workflow import run_dbt

ROOT = Path(__file__).resolve().parents[1]


def table_counts(database):
    with connect_local(database, read_only=True) as connection:
        return {
            table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in [
                "curated.accepted_trips",
                "curated.quarantined_trips",
                "control.source_artifacts",
                "control.ingestion_audit",
            ]
        }


def main():
    config = load_project_config(ROOT / "config/project.yaml")
    contract = load_yellow_taxi_contract(config)
    scope_root = ROOT / ".checks" / "scenarios" / uuid.uuid4().hex
    scope_root.mkdir(parents=True)
    evidence = {
        "engine": "duckdb",
        "scope": "isolated_deterministic_fixture",
        "started_at": utc_now(),
        "command": sys.argv,
        "scenarios": [],
    }
    database = scope_root / "rollback.duckdb"
    source, zones = scope_root / "fixture.parquet", scope_root / "zones.csv"
    generate_fixture(source, zones)

    def classify(path, dataset, partition, count):
        return classify_artifact(
            source_path=path,
            zone_path=zones,
            output_root=scope_root / dataset,
            contract=contract,
            source_manifest={
                "source_artifact_id": sha256_file(path),
                "dataset_id": dataset,
                "source_partition": partition,
                "expected_row_count": count,
                "dataset_scope": "fixture",
                "zone_artifact_sha256": sha256_file(zones),
            },
        )

    report = classify(source, "fixture", "2026-01", 12)
    try:
        ingest_classified(database=database, report=report, zone_path=zones, inject_failure=True)
    except RuntimeError as error:
        failure = str(error)
    else:
        raise AssertionError("Expected controlled failure")
    before_retry = table_counts(database)
    assert set(before_retry.values()) == {0}
    retry = ingest_classified(database=database, report=report, zone_path=zones)
    after_retry = table_counts(database)
    assert list(after_retry.values()) == [8, 4, 1, 1]
    replay = ingest_classified(database=database, report=report, zone_path=zones)
    assert replay["accepted_inserted"] == replay["quarantined_inserted"] == 0
    evidence["scenarios"].append(
        {
            "scenario": "local_rollback_retry_replay",
            "status": "PASSED",
            "failure": failure,
            "after_rollback": before_retry,
            "after_retry": after_retry,
            "retry": retry,
            "replay": replay,
        }
    )

    incremental_db = scope_root / "incremental.duckdb"
    prior_fact = 0
    reports = []
    for month in ["2026-02", "2026-03", "2026-01"]:
        rows = fixture_rows()[:2]
        pickup = datetime.fromisoformat(f"{month}-05T08:00:00")
        for row in rows:
            row["tpep_pickup_datetime"] = pickup
            row["tpep_dropoff_datetime"] = pickup + timedelta(minutes=18)
        path = scope_root / f"{month}.parquet"
        pq.write_table(pa.Table.from_pylist(rows, FIXTURE_SCHEMA), path)
        classified = classify(path, f"fixture-month-{month}", month, 2)
        incoming = ingest_classified(database=incremental_db, report=classified, zone_path=zones)
        run_dbt(ROOT, incremental_db, scope_root / month, docs=False)
        with connect_local(incremental_db, read_only=True) as connection:
            fact = connection.execute("SELECT count(*) FROM analytics.fct_trip").fetchone()[0]
            dates = connection.execute(
                "SELECT min(pickup_date)::VARCHAR,max(pickup_date)::VARCHAR FROM analytics.fct_trip"
            ).fetchone()
        assert fact - prior_fact == incoming["accepted_inserted"] == 2
        prior_fact = fact
        verified = reconcile(incremental_db, classified, scope_root / month / "verification.json")
        reports.append(verified)
        evidence["scenarios"].append(
            {
                "scenario": "older_month_backfill" if month == "2026-01" else "new_month_artifact",
                "status": "PASSED",
                "fixture_month": month,
                "artifact_id": classified["artifact_id"],
                "fact_rows": fact,
                "new_accepted_keys": incoming["accepted_inserted"],
                "actual_date_coverage": list(dates),
            }
        )
    replay = ingest_classified(database=incremental_db, report=classified, zone_path=zones)
    run_dbt(ROOT, incremental_db, scope_root / "replay", docs=False)
    with connect_local(incremental_db, read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM analytics.fct_trip").fetchone()[0] == 6
    assert replay["accepted_inserted"] == replay["quarantined_inserted"] == 0
    evidence["scenarios"].append(
        {
            "scenario": "incremental_fact_replay",
            "status": "PASSED",
            "logical_records_added": 0,
            "fact_rows": 6,
            "audit": replay,
        }
    )
    evidence.update(status="PASSED", ended_at=utc_now())
    write_manifest_atomically(ROOT / "reports/evidence/local_scenarios.json", evidence)
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
