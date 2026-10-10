"""Observe a completed real-artifact replay without deleting or modifying source data."""

import json
from pathlib import Path

from mobilityops.config import load_project_config
from mobilityops.manifest import utc_now, write_manifest_atomically
from mobilityops.storage import connect_local
from mobilityops.workflow import run_workflow

ROOT = Path(__file__).resolve().parents[1]


def totals(database):
    with connect_local(database, read_only=True) as connection:
        return {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "curated.accepted_trips",
                "curated.quarantined_trips",
                "analytics.fct_trip",
            )
        }


def main():
    database = ROOT / "data/db/real.duckdb"
    history = [
        json.loads(line)
        for line in (ROOT / "reports/local/workflow_attempts.jsonl").read_text().splitlines()
    ]
    first_runs = {}
    for event in history:
        if event["scope"] == "real" and event["outcome"] == "SUCCEEDED" and event["stage"] == "all":
            first_runs.setdefault(event["source_month"], event)
    if set(first_runs) != {"2026-01", "2026-02", "2026-03"}:
        raise ValueError("All three official months must complete before this replay exercise")
    before = totals(database)
    replay = run_workflow(
        load_project_config(ROOT / "config/project.yaml"),
        scope="real",
        month="2026-01",
        offline=True,
    )
    after = totals(database)
    ingestion = replay["results"]["ingestion"]
    if before != after or ingestion["accepted_inserted"] or ingestion["quarantined_inserted"]:
        raise ValueError("Real replay changed logical record/fact counts")
    if ingestion["replay_records"] != replay["results"]["verification"]["source_rows"]:
        raise ValueError("Replay audit does not account for every source key")
    incremental = []
    previous = 0
    for month, event in sorted(first_runs.items()):
        result = event["results"]
        accepted = result["verification"]["accepted_rows"]
        total_after = result["snapshot"]["accepted_rows"]
        if (
            result["ingestion"]["accepted_inserted"] != accepted
            or total_after - previous != accepted
        ):
            raise ValueError(
                "First monthly ingestion/fact increment disagrees with artifact counts"
            )
        incremental.append(
            {
                "month": month,
                "artifact_id": result["ingestion"]["artifact_id"],
                "expected_accepted_keys": accepted,
                "actual_accepted_inserted": result["ingestion"]["accepted_inserted"],
                "verified_fact_total_before": previous,
                "verified_fact_total_after": total_after,
                "workflow_run_id": event["run_id"],
                "elapsed_seconds": event["elapsed_seconds"],
            }
        )
        previous = total_after
    write_manifest_atomically(
        ROOT / "reports/evidence/real_incremental.json",
        {
            "status": "PASSED",
            "engine": "duckdb",
            "verified_at": utc_now(),
            "monthly_increments": incremental,
            "real_replay": {
                "month": "2026-01",
                "before": before,
                "after": after,
                "accepted_inserted": ingestion["accepted_inserted"],
                "quarantined_inserted": ingestion["quarantined_inserted"],
                "replay_records": ingestion["replay_records"],
                "run_id": replay["run_id"],
                "elapsed_seconds": replay["elapsed_seconds"],
            },
            "older_month_backfill": (
                "Isolated deterministic fixtures: reports/evidence/local_scenarios.json"
            ),
            "command": ".venv/Scripts/python.exe scripts/verify_real_replay.py",
        },
    )
    print("Real monthly increments and unchanged-artifact replay: PASSED")


if __name__ == "__main__":
    main()
