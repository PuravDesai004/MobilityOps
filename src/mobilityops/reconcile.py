"""Artifact-scoped gates plus an independently calculated raw-source charge reference."""

from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path
from typing import Any

from mobilityops.identity import sha256_file
from mobilityops.manifest import utc_now, write_manifest_atomically
from mobilityops.reference import eligible_source_charge
from mobilityops.storage import connect_local


def reconcile(database: Path, report: dict[str, Any], output_path: Path) -> dict[str, Any]:
    started = time.perf_counter()
    dataset, artifact = report["dataset_id"], report["artifact_id"]
    if sha256_file(Path(report["source_path"])) != artifact:
        raise ValueError("Source checksum changed before independent reconciliation")
    eligible_count, eligible_charge = eligible_source_charge(Path(report["source_path"]))
    if sha256_file(Path(report["source_path"])) != artifact:
        raise ValueError("Source checksum changed during independent reconciliation")
    with connect_local(database, read_only=True) as connection:

        def metric(table: str, count: str, money: str):
            return connection.execute(
                f"SELECT coalesce({count},0), coalesce({money},0) "
                f"FROM {table} WHERE dataset_id=? AND artifact_id=?",
                [dataset, artifact],
            ).fetchone()

        accepted, accepted_charge = metric(
            "curated.accepted_trips", "count(*)", "sum(recorded_charge)"
        )
        quarantined, _ = metric("curated.quarantined_trips", "count(*)", "sum(recorded_charge)")
        facts, fact_charge = metric("analytics.fct_trip", "count(*)", "sum(recorded_charge)")
        marts, mart_charge = metric(
            "analytics.mart_zone_hour", "sum(trip_count)", "sum(recorded_charge_sum)"
        )
        daily, daily_charge = metric(
            "analytics.mart_daily_reconciliation", "sum(source_count)", "sum(recorded_charge_sum)"
        )
        records = """(SELECT dataset_id, artifact_id, source_partition,
            trip_record_id, source_row_number
            FROM curated.accepted_trips UNION ALL SELECT dataset_id, artifact_id,
            source_partition, trip_record_id, source_row_number
            FROM curated.quarantined_trips)"""
        total, distinct, missing, low, high, distinct_rows, invalid = connection.execute(
            f"""SELECT count(*),
            count(DISTINCT trip_record_id), count(*) FILTER(WHERE trip_record_id IS NULL),
            min(source_row_number), max(source_row_number),
            count(DISTINCT source_row_number), count(*) FILTER(WHERE
              source_partition IS DISTINCT FROM ? OR trip_record_id IS DISTINCT FROM
              sha256(artifact_id || ':' || source_row_number::VARCHAR)) FROM {records}
            WHERE dataset_id=? AND artifact_id=?""",
            [report["source_partition"], dataset, artifact],
        ).fetchone()
        overlap = connection.execute(
            """SELECT count(*) FROM curated.accepted_trips a
            JOIN curated.quarantined_trips q USING(dataset_id,trip_record_id)
            WHERE a.dataset_id=? AND a.artifact_id=?""",
            [dataset, artifact],
        ).fetchone()[0]
        mismatched_fact_keys = connection.execute(
            """WITH accepted AS (
                SELECT trip_record_id FROM curated.accepted_trips
                WHERE dataset_id=? AND artifact_id=?
            ), facts AS (
                SELECT trip_record_id FROM analytics.fct_trip
                WHERE dataset_id=? AND artifact_id=?
            ) SELECT count(*) FROM (
                (SELECT * FROM accepted EXCEPT SELECT * FROM facts)
                UNION ALL
                (SELECT * FROM facts EXCEPT SELECT * FROM accepted))""",
            [dataset, artifact, dataset, artifact],
        ).fetchone()[0]
    differences = {
        "source_minus_dispositions": report["source_rows"] - accepted - quarantined,
        "accepted_minus_fact": accepted - facts,
        "accepted_fact_key_mismatches": mismatched_fact_keys,
        "fact_minus_mart": facts - marts,
        "source_minus_daily": report["source_rows"] - daily,
        "independent_eligible_minus_accepted": eligible_count - accepted,
        "independent_charge_minus_accepted": str(eligible_charge - accepted_charge),
        "accepted_charge_minus_fact": str(accepted_charge - fact_charge),
        "fact_charge_minus_mart": str(fact_charge - mart_charge),
        "accepted_charge_minus_daily": str(accepted_charge - daily_charge),
    }
    passed = (
        all(Decimal(str(value)) == 0 for value in differences.values())
        and total == distinct == distinct_rows == report["source_rows"]
        and missing == overlap == invalid == 0
        and (low, high) == ((1, report["source_rows"]) if total else (None, None))
    )
    result = {
        "status": "PASSED" if passed else "FAILED",
        "engine": "duckdb",
        "dataset_id": dataset,
        "artifact_id": artifact,
        "source_partition": report["source_partition"],
        "source_rows": report["source_rows"],
        "accepted_rows": accepted,
        "quarantined_rows": quarantined,
        "fact_rows": facts,
        "mart_trip_count": marts,
        "distinct_keys": distinct,
        "overlap_keys": overlap,
        "invalid_identity_rows": invalid,
        "recorded_charge_usd": str(accepted_charge),
        "independent_eligible_source_charge_usd": str(eligible_charge),
        "differences": differences,
        "policy_version": report["policy_version"],
        "verified_at": utc_now(),
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }
    write_manifest_atomically(output_path, result)
    if not passed:
        raise ValueError(f"Reconciliation failed: {differences}")
    return result
