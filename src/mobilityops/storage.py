"""Transactional local ingestion. No SQL business classifier and no Stream emulation."""

from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import Any

import duckdb

from mobilityops.classify import read_zones
from mobilityops.identity import sha256_file
from mobilityops.manifest import append_attempt_event, utc_now


def connect_local(path: Path, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(path), read_only=read_only)
    connection.execute("SET threads=2")
    connection.execute("SET memory_limit='1GB'")
    connection.execute("SET preserve_insertion_order=false")
    return connection


def ingest_classified(
    *, database: Path, report: dict[str, Any], zone_path: Path, inject_failure: bool = False
) -> dict[str, Any]:
    """Commit both key sets, artifact receipt and successful audit together."""
    if inject_failure and report["scope"] != "fixture":
        raise ValueError("Failure injection is restricted to fixture scope")
    attempt = str(uuid.uuid4())
    start = time.perf_counter()
    event = {
        "attempt_id": attempt,
        "event_type": "LOCAL_INGESTION",
        "dataset_id": report["dataset_id"],
        "artifact_id": report["artifact_id"],
        "started_at": utc_now(),
        "engine": "duckdb",
    }
    history = database.with_suffix(".attempts.jsonl")
    connection = connect_local(database)
    try:
        if sha256_file(Path(report["source_path"])) != report["artifact_id"]:
            raise ValueError("Original source checksum no longer matches classification")
        for name in ["accepted_path", "quarantined_path"]:
            if sha256_file(Path(report[name])) != report[f"{name}_sha256"]:
                raise ValueError("Classification output checksum mismatch")
        if sha256_file(zone_path) != report["zone_sha256"]:
            raise ValueError("Classification zone checksum mismatch")
        connection.execute("CREATE SCHEMA IF NOT EXISTS curated")
        connection.execute("CREATE SCHEMA IF NOT EXISTS control")
        for table, filename in [
            ("accepted_trips", "accepted_path"),
            ("quarantined_trips", "quarantined_path"),
        ]:
            connection.execute(
                f"CREATE TABLE IF NOT EXISTS curated.{table} AS "
                "SELECT * FROM read_parquet(?) LIMIT 0",
                [report[filename]],
            )
            connection.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {table}_key "
                f"ON curated.{table}(dataset_id, trip_record_id)"
            )
        connection.execute("""CREATE TABLE IF NOT EXISTS control.source_artifacts (
            dataset_id VARCHAR, artifact_id VARCHAR, source_partition VARCHAR,
            scope VARCHAR, source_rows BIGINT, accepted_rows BIGINT, quarantined_rows BIGINT,
            recorded_charge DECIMAL(38,2), policy_version VARCHAR, zone_sha256 VARCHAR,
            PRIMARY KEY(dataset_id, artifact_id))""")
        connection.execute("""CREATE TABLE IF NOT EXISTS control.ingestion_audit (
            attempt_id VARCHAR PRIMARY KEY, dataset_id VARCHAR, artifact_id VARCHAR,
            accepted_inserted BIGINT, quarantined_inserted BIGINT, replay_records BIGINT,
            committed_at VARCHAR, elapsed_seconds DOUBLE)""")
        connection.execute("""CREATE TABLE IF NOT EXISTS curated.zones (
            zone_key BIGINT PRIMARY KEY, borough VARCHAR, zone_name VARCHAR,
            service_zone VARCHAR, zone_sha256 VARCHAR)""")
        # Validate identities and complete coverage in the incoming pair, before touching targets.
        connection.execute(
            "CREATE TEMP VIEW incoming_a AS SELECT * FROM read_parquet('"
            + report["accepted_path"].replace("'", "''")
            + "')"
        )
        connection.execute(
            "CREATE TEMP VIEW incoming_q AS SELECT * FROM read_parquet('"
            + report["quarantined_path"].replace("'", "''")
            + "')"
        )
        incoming = connection.execute(
            """WITH incoming AS (
            SELECT * FROM incoming_a UNION ALL SELECT * FROM incoming_q)
            SELECT count(*), count(DISTINCT trip_record_id), min(source_row_number),
              max(source_row_number), count(*) FILTER (WHERE
                dataset_id <> ? OR artifact_id <> ? OR source_partition <> ? OR
                trip_record_id IS NULL OR source_row_number IS NULL OR
                trip_record_id <> sha256(artifact_id || ':' || source_row_number::VARCHAR))
            FROM incoming""",
            [report["dataset_id"], report["artifact_id"], report["source_partition"]],
        ).fetchone()
        n = report["source_rows"]
        if incoming != (n, n, 1, n, 0):
            raise ValueError(f"Incoming identity/coverage failed: {incoming}")
        invalid = connection.execute(
            """SELECT
            (SELECT count(*) FROM incoming_a WHERE len(reason_codes)<>0 OR
              pickup_at IS NULL OR dropoff_at IS NULL OR distance_miles IS NULL OR
              recorded_charge IS NULL OR duration_seconds < 0 OR distance_miles < 0)
            + (SELECT count(*) FROM incoming_q WHERE len(reason_codes)=0)
            + (SELECT abs(count(*) - ?) FROM incoming_a)
            + (SELECT abs(count(*) - ?) FROM incoming_q)""",
            [report["accepted_rows"], report["quarantined_rows"]],
        ).fetchone()[0]
        if invalid:
            raise ValueError("Classification disposition/count validation failed")
        connection.execute("BEGIN TRANSACTION")
        prior = connection.execute(
            """SELECT artifact_id, policy_version, zone_sha256
            FROM control.source_artifacts WHERE dataset_id=? AND source_partition=?""",
            [report["dataset_id"], report["source_partition"]],
        ).fetchall()
        if prior and prior != [
            (report["artifact_id"], report["policy_version"], report["zone_sha256"])
        ]:
            raise ValueError("SOURCE_REVISION_REQUIRES_REVIEW: active scope/policy changed")
        overlap = connection.execute("""SELECT count(*) FROM (
            SELECT a.trip_record_id FROM incoming_a a JOIN curated.quarantined_trips q
              USING(dataset_id,trip_record_id)
            UNION ALL
            SELECT q.trip_record_id FROM incoming_q q JOIN curated.accepted_trips a
              USING(dataset_id,trip_record_id))""").fetchone()[0]
        if overlap:
            raise ValueError("Accepted and quarantined key sets would overlap")
        changed_payload = connection.execute("""SELECT COUNT(*) FROM (
            SELECT i.trip_record_id FROM incoming_a i JOIN curated.accepted_trips t
              USING(dataset_id,trip_record_id) WHERE i.raw_payload_json<>t.raw_payload_json
            UNION ALL
            SELECT i.trip_record_id FROM incoming_q i JOIN curated.quarantined_trips t
              USING(dataset_id,trip_record_id) WHERE i.raw_payload_json<>t.raw_payload_json)
            """).fetchone()[0]
        if changed_payload:
            raise ValueError("Existing stable key has a conflicting original payload")
        inserted = {}
        for label, view, table in [
            ("accepted", "incoming_a", "accepted_trips"),
            ("quarantined", "incoming_q", "quarantined_trips"),
        ]:
            count = connection.execute(f"""SELECT count(*) FROM {view} i WHERE NOT EXISTS
                (SELECT 1 FROM curated.{table} t WHERE t.dataset_id=i.dataset_id AND
                 t.trip_record_id=i.trip_record_id)""").fetchone()[0]
            connection.execute(f"""INSERT INTO curated.{table} SELECT * FROM {view} i
                WHERE NOT EXISTS (SELECT 1 FROM curated.{table} t
                  WHERE t.dataset_id=i.dataset_id AND t.trip_record_id=i.trip_record_id)""")
            inserted[label] = count
            if inject_failure and label == "accepted":
                raise RuntimeError("Controlled fixture ingestion failure before commit")
        zones = read_zones(zone_path)
        existing_zones = connection.execute("SELECT DISTINCT zone_sha256 FROM curated.zones")
        zone_versions = existing_zones.fetchall()
        if zone_versions and zone_versions != [(report["zone_sha256"],)]:
            raise ValueError("Zone revision requires review")
        connection.executemany(
            "INSERT INTO curated.zones VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",
            [
                [
                    r["zone_key"],
                    r["borough"],
                    r["zone_name"],
                    r["service_zone"],
                    report["zone_sha256"],
                ]
                for r in zones
            ],
        )
        connection.execute(
            "INSERT INTO control.source_artifacts VALUES (?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT DO NOTHING",
            [
                report[k]
                for k in [
                    "dataset_id",
                    "artifact_id",
                    "source_partition",
                    "scope",
                    "source_rows",
                    "accepted_rows",
                    "quarantined_rows",
                    "recorded_charge",
                    "policy_version",
                    "zone_sha256",
                ]
            ],
        )
        elapsed = round(time.perf_counter() - start, 3)
        replay = n - inserted["accepted"] - inserted["quarantined"]
        connection.execute(
            "INSERT INTO control.ingestion_audit VALUES (?,?,?,?,?,?,?,?)",
            [
                attempt,
                report["dataset_id"],
                report["artifact_id"],
                inserted["accepted"],
                inserted["quarantined"],
                replay,
                utc_now(),
                elapsed,
            ],
        )
        connection.execute("COMMIT")
        event.update(
            outcome="COMMITTED",
            accepted_inserted=inserted["accepted"],
            quarantined_inserted=inserted["quarantined"],
            replay_records=replay,
            elapsed_seconds=elapsed,
            ended_at=utc_now(),
        )
        append_attempt_event(history, event)
        return event
    except Exception as error:
        try:
            connection.execute("ROLLBACK")
        except duckdb.TransactionException:
            pass
        append_attempt_event(
            history, dict(event, outcome="FAILED", ended_at=utc_now(), error=str(error))
        )
        raise
    finally:
        connection.close()
