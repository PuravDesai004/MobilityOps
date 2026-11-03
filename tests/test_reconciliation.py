import json
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from mobilityops.classify import classify_artifact
from mobilityops.identity import sha256_file
from mobilityops.reconcile import reconcile
from mobilityops.storage import connect_local, ingest_classified


def prepare_reconciliation_database(classified_fixture, tmp_path):
    report, _, zones = classified_fixture
    database = tmp_path / "reconciliation.duckdb"
    ingest_classified(database=database, report=report, zone_path=zones)
    with connect_local(database) as connection:
        connection.execute("CREATE SCHEMA analytics")
        connection.execute(
            "CREATE TABLE analytics.fct_trip AS SELECT * FROM curated.accepted_trips"
        )
        connection.execute("""CREATE TABLE analytics.mart_zone_hour AS
            SELECT dataset_id, artifact_id, count(*) trip_count,
                sum(recorded_charge) recorded_charge_sum
            FROM curated.accepted_trips GROUP BY dataset_id, artifact_id""")
        connection.execute("""CREATE TABLE analytics.mart_daily_reconciliation AS
            SELECT dataset_id, artifact_id, count(*) source_count,
                sum(CASE WHEN len(reason_codes)=0 THEN recorded_charge ELSE 0 END)
                    recorded_charge_sum
            FROM (SELECT * FROM curated.accepted_trips UNION ALL
                  SELECT * FROM curated.quarantined_trips)
            GROUP BY dataset_id, artifact_id""")
    return database, report


def test_reconciliation_passes_complete_source_rows(classified_fixture, tmp_path):
    database, report = prepare_reconciliation_database(classified_fixture, tmp_path)
    result = reconcile(database, report, tmp_path / "verification.json")
    assert result["status"] == "PASSED"
    assert result["invalid_identity_rows"] == 0


@pytest.mark.parametrize(
    "assignment",
    ["trip_record_id=repeat('b',64)", "source_row_number=2", "source_partition='2026-02'"],
)
def test_reconciliation_rejects_corrupted_identity_with_matching_totals(
    classified_fixture, tmp_path, assignment
):
    database, report = prepare_reconciliation_database(classified_fixture, tmp_path)
    with connect_local(database) as connection:
        connection.execute(
            "UPDATE curated.accepted_trips SET " + assignment + " WHERE source_row_number=3"
        )
    output = tmp_path / "verification.json"
    with pytest.raises(ValueError, match="Reconciliation failed"):
        reconcile(database, report, output)
    result = json.loads(output.read_text())
    assert result["status"] == "FAILED"
    assert all(
        float(value) == 0
        for name, value in result["differences"].items()
        if name != "accepted_fact_key_mismatches"
    )
    assert result["invalid_identity_rows"] == 1


def test_reconciliation_rejects_fact_keys_even_when_count_and_money_match(
    classified_fixture, tmp_path
):
    database, report = prepare_reconciliation_database(classified_fixture, tmp_path)
    with connect_local(database) as connection:
        connection.execute(
            "UPDATE analytics.fct_trip SET trip_record_id=repeat('b',64) WHERE source_row_number=3"
        )
    output = tmp_path / "verification.json"
    with pytest.raises(ValueError, match="Reconciliation failed"):
        reconcile(database, report, output)
    result = json.loads(output.read_text())
    assert result["differences"]["accepted_minus_fact"] == 0
    assert result["differences"]["accepted_charge_minus_fact"] == "0.00"
    assert result["differences"]["accepted_fact_key_mismatches"] == 2


def test_reconciliation_accepts_a_valid_empty_artifact(classified_fixture, tmp_path):
    _, arguments, zones = classified_fixture
    source = arguments["source_path"]
    pq.write_table(pq.read_table(source).slice(0, 0), source)
    arguments["source_manifest"]["expected_row_count"] = 0
    arguments["source_manifest"]["source_artifact_id"] = sha256_file(source)
    arguments["output_root"] = tmp_path / "empty-output"
    report = classify_artifact(**arguments)
    database, report = prepare_reconciliation_database((report, arguments, zones), tmp_path)
    result = reconcile(database, report, tmp_path / "empty-verification.json")
    assert result["status"] == "PASSED"
    assert result["source_rows"] == 0


@pytest.mark.parametrize("orphan_table", ["fct_trip", "mart_zone_hour"])
def test_dbt_reconciliation_detects_rows_without_an_accepted_artifact(orphan_table):
    with duckdb.connect() as connection:
        for table in ["stg_trips", "fct_trip"]:
            connection.execute(f"""CREATE TABLE {table} (
                dataset_id VARCHAR, artifact_id VARCHAR, recorded_charge DECIMAL(18,2))""")
            connection.execute(f"INSERT INTO {table} VALUES ('real','a',10)")
        connection.execute("""CREATE TABLE mart_zone_hour (
            dataset_id VARCHAR, artifact_id VARCHAR,
            trip_count BIGINT, recorded_charge_sum DECIMAL(18,2))""")
        connection.execute("INSERT INTO mart_zone_hour VALUES ('real','a',1,10)")
        query = Path("dbt/tests/fact_mart_reconciliation.sql").read_text()
        for table in ["stg_trips", "fct_trip", "mart_zone_hour"]:
            query = query.replace("{{ ref('" + table + "') }}", table)
        assert connection.execute(query).fetchall() == []
        values = "('real','orphan',10)" if orphan_table == "fct_trip" else "('real','orphan',1,10)"
        connection.execute(f"INSERT INTO {orphan_table} VALUES {values}")
        assert connection.execute(query).fetchall() == [("real", "orphan")]


def test_dbt_artifact_coverage_accepts_only_empty_receipts_without_daily_rows():
    with duckdb.connect() as connection:
        connection.execute("""CREATE TABLE mart_daily_reconciliation (
            dataset_id VARCHAR, artifact_id VARCHAR, source_count BIGINT,
            accepted_count BIGINT, quarantined_count BIGINT, recorded_charge_sum DECIMAL(18,2))""")
        connection.execute("""CREATE TABLE source_artifacts (
            dataset_id VARCHAR, artifact_id VARCHAR, source_rows BIGINT,
            accepted_rows BIGINT, quarantined_rows BIGINT, recorded_charge DECIMAL(18,2))""")
        connection.execute("INSERT INTO source_artifacts VALUES ('real','empty',0,0,0,0)")
        query = Path("dbt/tests/artifact_coverage.sql").read_text()
        query = query.replace(
            "{{ ref('mart_daily_reconciliation') }}", "mart_daily_reconciliation"
        ).replace("{{ source('control', 'source_artifacts') }}", "source_artifacts")
        assert connection.execute(query).fetchall() == []
        connection.execute("INSERT INTO source_artifacts VALUES ('real','missing',1,1,0,10)")
        assert connection.execute(query).fetchall() == [("real", "missing", 1, 1, 0, 10)]


@pytest.mark.parametrize("orphan_dataset,orphan_artifact", [("real", "orphan"), ("other", "a")])
def test_dbt_artifact_coverage_rejects_unregistered_daily_groups(orphan_dataset, orphan_artifact):
    with duckdb.connect() as connection:
        connection.execute("""CREATE TABLE mart_daily_reconciliation (
            dataset_id VARCHAR, artifact_id VARCHAR, source_count BIGINT,
            accepted_count BIGINT, quarantined_count BIGINT, recorded_charge_sum DECIMAL(18,2))""")
        connection.execute("""CREATE TABLE source_artifacts (
            dataset_id VARCHAR, artifact_id VARCHAR, source_rows BIGINT,
            accepted_rows BIGINT, quarantined_rows BIGINT, recorded_charge DECIMAL(18,2))""")
        connection.execute("INSERT INTO source_artifacts VALUES ('real','a',1,1,0,10)")
        connection.execute("INSERT INTO mart_daily_reconciliation VALUES ('real','a',1,1,0,10)")
        query = Path("dbt/tests/artifact_coverage.sql").read_text()
        query = query.replace(
            "{{ ref('mart_daily_reconciliation') }}", "mart_daily_reconciliation"
        ).replace("{{ source('control', 'source_artifacts') }}", "source_artifacts")
        assert connection.execute(query).fetchall() == []
        connection.execute(
            "INSERT INTO mart_daily_reconciliation VALUES (?,?,1,1,0,10)",
            [orphan_dataset, orphan_artifact],
        )
        assert connection.execute(query).fetchall() == [
            (orphan_dataset, orphan_artifact, None, None, None, None)
        ]
