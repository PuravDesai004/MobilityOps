import pytest

from mobilityops.storage import connect_local, ingest_classified


def counts(database):
    with connect_local(database, read_only=True) as connection:
        return tuple(
            connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in [
                "curated.accepted_trips",
                "curated.quarantined_trips",
                "control.source_artifacts",
                "control.ingestion_audit",
            ]
        )


def test_local_transaction_rollback_retry_and_replay(classified_fixture, tmp_path):
    report, _, zones = classified_fixture
    database = tmp_path / "local.duckdb"
    with pytest.raises(RuntimeError, match="before commit"):
        ingest_classified(database=database, report=report, zone_path=zones, inject_failure=True)
    assert counts(database) == (0, 0, 0, 0)
    first = ingest_classified(database=database, report=report, zone_path=zones)
    assert (first["accepted_inserted"], first["quarantined_inserted"]) == (8, 4)
    assert counts(database) == (8, 4, 1, 1)
    replay = ingest_classified(database=database, report=report, zone_path=zones)
    assert (
        replay["accepted_inserted"],
        replay["quarantined_inserted"],
        replay["replay_records"],
    ) == (0, 0, 12)
    assert counts(database) == (8, 4, 1, 2)


def test_ingestion_rejects_modified_output(classified_fixture, tmp_path):
    report, _, zones = classified_fixture
    from pathlib import Path

    Path(report["accepted_path"]).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        ingest_classified(database=tmp_path / "bad.duckdb", report=report, zone_path=zones)


def test_existing_key_rejects_conflicting_original_payload(classified_fixture, tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from mobilityops.identity import sha256_file

    report, _, zones = classified_fixture
    database = tmp_path / "local.duckdb"
    ingest_classified(database=database, report=report, zone_path=zones)
    before = counts(database)
    table = pq.read_table(report["accepted_path"])
    rows = table.to_pylist()
    rows[0]["raw_payload_json"] = '{"different_original":true}'
    pq.write_table(pa.Table.from_pylist(rows, table.schema), report["accepted_path"])
    report["accepted_path_sha256"] = sha256_file(report["accepted_path"])
    with pytest.raises(ValueError, match="conflicting original payload"):
        ingest_classified(database=database, report=report, zone_path=zones)
    assert counts(database) == before


def test_ingestion_rejects_changed_original_source(classified_fixture, tmp_path):
    from pathlib import Path

    report, _, zones = classified_fixture
    Path(report["source_path"]).write_bytes(b"changed")
    with pytest.raises(ValueError, match="Original source checksum"):
        ingest_classified(database=tmp_path / "bad.duckdb", report=report, zone_path=zones)


@pytest.mark.parametrize(
    "field", ["dataset_id", "artifact_id", "source_partition", "raw_payload_json"]
)
def test_ingestion_rejects_null_provenance(classified_fixture, tmp_path, field):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from mobilityops.identity import sha256_file

    report, _, zones = classified_fixture
    table = pq.read_table(report["accepted_path"])
    rows = table.to_pylist()
    rows[0][field] = None
    pq.write_table(pa.Table.from_pylist(rows, table.schema), report["accepted_path"])
    report["accepted_path_sha256"] = sha256_file(report["accepted_path"])

    with pytest.raises(ValueError, match="identity/coverage"):
        ingest_classified(database=tmp_path / "bad.duckdb", report=report, zone_path=zones)


@pytest.mark.parametrize(
    ("output", "field"),
    [
        ("accepted_path", "reason_codes"),
        ("quarantined_path", "reason_codes"),
        ("accepted_path", "duration_seconds"),
        ("accepted_path", "warning_codes"),
        ("quarantined_path", "warning_codes"),
    ],
)
def test_ingestion_rejects_null_disposition_fields(classified_fixture, tmp_path, output, field):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from mobilityops.identity import sha256_file

    report, _, zones = classified_fixture
    table = pq.read_table(report[output])
    rows = table.to_pylist()
    rows[0][field] = None
    pq.write_table(pa.Table.from_pylist(rows, table.schema), report[output])
    report[f"{output}_sha256"] = sha256_file(report[output])

    with pytest.raises(ValueError, match="disposition/count"):
        ingest_classified(database=tmp_path / "bad.duckdb", report=report, zone_path=zones)


def test_replay_rejects_changed_classification_policy(classified_fixture, tmp_path):
    from mobilityops.classify import classify_artifact

    report, arguments, zones = classified_fixture
    database = tmp_path / "local.duckdb"
    ingest_classified(database=database, report=report, zone_path=zones)
    before = counts(database)
    changed_report = classify_artifact(**arguments, long_duration_seconds=1)

    with pytest.raises(ValueError, match="conflicting classified values"):
        ingest_classified(database=database, report=changed_report, zone_path=zones)

    assert counts(database) == before


def test_ingestion_allows_empty_source_with_complete_coverage(classified_fixture, tmp_path):
    import pyarrow.parquet as pq

    from mobilityops.classify import classify_artifact
    from mobilityops.identity import sha256_file

    report, arguments, zones = classified_fixture
    source = arguments["source_path"]
    pq.write_table(pq.read_table(source).slice(0, 0), source)
    arguments["source_manifest"]["source_artifact_id"] = sha256_file(source)
    arguments["source_manifest"]["expected_row_count"] = 0
    arguments["output_root"] = tmp_path / "empty_outputs"
    report = classify_artifact(**arguments)
    database = tmp_path / "empty.duckdb"

    result = ingest_classified(database=database, report=report, zone_path=zones)

    assert result["accepted_inserted"] == result["quarantined_inserted"] == 0
    assert result["replay_records"] == 0
    assert counts(database) == (0, 0, 1, 1)
