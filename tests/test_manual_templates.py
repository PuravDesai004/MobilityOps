"""Local checks of prepared assets. None of these tests connects to Snowflake."""

import csv
import json
from pathlib import Path

import pytest
from scripts.compare_manual_exports import FIELDS, compare_export

from mobilityops.policy import _json_value, classify_row


def test_json_policy_vectors_match_shared_classifier():
    vectors = json.loads(Path("sql/snowflake/assets/classification_vectors.json").read_text())
    assert len(vectors) == 19
    for vector in vectors:
        actual = classify_row(
            vector["payload"],
            artifact_id="a" * 64,
            source_row_number=1,
            dataset_id="fixture",
            source_partition="2026-01",
            zone_ids={132, 138, 161, 162, 264, 265},
        )
        assert _json_value(actual) == vector["expected"], vector["label"]


def test_manual_export_comparator_is_offline_and_checks_money(tmp_path, monkeypatch):
    import socket

    monkeypatch.setattr(
        socket,
        "create_connection",
        lambda *args, **kwargs: pytest.fail("Manual export comparison attempted a connection"),
    )
    report_path = Path("reports/local/fixture/fixture/verification.json")
    # Test inputs are explicitly synthetic, not a claim of an actual cloud export.
    source = json.loads(report_path.read_text())
    export = tmp_path / "synthetic.csv"
    row = {field: source[field] for field in ("dataset_id", "artifact_id", *FIELDS)}

    def write():
        with export.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(row))
            writer.writeheader()
            writer.writerow(row)

    write()
    result = compare_export(export, [report_path])
    assert result["status"] == "PASSED"
    assert result["snowflake_connection_performed"] is False
    row["recorded_charge_usd"] = "134.01"
    write()
    assert compare_export(export, [report_path])["status"] == "FAILED"
    row["artifact_id"] = "b" * 64
    write()
    with pytest.raises(ValueError, match="identities"):
        compare_export(export, [report_path])


def test_prepared_procedure_has_no_ddl_inside_explicit_transaction():
    sql = Path("sql/snowflake/06_transactional_promotion.sql").read_text()
    transaction = sql.split("BEGIN TRANSACTION;", 1)[1].split("COMMIT;", 1)[0]
    assert "INSERT INTO WORK.INCOMING" in transaction
    assert "MERGE INTO CURATED.ACCEPTED_TRIPS" in transaction
    assert "MERGE INTO CURATED.QUARANTINED_TRIPS" in transaction
    assert "INSERT INTO CONTROL.INGESTION_AUDIT" in transaction
    assert (
        "CREATE " not in transaction and "ALTER " not in transaction and "DROP " not in transaction
    )
    assert "ROLLBACK;" in sql and "RAISE;" in sql
    # VARIANT objects are unordered; JSON string ordering must not create false conflicts.
    for scope in ("", "real/", "recovery/"):
        prepared = Path(f"sql/snowflake/{scope}06_transactional_promotion.sql").read_text()
        assert "COUNT(DISTINCT payload)>1" in prepared
        assert "COUNT(DISTINCT TO_JSON(payload))" not in prepared
        assert "PARSE_JSON(s.raw_payload_json)<>PARSE_JSON(t.raw_payload_json)" in prepared
    assert (
        "from mobilityops.policy import classify_row"
        in Path("sql/snowflake/05_shared_classifier.sql").read_text()
    )
