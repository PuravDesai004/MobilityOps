import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mobilityops.classify import (
    ProvenanceError,
    classify_artifact,
    classify_row,
    decimal_value,
)
from mobilityops.config import load_project_config, load_yellow_taxi_contract
from mobilityops.contract import FileContractError
from mobilityops.fixture import fixture_rows, generate_fixture
from mobilityops.identity import sha256_file
from mobilityops.reference import eligible_source_charge


def classify(row, **kwargs):
    return classify_row(
        row,
        artifact_id="a" * 64,
        source_row_number=1,
        dataset_id="fixture",
        source_partition="2026-01",
        zone_ids={132, 138, 161, 162, 264, 265},
        **kwargs,
    )


def test_reviewed_fixture_and_independent_money(classified_fixture):
    report, arguments, _ = classified_fixture
    expectations = json.loads(Path("tests/expected/yellow_taxi_fixture_v1.json").read_text())
    assert report["accepted_rows"] == expectations["expected_dispositions"]["accepted"] == 8
    assert report["quarantined_rows"] == 4
    accepted = pq.read_table(report["accepted_path"]).to_pylist()
    rejected = pq.read_table(report["quarantined_path"]).to_pylist()
    assert [r["source_row_number"] for r in accepted] == [1, 2, 3, 4, 9, 10, 11, 12]
    assert [r["source_row_number"] for r in rejected] == [5, 6, 7, 8]
    assert len({r["trip_record_id"] for r in accepted + rejected}) == 12
    assert accepted[0]["trip_record_id"] != accepted[1]["trip_record_id"]
    assert report["recorded_charge"] == "134.00"
    assert eligible_source_charge(arguments["source_path"]) == (8, Decimal("134.00"))
    assert accepted[4]["pickup_zone_key"] == 0
    assert accepted[4]["pickup_zone_source_id"] == 999
    assert accepted[5]["recorded_charge"] == Decimal("-2.25")
    assert accepted[-1]["pickup_at"].year == 2025


@pytest.mark.parametrize(
    ("value", "scale", "expected"),
    [
        ("1.005", 2, "1.01"),
        ("-1.005", 2, "-1.01"),
        ("2.3455", 3, "2.346"),
        ("9999999999999999.994", 2, "9999999999999999.99"),
        ("9999999999999999.995", 2, None),
        ("1e20", 2, None),
        ("1e1000001", 2, None),
        ("-1e1000001", 3, None),
        (float("nan"), 2, None),
        (float("inf"), 3, None),
        (True, 2, None),
        ("garbage", 2, None),
    ],
)
def test_decimal_policy_boundaries(value, scale, expected):
    assert decimal_value(value, scale) == (Decimal(expected) if expected else None)


def test_multiple_reasons_are_distinct_and_original_fields_survive():
    row = fixture_rows()[0] | {
        "tpep_pickup_datetime": "bad",
        "tpep_dropoff_datetime": None,
        "trip_distance": float("nan"),
        "total_amount": "bad",
        "new_field": 42,
    }
    result = classify(row)
    assert result["reason_codes"] == [
        "INVALID_DISTANCE",
        "INVALID_PICKUP_TIMESTAMP",
        "INVALID_TOTAL_AMOUNT",
        "MISSING_DROPOFF",
    ]
    original = json.loads(result["raw_payload_json"])
    assert original["new_field"] == 42
    assert original["trip_distance"]["__type__"] == "nonfinite_float"


def test_warning_only_unknown_official_zone_and_tiny_negative_distance():
    row = fixture_rows()[0] | {
        "PULocationID": 264,
        "passenger_count": None,
        "payment_type": 999,
        "total_amount": -1.005,
        "tpep_dropoff_datetime": datetime(2026, 1, 5, 8) + timedelta(days=2),
    }
    result = classify(row)
    assert result["reason_codes"] == []
    assert result["pickup_zone_key"] == 264
    assert "UNRESOLVED_PICKUP_ZONE" not in result["warning_codes"]
    assert result["warning_codes"] == [
        "LONG_DURATION",
        "MISSING_PASSENGER_COUNT",
        "NEGATIVE_RECORDED_CHARGE",
        "UNKNOWN_PAYMENT_CODE",
    ]
    assert "NEGATIVE_DISTANCE" in classify(row | {"trip_distance": -0.00001})["reason_codes"]


def test_provenance_is_processing_failure():
    with pytest.raises(ProvenanceError):
        classify_row(
            fixture_rows()[0],
            artifact_id="bad",
            source_row_number=1,
            dataset_id="fixture",
            source_partition="2026-01",
            zone_ids={161, 162},
        )
    with pytest.raises(ProvenanceError):
        classify(
            fixture_rows()[0] | {"_mobilityops_source_row_number": 0}, parent_artifact_id="b" * 64
        )


def test_failed_republication_retains_last_success(classified_fixture):
    report, arguments, _ = classified_fixture
    pointer = arguments["output_root"] / "classification.json"
    before = pointer.read_bytes()
    # A different warning policy forces recomputation while the artifact itself stays intact.
    with pytest.raises(RuntimeError, match="publication failure"):
        classify_artifact(**arguments, long_duration_seconds=10, fail_after_batches=1)
    assert pointer.read_bytes() == before
    assert Path(report["accepted_path"]).exists()
    assert len(list((arguments["output_root"] / "generations").iterdir())) == 1
    assert (
        json.loads((arguments["output_root"] / "attempts.jsonl").read_text().splitlines()[-1])[
            "outcome"
        ]
        == "FAILED"
    )


def test_required_missing_stops_file_and_optional_fields_preserved(tmp_path):
    contract = load_yellow_taxi_contract(load_project_config("config/project.yaml"))
    source, zones = tmp_path / "source.parquet", tmp_path / "zones.csv"
    generate_fixture(source, zones)
    table = pq.read_table(source).drop(["total_amount"])
    pq.write_table(table, source)
    args = dict(
        source_path=source,
        zone_path=zones,
        contract=contract,
        output_root=tmp_path / "out",
        source_manifest={
            "source_artifact_id": sha256_file(source),
            "dataset_id": "fixture",
            "source_partition": "2026-01",
            "expected_row_count": 12,
        },
    )
    with pytest.raises(FileContractError):
        classify_artifact(**args)
    assert not (tmp_path / "out/classification.json").exists()
    generate_fixture(source, zones)
    pq.write_table(pq.read_table(source).append_column("extra_field", pa.array(range(12))), source)
    args["source_manifest"]["source_artifact_id"] = sha256_file(source)
    report = classify_artifact(**args)
    assert (
        json.loads(pq.read_table(report["accepted_path"]).to_pylist()[0]["raw_payload_json"])[
            "extra_field"
        ]
        == 0
    )


@pytest.mark.parametrize(
    "field,value",
    [("dataset_id", "different"), ("source_partition", "2026-02"), ("expected_row_count", 13)],
)
def test_reuse_rejects_manifest_provenance_drift(classified_fixture, field, value):
    _, arguments, _ = classified_fixture
    pointer = arguments["output_root"] / "classification.json"
    before = pointer.read_bytes()
    arguments["source_manifest"][field] = value
    with pytest.raises(ProvenanceError, match="manifest"):
        classify_artifact(**arguments)
    assert pointer.read_bytes() == before


def test_sample_rows_have_separate_identity_and_parent_lineage(tmp_path):
    from mobilityops.identity import trip_record_id
    from mobilityops.sample import create_deterministic_sample

    config = load_project_config("config/project.yaml")
    contract = load_yellow_taxi_contract(config)
    source, zones, sample = (
        tmp_path / "parent.parquet",
        tmp_path / "zones.csv",
        tmp_path / "sample.parquet",
    )
    generate_fixture(source, zones)
    parent = sha256_file(source)
    selection = create_deterministic_sample(
        source_path=source,
        sample_path=sample,
        contract=contract,
        parent_artifact_id=parent,
        limit_rows=3,
    )
    report = classify_artifact(
        source_path=sample,
        zone_path=zones,
        contract=contract,
        output_root=tmp_path / "out",
        source_manifest={
            "source_artifact_id": selection.sample_artifact_id,
            "parent_artifact_id": parent,
            "dataset_id": "sample-fixture",
            "dataset_scope": "sample",
            "source_partition": "2026-01",
            "expected_row_count": 3,
        },
    )
    rows = sorted(
        pq.read_table(report["accepted_path"]).to_pylist()
        + pq.read_table(report["quarantined_path"]).to_pylist(),
        key=lambda row: row["source_row_number"],
    )
    assert [r["parent_source_row_number"] for r in rows] == [1, 6, 12]
    assert all(r["parent_artifact_id"] == parent for r in rows)
    assert all(
        r["trip_record_id"] == trip_record_id(selection.sample_artifact_id, i)
        for i, r in enumerate(rows, 1)
    )
    assert rows[0]["trip_record_id"] != trip_record_id(parent, 1)
