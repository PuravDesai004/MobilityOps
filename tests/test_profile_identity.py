import hashlib

from mobilityops.fixture import generate_fixture
from mobilityops.identity import sha256_file, trip_record_id
from mobilityops.profile import profile_parquet


def test_profile_reports_actual_parquet_metadata_and_byte_checksum(tmp_path):
    parquet_path = tmp_path / "trips.parquet"
    generate_fixture(parquet_path, tmp_path / "zones.csv")

    profile = profile_parquet(parquet_path)

    assert profile.row_count == 12
    assert profile.row_group_count == 3
    assert profile.file_size_bytes == parquet_path.stat().st_size
    assert profile.source_artifact_id == hashlib.sha256(parquet_path.read_bytes()).hexdigest()
    assert len(profile.schema_sha256) == 64
    assert set(profile.schema_sha256) <= set("0123456789abcdef")
    fields = {field.name: field.arrow_type for field in profile.fields}
    assert fields["tpep_pickup_datetime"] == "timestamp[us]"
    assert fields["trip_distance"] == "double"
    assert fields["total_amount"] == "double"
    parquet_fields = {field.name: field for field in profile.fields}
    assert parquet_fields["tpep_pickup_datetime"].parquet_physical_type == "INT64"
    assert "TIMESTAMP" in parquet_fields["tpep_pickup_datetime"].parquet_logical_type.upper()
    assert profile.null_counts["tpep_pickup_datetime"] == 1
    assert profile.null_counts["total_amount"] == 1
    assert profile.null_counts["trip_distance"] == 0
    assert set(fields) >= {
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "total_amount",
    }


def test_source_row_identity_depends_on_artifact_bytes_and_physical_row_not_context(tmp_path):
    original_path = tmp_path / "original.parquet"
    renamed_path = tmp_path / "moved" / "renamed.parquet"
    generate_fixture(original_path, tmp_path / "original-zones.csv")
    renamed_path.parent.mkdir()
    renamed_path.write_bytes(original_path.read_bytes())

    original_artifact_id = sha256_file(original_path)
    renamed_artifact_id = sha256_file(renamed_path)
    assert original_artifact_id == renamed_artifact_id

    first_context_id = trip_record_id(original_artifact_id, 1)
    contexts = [
        {"run_id": "run-a", "attempt_id": "attempt-a", "path": original_path},
        {"run_id": "run-b", "attempt_id": "attempt-b", "path": renamed_path},
    ]
    assert contexts[0] != contexts[1]
    context_ids = [trip_record_id(sha256_file(context["path"]), 1) for context in contexts]
    assert context_ids == [first_context_id, first_context_id]
    assert first_context_id == trip_record_id(renamed_artifact_id, 1)
    assert trip_record_id(original_artifact_id, 1) != trip_record_id(original_artifact_id, 2)
    assert (
        first_context_id == hashlib.sha256(f"{original_artifact_id}:1".encode("utf-8")).hexdigest()
    )

    renamed_path.write_bytes(renamed_path.read_bytes() + b"one different trailing byte")
    changed_artifact_id = sha256_file(renamed_path)
    assert changed_artifact_id != original_artifact_id
    assert trip_record_id(changed_artifact_id, 1) != first_context_id


def test_identity_rejects_invalid_artifact_hash_and_row_number():
    try:
        trip_record_id("not-a-sha", 1)
    except ValueError:
        pass
    else:
        raise AssertionError("invalid artifact hash was accepted")

    try:
        trip_record_id("a" * 64, 0)
    except ValueError:
        pass
    else:
        raise AssertionError("row zero was accepted")
