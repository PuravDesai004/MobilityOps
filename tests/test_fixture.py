import json
from pathlib import Path

import pyarrow.parquet as pq

from mobilityops.fixture import FIXTURE_ROW_COUNT, generate_fixture


def test_fixture_has_twelve_source_shaped_rows_and_three_row_groups(tmp_path):
    parquet_path = tmp_path / "fixture.parquet"
    zone_path = tmp_path / "zones.csv"

    artifacts = generate_fixture(parquet_path, zone_path)
    metadata = pq.read_metadata(parquet_path)
    table = pq.read_table(parquet_path)

    assert table.num_rows == FIXTURE_ROW_COUNT == 12
    assert metadata.num_rows == 12
    assert metadata.num_row_groups == 3
    assert artifacts.parquet_bytes == parquet_path.stat().st_size
    assert artifacts.parquet_sha256
    assert artifacts.zone_lookup_sha256
    assert table.schema.field("tpep_pickup_datetime").type == "timestamp[us]"
    assert table.schema.field("trip_distance").type == "double"
    assert table.column("tpep_pickup_datetime")[4].as_py() is None
    assert (
        table.column("tpep_dropoff_datetime")[5].as_py()
        < table.column("tpep_pickup_datetime")[5].as_py()
    )
    assert table.column("trip_distance")[6].as_py() < 0
    assert table.column("total_amount")[7].as_py() is None
    assert table.column("PULocationID")[8].as_py() == 999
    assert table.column("total_amount")[9].as_py() < 0
    assert table.column("passenger_count")[10].as_py() == 0
    assert table.column("tpep_pickup_datetime")[11].as_py().year == 2025

    first_row = table.slice(0, 1).to_pylist()[0]
    second_row = table.slice(1, 1).to_pylist()[0]
    assert first_row == second_row

    zone_lines = zone_path.read_text(encoding="utf-8").splitlines()
    assert zone_lines[0] == "LocationID,Borough,Zone,service_zone"
    assert len(zone_lines) == 5


def test_fixture_bytes_are_repeatable_with_the_installed_pyarrow(tmp_path):
    first = generate_fixture(tmp_path / "one.parquet", tmp_path / "one.csv")
    second = generate_fixture(tmp_path / "two.parquet", tmp_path / "two.csv")

    assert first.parquet_sha256 == second.parquet_sha256
    assert first.zone_lookup_sha256 == second.zone_lookup_sha256
    assert (tmp_path / "one.parquet").read_bytes() == (tmp_path / "two.parquet").read_bytes()


def test_expected_results_are_documented_but_not_observed():
    expected_path = Path("tests/expected/yellow_taxi_fixture_v1.json")
    expected = json.loads(expected_path.read_text(encoding="utf-8"))

    assert expected["status"] == "expected_only"
    assert expected["classification_implemented"] is False
    assert expected["classification_run"] is False
    assert expected["expected_dispositions"] == {"accepted": 8, "quarantined": 4}
    assert expected["expected_source_rows"] == 12
