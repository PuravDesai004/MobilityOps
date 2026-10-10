import hashlib
import shutil

import pyarrow.parquet as pq

from mobilityops.config import load_project_config, load_yellow_taxi_contract
from mobilityops.fixture import generate_fixture
from mobilityops.identity import trip_record_id
from mobilityops.sample import (
    SOURCE_ROW_NUMBER_COLUMN,
    create_deterministic_sample,
    evenly_spaced_source_rows,
)


def _temporary_contract(tmp_path):
    config_dir = tmp_path / "config"
    contract_dir = config_dir / "contracts"
    contract_dir.mkdir(parents=True)
    shutil.copy2("config/project.yaml", config_dir / "project.yaml")
    shutil.copy2(
        "config/contracts/yellow_taxi_v1.yaml",
        contract_dir / "yellow_taxi_v1.yaml",
    )
    config = load_project_config(config_dir / "project.yaml")
    return config, load_yellow_taxi_contract(config)


def test_evenly_spaced_selection_is_stable_bounded_and_covers_source():
    assert evenly_spaced_source_rows(12, 5) == (1, 3, 6, 9, 12)
    assert evenly_spaced_source_rows(12, 10_000) == tuple(range(1, 13))
    assert len(evenly_spaced_source_rows(50_000, 10_000)) == 10_000
    assert evenly_spaced_source_rows(0, 10_000) == ()


def test_sample_is_separate_deterministic_artifact_with_parent_row_lineage(tmp_path):
    _config, contract = _temporary_contract(tmp_path)
    source = tmp_path / "yellow.parquet"
    generate_fixture(source, tmp_path / "zones.csv")
    parent_id = hashlib.sha256(source.read_bytes()).hexdigest()
    sample_path = tmp_path / "samples" / "yellow-sample.parquet"

    first = create_deterministic_sample(
        source_path=source,
        sample_path=sample_path,
        contract=contract,
        parent_artifact_id=parent_id,
        limit_rows=5,
        batch_rows=3,
    )
    first_bytes = sample_path.read_bytes()
    table = pq.read_table(sample_path)

    assert first.row_count == 5
    assert first.parent_row_count == 12
    assert first.selected_source_row_numbers == (1, 3, 6, 9, 12)
    assert first.sample_artifact_id != parent_id
    assert first.source_row_number_column == SOURCE_ROW_NUMBER_COLUMN
    assert table.column(SOURCE_ROW_NUMBER_COLUMN).to_pylist() == [1, 3, 6, 9, 12]
    assert table.schema.names[:-1] == pq.read_schema(source).names
    assert trip_record_id(first.sample_artifact_id, 1) != trip_record_id(parent_id, 1)

    second = create_deterministic_sample(
        source_path=source,
        sample_path=sample_path,
        contract=contract,
        parent_artifact_id=parent_id,
        limit_rows=5,
        batch_rows=3,
    )
    assert second.outcome == "REUSED_VERIFIED_UNCHANGED"
    assert sample_path.read_bytes() == first_bytes
    assert second.sample_artifact_id == first.sample_artifact_id


def test_sample_rejects_a_parent_checksum_mismatch(tmp_path):
    _config, contract = _temporary_contract(tmp_path)
    source = tmp_path / "yellow.parquet"
    generate_fixture(source, tmp_path / "zones.csv")

    try:
        create_deterministic_sample(
            source_path=source,
            sample_path=tmp_path / "sample.parquet",
            contract=contract,
            parent_artifact_id="0" * 64,
        )
    except ValueError as error:
        assert "checksum does not match" in str(error)
    else:
        raise AssertionError("mismatched parent artifact identity was accepted")
