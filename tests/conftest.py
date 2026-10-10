import pytest

from mobilityops.classify import classify_artifact
from mobilityops.config import load_project_config, load_yellow_taxi_contract
from mobilityops.fixture import generate_fixture
from mobilityops.identity import sha256_file


@pytest.fixture
def classified_fixture(tmp_path):
    source, zones = tmp_path / "source.parquet", tmp_path / "zones.csv"
    generate_fixture(source, zones)
    contract = load_yellow_taxi_contract(load_project_config("config/project.yaml"))
    manifest = {
        "dataset_id": "fixture",
        "source_partition": "2026-01",
        "dataset_scope": "fixture",
        "source_artifact_id": sha256_file(source),
        "expected_row_count": 12,
        "zone_artifact_sha256": sha256_file(zones),
    }
    arguments = dict(
        source_path=source,
        source_manifest=manifest,
        zone_path=zones,
        output_root=tmp_path / "outputs",
        contract=contract,
        batch_rows=3,
    )
    report = classify_artifact(**arguments)
    return report, arguments, zones
