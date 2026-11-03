import importlib.metadata
import shutil

import pyarrow.parquet as pq
import pytest

from mobilityops.config import load_project_config
from mobilityops.fixture import generate_fixture
from mobilityops.workflow import prepare, runtime_versions


def test_runtime_versions_allows_missing_optional_packages(monkeypatch):
    def version(name):
        if name in {"streamlit", "plotly", "pytest", "ruff"}:
            raise importlib.metadata.PackageNotFoundError(name)
        return "1.2.3"

    monkeypatch.setattr(importlib.metadata, "version", version)

    versions = runtime_versions()

    assert versions["duckdb"] == "1.2.3"
    assert versions["streamlit"] == "not installed"
    assert versions["ruff"] == "not installed"


@pytest.mark.parametrize("missing", ["source", "zones"])
def test_preparing_missing_fixture_asset_preserves_the_existing_asset(tmp_path, missing):
    config = load_project_config("config/project.yaml")
    config._project_root = tmp_path
    contract_path = config.resolve_path("contract_yaml")
    contract_path.parent.mkdir(parents=True)
    shutil.copy2("config/contracts/yellow_taxi_v1.yaml", contract_path)
    source = config.resolve_path("fixture_parquet")
    zones = config.resolve_path("zone_lookup_csv")
    generate_fixture(source, zones)
    if missing == "source":
        source.unlink()
        zones.write_text(zones.read_text() + "999,Queens,Extra zone,Yellow Zone\n")
        preserved = zones
    else:
        zones.unlink()
        table = pq.read_table(source).replace_schema_metadata({b"fixture_version": b"custom"})
        pq.write_table(table, source)
        preserved = source
    original = preserved.read_bytes()

    prepare(config, "fixture", offline=True, run_id="repair-fixture")

    assert source.is_file() and zones.is_file()
    assert preserved.read_bytes() == original
