"""Minimum syntax and manual shared-policy packaging checks, without cloud execution."""

import ast
import json
import zipfile
from pathlib import Path

from mobilityops.identity import sha256_file


def test_sources_support_declared_python_312_minimum():
    for directory in ("src/mobilityops", "scripts", "app", "airflow/dags"):
        for path in Path(directory).rglob("*.py"):
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path), feature_version=(3, 12))


def test_snowflake_package_uses_identical_shared_policy():
    assets = Path("sql/snowflake/assets")
    package = assets / "mobilityops_policy.zip"
    receipt = json.loads((assets / "policy_package_receipt.json").read_text())
    assert receipt["snowflake_execution"] == "UNVERIFIED"
    assert receipt["sha256"] == sha256_file(package)
    with zipfile.ZipFile(package) as archive:
        for name in ("identity.py", "policy.py"):
            assert (
                archive.read(f"mobilityops/{name}") == Path(f"src/mobilityops/{name}").read_bytes()
            )
