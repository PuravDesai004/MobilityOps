"""Read verified local outcomes and save portable final evidence; no warehouse access."""

import json
import os
import re
import subprocess
from pathlib import Path

import pyarrow.parquet as pq

from mobilityops.identity import sha256_file
from mobilityops.manifest import utc_now, write_manifest_atomically
from mobilityops.snapshot import load_snapshot

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "reports/evidence"


def read(path):
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def portable(value):
    if isinstance(value, dict):
        return {key: portable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [portable(item) for item in value]
    if isinstance(value, str):
        return value.replace(str(ROOT), ".").replace(str(ROOT).replace("\\", "/"), ".")
    return value


def command(arguments):
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    result = subprocess.run(
        arguments,
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=90,
    )
    receipt = {
        "command": portable(arguments),
        "exit_code": result.returncode,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
    }
    if result.returncode:
        raise ValueError(f"Evidence command failed: {receipt}")
    return receipt


def main():
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    artifacts = []
    for scope, partition in [
        ("fixture", "fixture"),
        ("sample", "2026-01"),
        ("real", "2026-01"),
        ("real", "2026-02"),
        ("real", "2026-03"),
    ]:
        directory = f"reports/local/{scope}/{partition}"
        verification = read(f"{directory}/verification.json")
        workflow = read(f"{directory}/latest_workflow.json")
        execution = read(f"{directory}/dbt_execution.json")
        results = read(f"{directory}/dbt_target/run_results.json")["results"]
        if verification["status"] != "PASSED" or workflow["outcome"] != "SUCCEEDED":
            raise ValueError("Workflow/verification not passed")
        if any(r["status"] not in {"success", "pass"} for r in results):
            raise ValueError("dbt has a failed/skipped model or data test")
        if (
            sum(r["unique_id"].startswith("model.") for r in results) != 6
            or sum(r["unique_id"].startswith("test.") for r in results) != 24
        ):
            raise ValueError("Expected six dbt models and 24 data tests for every scope")
        classification = read(f"data/classified/{scope}/{partition}/classification.json")
        source_path = Path(classification["source_path"])
        if sha256_file(source_path) != verification["artifact_id"]:
            raise ValueError("Current original source bytes differ from verified artifact")
        with pq.ParquetFile(source_path) as source:
            current_source = {
                "artifact_id": verification["artifact_id"],
                "bytes": source_path.stat().st_size,
                "rows": source.metadata.num_rows,
                "row_groups": source.metadata.num_row_groups,
                "field_count": len(source.schema_arrow),
                "checksum_rechecked": True,
            }
        if current_source["rows"] != verification["source_rows"]:
            raise ValueError("Current source metadata differs from verified row count")
        detail = {
            "scope": scope,
            "verification": verification,
            "workflow": portable(workflow),
            "dbt_execution": portable(execution),
            "current_source": current_source,
            "dbt_results": [
                {
                    key: row.get(key)
                    for key in ("unique_id", "status", "execution_time", "adapter_response")
                }
                for row in results
            ],
        }
        write_manifest_atomically(EVIDENCE / f"{scope}_{partition}.json", detail)
        artifacts.append(
            {
                "scope": scope,
                **verification,
                "dbt_models_passed": sum(r["unique_id"].startswith("model.") for r in results),
                "dbt_data_tests_passed": sum(r["unique_id"].startswith("test.") for r in results),
            }
        )
    snapshots = {}
    for scope, path in [
        ("fixture", "demo/fixture"),
        ("sample", "demo/sample"),
        ("real", "demo/published"),
    ]:
        _, _, summary, manifest = load_snapshot(ROOT / path)
        snapshots[scope] = {
            "status": "VALIDATED",
            "path": path,
            "manifest_sha256": sha256_file(ROOT / path / "snapshot_manifest.json"),
            "origin_engine": summary["origin_engine"],
            "actual_date_coverage": summary["actual_date_coverage"],
            "source_rows": summary["source_rows"],
            "accepted_rows": summary["accepted_rows"],
            "quarantined_rows": summary["quarantined_rows"],
            "recorded_charge_usd": summary["recorded_charge_usd"],
            "file_bytes": sum(f["bytes"] for f in manifest["files"].values()),
        }
    test_output = (ROOT / "reports/local/pytest-final.log").read_text()
    match = re.search(r"(\d+) passed in ([0-9.]+)s", test_output)
    if not match or re.search(r"\d+ (failed|error)", test_output):
        raise ValueError("Final pytest result is not all passed")
    lint = read("reports/evidence/ruff.json")
    if lint["diagnostics"]:
        raise ValueError("Ruff has diagnostics")
    environments = {}
    for name, packages in [
        (
            ".venv",
            [
                "requests",
                "pyarrow",
                "duckdb",
                "PyYAML",
                "pydantic",
                "streamlit",
                "pandas",
                "plotly",
                "pytest",
                "ruff",
            ],
        ),
        (".venv-dbt-local", ["dbt-core", "dbt-duckdb"]),
        (".venv-dbt-snowflake", ["dbt-core", "dbt-snowflake"]),
        (".checks/clean-app", ["mobilityops", "pyarrow", "duckdb"]),
        (".checks/clean-dbt", ["dbt-core", "dbt-duckdb"]),
    ]:
        python = str(ROOT / name / "Scripts/python.exe")
        metadata = command(
            [
                python,
                "-I",
                "-c",
                "import importlib.metadata as m,json,sys; "
                "print(json.dumps({'python':sys.version.split()[0],"
                "'packages':{n:m.version(n) for n in sys.argv[1:]}}))",
                *packages,
            ]
        )
        environments[name] = {
            "versions": json.loads(metadata["stdout"]),
            "pip_check": command([python, "-I", "-m", "pip", "check"]),
            "snowflake_connection_performed": False,
        }
    main_imports = command(
        [
            str(ROOT / ".venv/Scripts/python.exe"),
            "-I",
            "-c",
            "import requests,pyarrow,duckdb,yaml,pydantic,streamlit,pandas,plotly,pytest; "
            "print('Application imports passed')",
        ]
    )
    local_dbt_imports = command(
        [
            str(ROOT / ".venv-dbt-local/Scripts/python.exe"),
            "-I",
            "-c",
            "import dbt.cli.main,dbt.adapters.duckdb; print('Local dbt imports passed')",
        ]
    )
    for path in ["reports/evidence/real_incremental.json", "reports/evidence/local_scenarios.json"]:
        if read(path)["status"] != "PASSED":
            raise ValueError("Replay/recovery/incremental evidence not passed")
    if (
        read("reports/local/fixture/fixture/latest_workflow.json")["results"]["snapshot"]["status"]
        != "VALIDATED"
    ):
        raise ValueError("Current fixture workflow did not validate its snapshot")
    clean_text = (ROOT / "reports/local/clean-fixture-run.log").read_text()
    if '"outcome": "SUCCEEDED"' not in clean_text:
        raise ValueError("Clean isolated fixture run did not succeed")
    clean_dashboard_text = (ROOT / "reports/local/clean-dashboard-pytest.log").read_text()
    clean_dashboard_match = re.search(r"(\d+) passed in ([0-9.]+)s", clean_dashboard_text)
    if not clean_dashboard_match or re.search(r"\d+ (failed|error)", clean_dashboard_text):
        raise ValueError("Clean isolated dashboard checks did not succeed")
    candidates = command(["git", "ls-files", "--others", "--exclude-standard"])[
        "stdout"
    ].splitlines()
    forbidden = [
        p
        for p in candidates
        if p.startswith(
            (
                ".venv",
                ".checks/",
                ".private/",
                "data/raw/",
                "data/db/",
                "data/classified/",
                "reports/local/",
            )
        )
        or p.endswith((".duckdb", ".duckdb.wal", ".log", ".jsonl"))
    ]
    if forbidden:
        raise ValueError(f"Git candidate contains private/runtime files: {forbidden}")
    locks = {
        p.relative_to(ROOT).as_posix(): sha256_file(p)
        for p in sorted((ROOT / "requirements").glob("*.lock.txt"))
    }
    if (
        sha256_file(ROOT / "tests/fixtures/yellow_taxi_fixture.parquet")
        != "19d61b096876bd9739a6d09a601f096d76f2186ffac87b9e281e087dc20668a8"
    ):
        raise ValueError("Preserved fixture bytes changed")
    completion = {
        "status": "LOCAL_COMPLETION_VERIFIED",
        "verified_at": utc_now(),
        "artifacts": artifacts,
        "snapshots": snapshots,
        "pytest": {
            "passed": int(match[1]),
            "elapsed_seconds": float(match[2]),
            "command": ".venv/Scripts/python.exe -m pytest -q",
            "offline": True,
        },
        "ruff": lint,
        "environments": environments,
        "lock_sha256": locks,
        "main_imports": main_imports,
        "local_dbt_imports": local_dbt_imports,
        "clean_install": {
            "status": "PASSED",
            "existing_environments_preserved": True,
            "fixture_workflow": "reports/local/clean-fixture-run.log",
            "dashboard_checks": {
                "command": ".checks/clean-app/Scripts/python.exe -m pytest -q "
                "tests/test_snapshot_dashboard.py",
                "passed": int(clean_dashboard_match[1]),
                "elapsed_seconds": float(clean_dashboard_match[2]),
                "offline": True,
            },
        },
        "real_incremental_and_replay": read("reports/evidence/real_incremental.json"),
        "isolated_recovery_and_backfill": read("reports/evidence/local_scenarios.json"),
        "git_preparation": {
            "branch": "main",
            "candidate_files": len(candidates),
            "forbidden_runtime_candidates": [],
            "remote_publication_checked": False,
        },
        "dashboard": read("reports/evidence/dashboard_smoke.json"),
        "limitations": {
            "airflow_runtime": "PENDING_DOCKER_UNAVAILABLE",
            "snowflake_execution": "UNVERIFIED_USER_ONLY",
            "github_ci_execution": "PREPARED_NOT_EXECUTED",
            "public_deployment": "NOT_DEPLOYED",
        },
    }
    if completion["dashboard"]["status"] != "PASSED":
        raise ValueError("Dashboard startup/render evidence missing")
    write_manifest_atomically(EVIDENCE / "completion.json", completion)
    print(
        json.dumps(
            {
                "status": completion["status"],
                "pytest_passed": int(match[1]),
                "real": snapshots["real"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
