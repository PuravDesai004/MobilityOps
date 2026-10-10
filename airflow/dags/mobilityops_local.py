"""Prepared local orchestration. Docker/Airflow runtime verification is pending."""

import os
import subprocess
from datetime import datetime, timedelta

from airflow.sdk import DAG, Param, task

with DAG(
    "mobilityops_local",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "scope": Param("fixture", enum=["fixture", "sample", "real"]),
        "month": Param("2026-01", pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
        "offline": Param(True, type="boolean"),
    },
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mobilityops", "local-only"],
) as dag:

    @task
    def execute_stage(stage: str, params: dict, upstream_receipt: dict | None = None):
        del upstream_receipt
        environment = dict(
            os.environ,
            PYTHONPATH="/workspace/src",
            MOBILITYOPS_DBT_PYTHON="/opt/dbt-venv/bin/python",
        )
        command = [
            "/opt/app-venv/bin/python",
            "-m",
            "mobilityops.cli",
            "run-local",
            "--scope",
            params["scope"],
            "--month",
            params["month"],
            "--stage",
            stage,
        ]
        if params["offline"] or stage != "prepare":
            command.append("--offline")
        subprocess.run(command, cwd="/workspace", env=environment, check=True, timeout=1800)
        return {
            "stage": stage,
            "scope": params["scope"],
            "month": params["month"],
            "report_root": f"/workspace/reports/local/{params['scope']}",
        }

    prepared = execute_stage.override(task_id="prepare_source", retries=0)("prepare")
    classified = execute_stage.override(task_id="classify")("classify", upstream_receipt=prepared)
    ingested = execute_stage.override(task_id="ingest")("ingest", upstream_receipt=classified)
    modeled = execute_stage.override(task_id="local_dbt")("models", upstream_receipt=ingested)
    verified = execute_stage.override(task_id="reconcile")("verify", upstream_receipt=modeled)
    exported = execute_stage.override(task_id="validated_snapshot")(
        "snapshot", upstream_receipt=verified
    )
