# Airflow: prepared local learning runtime

**Runtime pending:** Docker/Compose are absent on the verified Windows host. No image pull,
container startup, DAG import or DAG execution has been verified. The mandatory Python
workflow works without Docker. This single-container standalone setup is for learning;
it is not the official multi-service production deployment.

The official image is pinned to **apache/airflow:3.3.2-python3.12**. Official images use the
release constraints; the matching [3.3.2/Python 3.12 constraints](https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt)
were retrieved successfully during documentation review. Application and local dbt packages
install into separate /opt/app-venv and /opt/dbt-venv environments from preserved locks,
so they do not change Airflow's own constrained packages. No Snowflake provider is installed.
See [official image guidance](https://airflow.apache.org/docs/docker-stack/index.html) and
[installation constraints](https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/installing-from-pypi.html).

## When Docker is available

Install Docker Desktop yourself through its [Windows guide](https://docs.docker.com/desktop/setup/install/windows-install/),
enable supported WSL2/Linux containers and check current OS/RAM requirements. This machine
reported about 16.85 GB total RAM, with roughly 3.2 GB free at build inspection; close
heavy programs before giving the container 4 GB. Windows support is through Linux containers
or WSL2 rather than a native Airflow installation. See [Airflow prerequisites](https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/prerequisites.html).

From the MobilityOps root in PowerShell, these are **future verification commands**:

~~~powershell
docker --version
docker compose version
# Private random session secret in this terminal; never print or commit it.
$env:AIRFLOW_API_SECRET_KEY = .\.venv\Scripts\python.exe -c 'import secrets; print(secrets.token_hex(32))'
docker compose -f airflow/compose.yaml config --quiet
docker compose -f airflow/compose.yaml build
docker compose -f airflow/compose.yaml up -d
docker compose -f airflow/compose.yaml logs --tail 80
docker compose -f airflow/compose.yaml exec airflow airflow version
docker compose -f airflow/compose.yaml exec airflow airflow dags list-import-errors
docker compose -f airflow/compose.yaml exec airflow /opt/app-venv/bin/python -m pip check
docker compose -f airflow/compose.yaml exec airflow /opt/dbt-venv/bin/python -m pip check
~~~

Keep standalone authentication output private. UI binds only to localhost:8080. The metadata
volume is airflow-home; the project bind mount is /workspace, not its Windows host path.
The DAG stays paused at creation, schedule is manual, and active runs/tasks are each capped
at one. Manually trigger mobilityops_local in the UI with scope=fixture, month=2026-01,
offline=true. Inspect all six task receipts and the actual generated verification/snapshot.
A DAG file existing is not proof those tasks ran.

The DAG calls prepare → classify → ingest → models → verify → snapshot through the tested
CLI. XCom contains stage/scope/month/report paths, never records or dataframes. Prepare has
zero Airflow retries because the downloader handles transient retries and permanent errors
must stop. Later stages allow one retry. Each subprocess has a 30-minute limit. Real months
require explicit parameters and enough available memory; run them one at a time.

For real uploads to Snowflake use **Windows host paths** in the manual client. This DAG has
no Snowflake branch or connection and is never a concurrent warehouse promotion owner.

~~~powershell
docker compose -f airflow/compose.yaml down
Remove-Item Env:AIRFLOW_API_SECRET_KEY
~~~

Down preserves the named metadata volume. Removing that volume is a separate destructive
decision; no routine command here removes it. After successful container checks, record
actual versions/image digest/task IDs/times and update pending status from evidence.
