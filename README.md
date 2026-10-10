# MobilityOps

A working local NYC Yellow Taxi pipeline: official source → immutable Parquet/manifests →
bounded profiling/contracts → one Python classifier → transactional DuckDB → dbt →
reconciled snapshots → credential-free Streamlit dashboard.

**Verified locally, 10 October 2026:** three consecutive official months, 11,077,206 source
records, 11,077,201 accepted records, 5 quarantined records, and **$330,026,139.73 accepted
recorded passenger charge**. Every artifact's count and monetary differences are zero.
These are historical source records, not globally deduplicated physical trips or profit.
The final suite passed **65 offline tests**, Ruff reported **zero diagnostics**, and every
scope's local dbt build passed six models and 24 data tests. Clean isolated installation,
fixture workflow and 11 dashboard checks passed with the existing environments preserved.

| Scope | Source | Accepted | Quarantined | Accepted recorded charge USD |
| --- | ---: | ---: | ---: | ---: |
| Preserved deterministic fixture | 12 | 8 | 4 | 134.00 |
| Separate January sample | 10,000 | 10,000 | 0 | 293,899.66 |
| January 2026 | 3,724,889 | 3,724,888 | 1 | 108,686,701.73 |
| February 2026 | 3,399,866 | 3,399,864 | 2 | 102,381,010.56 |
| March 2026 | 3,952,451 | 3,952,449 | 2 | 118,958,427.44 |

## Launch the demo

From PowerShell in the existing workspace:

~~~powershell
Set-Location 'C:\Users\purav\OneDrive\Desktop\Data Projects\MobilityOps'
$env:PYTHONPATH = "$PWD\src"
.\.venv\Scripts\python.exe -m mobilityops.cli validate-snapshot --path demo/published
.\.venv\Scripts\python.exe -m streamlit run app/streamlit_app.py --server.address 127.0.0.1 --browser.gatherUsageStats false
~~~

Open localhost:8501; stop with Ctrl+C. Operations shows date/zone filters, demand, signed
charge and weighted distance/duration. Reliability shows dispositions, reconciliation and
actual replay/recovery evidence. The app reads only checksummed small snapshots, has no
warehouse credentials, no cloud fallback and no visitor-triggered pipeline run.

## Run the complete local workflow

The wrapper uses .venv for Python and .venv-dbt-local for explicit **--target local** dbt.
Activation/switching is unnecessary. Existing environments and locks are preserved.

~~~powershell
.\scripts\run_local.ps1 -Scope fixture -Offline
.\scripts\run_local.ps1 -Scope sample -Month '2026-01' -Offline
.\scripts\run_local.ps1 -Scope real -Month '2026-01' -Offline
.\scripts\run_local.ps1 -Scope real -Month '2026-02' -Offline
.\scripts\run_local.ps1 -Scope real -Month '2026-03' -Offline
~~~

Drop -Offline only when source preparation/network access is needed. Process real months
sequentially. Fixture/sample/real use separate databases. Replays add no logical source
keys. A changed active source checksum requires deliberate revision review.

Stages are prepare/classify/ingest/models/verify/snapshot; **-Stage models** resumes modeling
after a failure, followed by verify/snapshot. Errors return a failing exit code and append
attempt diagnostics; completed source and snapshot generations survive failed later attempts.

~~~powershell
$env:PYTHONPATH = "$PWD\src"
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -I -m pip check
.\.venv-dbt-local\Scripts\python.exe -I -m pip check
.\.venv\Scripts\python.exe scripts/verify_local_scenarios.py
~~~

Use **python -m ruff check src tests app scripts airflow/dags** where native Ruff is allowed.
This laptop's Windows Application Control blocks native Ruff/dbt executables; local dbt uses
its installed Python API. The official Ruff 0.16.10 WASM fallback is project-local:

~~~powershell
# Existing .tools install is already present. For a new clone only:
.\.venv\Scripts\python.exe scripts/install_ruff_wasm.py
node scripts/lint_wasm.cjs
~~~

Node is needed only for that fallback, not the pipeline/dashboard. Its integrity receipt
is requirements/ruff-wasm.lock.json. Tests are offline, use mocked downloads and isolated
failure fixtures, and never run a real TLC download or Snowflake action.

## New clone installation

These are reproduction commands, not required reinstallation in this existing folder.
The verified local runtime is Python 3.14.7; minimum source syntax is Python 3.12.

~~~powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
python -m venv .venv-dbt-local
.\.venv-dbt-local\Scripts\python.exe -m pip install -r requirements/dbt-local.lock.txt
.\scripts\run_local.ps1 -Scope fixture -Offline
~~~

Root requirements use the existing exact application lock and editable local package.
Local dbt has its own exact lock. Isolated clean app/dbt installs and a full fixture run
were actually verified without replacing existing environments. For a fresh clone, real
data is intentionally absent; first run a real month without -Offline to prepare it.
Optional activation: **.\.venv\Scripts\Activate.ps1** (then deactivate before switching).

## Deliverables and evidence

| Read | Contents |
| --- | --- |
| [architecture.md](architecture.md) | Implemented architecture, functions, grains, policies, walkthroughs, glossary and interview answers |
| [docs/progress.md](docs/progress.md) | Confirmed checks, current status and resume context |
| [docs/completion_audit.md](docs/completion_audit.md) | Requirement-by-requirement evidence and scope boundaries |
| [docs/operations_runbook.md](docs/operations_runbook.md) | Resume/replay/revision/failure and shutdown commands |
| [docs/data_dictionary.md](docs/data_dictionary.md) | Source/curated/analytical fields and metric interpretation |
| [docs/demo_walkthrough.md](docs/demo_walkthrough.md) | Recruiter demonstration and optional public-hosting instructions |
| [docs/resume_and_handoff.md](docs/resume_and_handoff.md) | Evidence-backed local wording and conditional cloud wording |
| [docs/snowflake_implementation.md](docs/snowflake_implementation.md) | Complete ordered user-run warehouse steps |
| [reports/evidence/](reports/evidence/) | Actual machine-readable checks and completion audit |

Docker is unavailable: Airflow DAG/Docker setup is **prepared, runtime pending**; see
[airflow/README.md](airflow/README.md). Snowflake SQL/UDF/profile templates are **prepared,
execution unverified** and only the user may run them. GitHub publication/CI status is
recorded in docs/progress.md; completion.json preserves the earlier local-build audit.
No Snowflake connection, credentials, hosted app deployment or message was performed.
`.env.example` documents explicit local snapshot/interpreter overrides; it is not loaded
automatically. `config/snowflake.example.yaml` is a manual object-name reference only.

Original monthly source, environments, databases and private logs are ignored by Git.
Repository attributes preserve exact file bytes across clones, including checksummed
snapshots, the shared-policy package and original dependency locks.
The preserved 12-row fixture and small demo/published snapshot bundle are repository-ready.
The fixture expectation file's old expected_only flags remain historical documentation;
actual completed classification results are saved separately. Removing this project folder
later removes its local files/environments; globally installed Python remains separate.

