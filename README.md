# MobilityOps

[![Offline fixture, SQL and snapshot checks](https://github.com/PuravDesai004/MobilityOps/actions/workflows/offline-checks.yml/badge.svg?branch=presentation%2Fsql-business-findings)](https://github.com/PuravDesai004/MobilityOps/actions/workflows/offline-checks.yml?query=branch%3Apresentation%2Fsql-business-findings)

## Purpose

MobilityOps turns NYC Yellow Taxi deliveries into traceable operations metrics. It handles
malformed records and repeated deliveries, reconciles counts and money, and exposes observed
pickup activity, monthly changes and quality warnings. Recruiters can run its historical
dashboard without raw monthly data, a private database or warehouse credentials.

[Setup](#setup-and-pipeline-commands) · [Demo](#dashboard-and-three-minute-walkthrough) ·
[Findings](#measured-business-findings) · [Detailed architecture](architecture.md)

## Technologies actually used

Python, Requests, PyArrow/Parquet, PyYAML, pydantic, Decimal, DuckDB, dbt-duckdb,
Streamlit, pandas, Plotly, pytest, Ruff, PowerShell, Git and GitHub Actions.
Node runs the optional official Ruff WASM fallback on this Windows machine.
Snowflake and Airflow are prepared assets; their execution status is below.

## Architecture overview

Official TLC files → checksummed manifests, schema contracts and bounded Arrow profiling →
shared Python classification → atomic accepted/quarantine outputs → transactional DuckDB →
six dbt models → independent reconciliation → seven SQL analyses and validated snapshots →
credential-free Streamlit dashboard.

Identity is original artifact SHA-256 plus one-based physical row position. Equal payloads
at different positions stay distinct; rerunning a delivery adds no new logical keys.
Fixture, sample and real sources use separate datasets, databases, manifests and exports.
See [implemented architecture](architecture.md) for functions, grains, worked examples,
recovery boundaries, SQL explanations and interview questions.

## Verified dataset coverage and reconciliation

These scopes completed the local pipeline, including classification, ingestion, dbt and
reconciliation; the real results are preserved runs from 10 October 2026, not profiling alone.

| Scope / source-file period | Source rows | Accepted | Quarantined | Accepted recorded charge USD | Evidence |
| --- | ---: | ---: | ---: | ---: | --- |
| Deterministic fixture | 12 | 8 | 4 | 134.00 | [receipt](reports/evidence/fixture_fixture.json) |
| Separate January sample | 10,000 | 10,000 | 0 | 293,899.66 | [receipt](reports/evidence/sample_2026-01.json) |
| January 2026 | 3,724,889 | 3,724,888 | 1 | 108,686,701.73 | [receipt](reports/evidence/real_2026-01.json) |
| February 2026 | 3,399,866 | 3,399,864 | 2 | 102,381,010.56 | [receipt](reports/evidence/real_2026-02.json) |
| March 2026 | 3,952,451 | 3,952,449 | 2 | 118,958,427.44 | [receipt](reports/evidence/real_2026-03.json) |
| **Three full source files, Jan–Mar 2026** | **11,077,206** | **11,077,201** | **5** | **330,026,139.73** | [analysis receipt](reports/analysis/real/business_insights.json) |

Every real artifact has **zero count differences and $0.00 differences** between accepted
records, facts, aggregate marts and an independently calculated eligible-source reference.
Source = accepted + quarantined; stable keys are unique and dispositions are disjoint.
These checks do not prove complete real-world data accuracy.

Actual retained pickup dates span **2008-12-31 to 2026-04-01**; source-file months remain
January–March 2026. Valid out-of-partition dates are retained with warnings. Records are
physical source rows, not globally deduplicated journeys. Charge is signed recorded passenger
charge, including adjustments; cash tips are incomplete, and this is not profit or complete revenue.
The deterministic sample has its own checksum and parent lineage; it is not a population estimate.

## Measured business findings

All findings below concern accepted records in the three verified January–March 2026 source
files. Exact query hashes, artifact identities, definitions and result checksums are in the
[SQL export receipt](reports/analysis/real/business_insights.json).

| Observation | Measured evidence | Interpretation and limit |
| --- | --- | --- |
| Pickup concentration | Upper East Side South: **479,826 / 11,077,201 = 4.33%** of accepted pickups. Its peak is **17:00–18:00 New York wall clock**, with **38,469 / 479,826 = 8.02%** of the zone's pickups. [SQL](sql/analysis/pickup_zone_hour.sql) · [CSV](reports/analysis/real/pickup_zone_hour.csv) | Input for investigating vehicle positioning or staffing. These pickups do not measure all transport demand or establish a staffing benefit. |
| Month-length comparison | February has **325,024 fewer accepted records (−8.73% of January's 3,724,888)**, but in-month pickups/calendar day rise **1.05%**. March count rises **16.25% of February's 3,399,864**, and daily rate rises **5.00%**. [SQL](sql/analysis/monthly_changes.sql) · [CSV](reports/analysis/real/monthly_changes.csv) | Compare daily workload as well as monthly totals; weekday mix, seasonality and reporting remain unadjusted. These are observations, not improvements caused by the project. |
| Warning concentration | **3,186,377 / 11,077,201 = 28.77%** of accepted records have ≥1 warning. January Astoria: **8,593 / 10,044 = 85.55%**, versus January's **1,142,828 / 3,724,888 = 30.68%**. [month SQL](sql/analysis/quality_month.sql) · [zone SQL](sql/analysis/quality_zones.sql) · [CSV](reports/analysis/real/quality_zones.csv) | Investigate optional-field coverage before passenger/payment segmentation. Zone flags require ≥10,000 accepted records and a rate above the source-month baseline; this is screening, not statistical significance. |

Daily rate = accepted pickups within their own source-file month / calendar days. Daily-rate
MoM = (current rate − previous adjacent-month rate) / previous rate × 100; unavailable or
zero-denominator comparisons stay null. Displayed values are rounded; exports retain precision.

| Month | In-month accepted numerator | Calendar days | Pickups/day | Daily-rate MoM |
| --- | ---: | ---: | ---: | ---: |
| January 2026 | 3,724,881 | 31 | 120,157.45 | unavailable |
| February 2026 | 3,399,848 | 28 | 121,423.14 | +1.05% |
| March 2026 | 3,952,430 | 31 | 127,497.74 | +5.00% |

Only daily normalization excludes **7 January / 16 February / 19 March records**:
**42 records and $1,191.26** in total. Its scope is 11,077,159 records and $330,024,948.47;
headline counts/charges retain all 42. [Coverage SQL](sql/analysis/pickup_month_coverage.sql)
and [CSV](reports/analysis/real/pickup_month_coverage.csv) explain every difference.
Observed dates do not prove complete daily reporting; naive timestamps have DST limitations.

Warning rates count **distinct accepted records**, not warning occurrences. Different codes
overlap; missing-passenger and unknown-payment codes each affect 3,057,120 records across
Jan–Mar, without proving they affect the same records. [Monthly quality](reports/analysis/real/quality_month.csv)
and [per-code results](reports/analysis/real/warning_codes.csv) retain their denominators.
All **5 / 11,077,206 = 0.00004514%** quarantines have DROPOFF_BEFORE_PICKUP
([reason results](reports/analysis/real/quarantine_codes.csv)). Warnings do not prove incorrect journeys.

## Setup and pipeline commands

Run from the repository root in PowerShell. **Existing workspace:** use installed
.venv and .venv-dbt-local; no reinstallation is needed. Verified runtime: Python 3.14.7,
dbt-core 1.12.5, dbt-duckdb 1.11.0. [Exact app/local-dbt locks](requirements/)
and [completion versions](reports/evidence/completion.json) record the remaining packages.
Minimum source syntax is Python 3.12; that alone does not verify every dependency on 3.12.

~~~powershell
# New clone only: create isolated environments and install preserved exact locks.
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
python -m venv .venv-dbt-local
.\.venv-dbt-local\Scripts\python.exe -m pip install -r requirements/dbt-local.lock.txt
~~~

~~~powershell
# Existing workspace or installed clone: bounded offline demonstration.
$env:PYTHONPATH = "$PWD\src"
.\scripts\run_local.ps1 -Scope fixture -Offline
.\.venv\Scripts\python.exe scripts/run_analysis.py --database data/db/fixture.duckdb --scope fixture --verification reports/local/fixture/fixture/verification.json --output reports/analysis/fixture --snapshot demo/fixture
.\.venv\Scripts\python.exe -m mobilityops.cli validate-snapshot --path demo/fixture
~~~

The wrapper selects explicit **local** dbt; activation is optional:
**.\.venv\Scripts\Activate.ps1**, then deactivate before switching environments.
With preserved inputs, use **-Scope sample -Month 2026-01 -Offline** or
**-Scope real -Month 2026-01 -Offline**; process real months sequentially. A fresh clone has
no raw months: omit -Offline only for deliberate source preparation. Changed active
checksums stop for revision review. Stage/resume and revision commands are in the
[operations runbook](docs/operations_runbook.md).

~~~powershell
# Existing verified real DB only: read-only SQL, then publish a fresh analysis export.
.\.venv\Scripts\python.exe scripts/run_analysis.py --snapshot demo/published
# Offline validation; works in a fresh clone with the committed bundle.
.\.venv\Scripts\python.exe -m mobilityops.cli validate-snapshot --path demo/published
~~~

Seven [SQL files](sql/analysis/) produce [seven CSVs and JSON](reports/analysis/real/).
Analysis checks the active artifact set and live totals. Snapshot attachment preserves the
original three data files byte-for-byte and adds checksummed analysis in a new generation.
Run analysis after a pipeline snapshot refresh; a fresh clone can review exports without the DB.

## Dashboard and three-minute walkthrough

~~~powershell
$env:PYTHONPATH = "$PWD\src"
.\.venv\Scripts\python.exe -m mobilityops.cli validate-snapshot --path demo/published
.\.venv\Scripts\python.exe -m streamlit run app/streamlit_app.py --server.address 127.0.0.1 --server.port 8502 --browser.gatherUsageStats false
~~~

Open localhost:8502; stop with Ctrl+C. Default input is the small verified demo/published
bundle. For fixture review, set **$env:MOBILITYOPS_SNAPSHOT_DIR = "$PWD\demo\fixture"**;
remove that variable afterward. The app reads validated exports only and triggers no pipeline
or warehouse operation. .env.example shows explicit overrides; it is not automatically loaded.

1. **0:00–1:00 — Operations:** explain source months versus actual dates; filter a date/zone.
   Counts, signed USD charge, miles and weighted minutes change together. The chart displays
   the 12 busiest selected zones; headline metrics include every selected zone.
2. **1:00–1:50 — Business Insights:** Jan–Mar scope stays fixed when Operations filters change.
   Explain February's monthly/daily contrast and LAG/NULLIF in the linked SQL.
3. **1:50–2:40 — Reliability:** source = accepted + quarantined; show zero differences and
   real January zero-insert replay. Label rollback/retry/backfill as separate fixture evidence.
4. **2:40–3:00 — Architecture:** show artifact/row identity and incremental models, then
   distinguish executed local/CI features from pending Snowflake, Airflow and public hosting.

![Operations metrics and pickup-hour chart](assets/screenshots/operations.jpg)

![Reliability reconciliation and quality](assets/screenshots/reliability.jpg)

![Business Insights with fixed scope](assets/screenshots/business_insights.jpg)

Genuine unedited localhost:8502 captures show real data, all dates/zones for Operations and
fixed Jan–Mar Insights. [Capture receipt](reports/evidence/presentation_dashboard.json) records
hashes, provenance and 1280×1024 review. [Earlier replay/recovery capture](docs/screenshots/replay_recovery.jpg)
and [original app receipt](reports/evidence/dashboard_smoke.json) remain historical evidence.
For replacements, wait for complete charts/loading to finish; retain filters, units, origin,
dates and export timestamps, and review values/axes/legends before replacing images.

## Reliability and executed validation

- **Classification:** required columns gate the artifact; blocking row reasons quarantine
  one record once. Optional-field warnings stay attached to accepted records. Original values,
  signed Decimal rounding and provenance are preserved. [Contract](config/contracts/yellow_taxi_v1.yaml)
  · [metric dictionary](docs/data_dictionary.md).
- **Replay:** real January replay processed 3,724,889 source keys and inserted **0 accepted /
  0 quarantined**; facts stayed 11,077,201. Counts/charges reconcile at zero differences.
  [Real increment/replay evidence](reports/evidence/real_incremental.json).
- **Recovery:** isolated fixture rollback leaves accepted/quarantine/artifact/audit tables
  empty; retry produces 8/4/1/1; replay inserts zero. Feb → Mar → older-Jan fixtures yield
  2 → 4 → 6 facts and replay keeps six. [Original scenarios](reports/evidence/local_scenarios.json).
- **Checks on 11 October:** **81 offline tests**, zero Ruff diagnostics, app/local-dbt
  dependency checks, six dbt models / 24 data tests, fixture SQL, snapshot validation and
  dashboard AppTest passed. Fixture recheck: 12 source / 8 accepted / 4 quarantined / $134.00,
  zero count/currency differences; 665 protected files unchanged.
  [Cleanup verification](reports/evidence/cleanup_verification.json).
- **Executed CI:** every step passed for cleanup commit
  [467dcba](https://github.com/PuravDesai004/MobilityOps/commit/467dcba0af3a46314da707e022c50a4b54ff6c05),
  [GitHub run 38152281988](https://github.com/PuravDesai004/MobilityOps/actions/runs/38152281988).
  The [offline workflow](.github/workflows/offline-checks.yml) checks fixture/replay, tests,
  SQL/snapshots and recovery in separate exact-lock app/dbt environments. This is fixture
  CI, not full-month processing; later commits have their own run status.

~~~powershell
$env:PYTHONPATH = "$PWD\src"
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -I -m pip check
.\.venv-dbt-local\Scripts\python.exe -I -m pip check
node scripts/lint_wasm.cjs
~~~

Windows Application Control blocks native Ruff/dbt here. The retained dbt Python API wrapper
and official Ruff 0.16.10 WASM work; -I prevents app metadata leaking into dependency checks.
New clones needing WASM can run scripts/install_ruff_wasm.py; native Ruff may be used where
allowed. Keep isolated recovery paths short on Windows. See the runbook for diagnostics.

## Limitations and pending work

- **Snowflake:** templates, shared-policy ZIP, 19 policy vectors and isolated dbt-snowflake
  environment are prepared; warehouse compilation/execution, COPY alignment, Streams, Tasks,
  privileges and costs remain **unverified and user-run only**. Start with the
  [17-step manual guide](docs/snowflake_implementation.md). No agent Snowflake action occurred.
- **Airflow/Docker:** Docker/Compose are absent; pinned apache/airflow:3.3.2-python3.12,
  constraints and DAG assets are prepared, **runtime pending**. [Instructions](airflow/README.md).
- **Public hosting: not deployed.** For later manual hosting, follow
  [deployment](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app)
  and [dependency guidance](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/app-dependencies):
  select this repository/branch, app/streamlit_app.py, root requirements.txt, a supported
  runtime matching the verified environment, and no secrets. Validate the bundle first and
  personally check tabs/filters/origin/dates afterward. Verify an alternate runtime in isolation.
  GitHub publication/CI does not deploy the app.
- **Operational scope:** single-writer local DuckDB and fixed historical exports; no SLA,
  real-time feed, causal business improvement or measured performance improvement is claimed.
  Source-revision adoption needs deliberate review. Trial expiry does not affect the local demo.

Original raw inputs, databases, private logs and environments remain local and Git-ignored.
Cleanup removed duplicate validation environments and abandoned test/export state, with a
verified sibling backup; original source bytes, real history, selected dbt docs, screenshots,
locks and the active demo remain. Ordinary commits retain earlier Git history. Work is on
**presentation/sql-business-findings**; default **main** awaits review/merge.

## Focused documentation and evidence

| Document / artifact | Purpose |
| --- | --- |
| [architecture.md](architecture.md) | One implemented architecture; concepts, grains, journeys and interview explanations |
| [Operations runbook](docs/operations_runbook.md) | Stage/resume, revision, replay, recovery and diagnostics |
| [Data dictionary](docs/data_dictionary.md) | Source attribution, field contracts, units and analytical grains |
| [Completion matrix](docs/completion_audit.md) | Original requirement-to-code/test/evidence mapping; historical statuses labeled |
| [Resume and handoff](docs/resume_and_handoff.md) | Actual resume evidence equivalent; interview defense and conditional cloud wording |
| [Manual Snowflake guide](docs/snowflake_implementation.md) · [evidence template](docs/evidence_template.md) | Ordered user-only warehouse work and blank actual-result capture |
| [Evidence](reports/evidence/) · [SQL exports](reports/analysis/real/) · [demo manifest](demo/published/snapshot_manifest.json) | Immutable verification, CI, reconciliation, analysis and provenance |

Business findings, demo walkthrough and current progress are consolidated above; learning
explanations are in architecture.md. Original completion/publication/link receipts remain
unchanged historical observations, even where they name earlier documentation paths or
snapshot generations. Removed documents are recoverable exactly from commit **467dcba**.
The [original completion](reports/evidence/completion.json) and
[presentation receipt](reports/evidence/presentation_verification.json) retain their own dates,
versions, commands and results; they are not rewritten as current status.

Consolidation rechecked 62 local links/images (including seven section anchors), five scope
receipts, SQL-export figures, both snapshots and 27 dashboard/analysis tests. All 655 protected
non-document assets were unchanged. No full dataset rerun, download or Snowflake action occurred.

