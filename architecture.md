# MobilityOps: implemented architecture and learning guide

## 1. Problem and outcome

A mobility operator needs trustworthy trip counts, recorded passenger charges, and pickup
zone/hour demand. Monthly deliveries can arrive twice, contain malformed values, or fail
halfway through processing. MobilityOps makes the original source, each disposition, and
each reported total traceable. Its working implementation runs on this Windows laptop
without a cloud account. It is a portfolio system with measured correctness, rather than
an operated service with availability guarantees.

The implementation is Python → DuckDB → dbt → validated snapshots → Streamlit. January
through March 2026 have completed verification; see `reports/evidence/completion.json` for
the final coverage and checks, and `docs/progress.md` for current work. Generated Snowflake
objects and the Airflow DAG are separate prepared deliverables. Local transactions do not
demonstrate Snowflake Streams, Tasks, permissions, or billing.

```mermaid
flowchart TD
    TLC[Official TLC Parquet and zone CSV] --> D[Streaming downloader]
    D --> RAW[Original files + SHA-256 manifests]
    RAW --> P[Contract + bounded Arrow profile]
    P --> C[One Python classifier]
    C --> A[Accepted Parquet]
    C --> Q[Quarantined Parquet]
    A --> I[Transactional DuckDB ingestion]
    Q --> I
    I --> S[dbt staging and dimensions]
    S --> F[Incremental trip fact]
    F --> M[Rebuilt aggregate marts]
    RAW --> R[Independent Decimal reference]
    M --> V[Per-artifact reconciliation]
    R --> V
    V --> E[Checksummed small snapshots]
    E --> UI[Streamlit Operations + Reliability]
    O[Python workflow / local CLI] -. coordinates .-> D
    O -. coordinates .-> C
    O -. coordinates .-> I
    O -. coordinates .-> V
    AF[Prepared Airflow DAG: runtime pending] -. same CLI stages .-> O
    RAW -. user uploads later .-> SF[Manual Snowflake phase: unverified]
```

## 2. Technologies and responsibilities

| Technology | What it contributes here | Concrete example |
| --- | --- | --- |
| Python | Downloads, validates, classifies, orchestrates and verifies | A missing pickup creates `MISSING_PICKUP` |
| Requests | HTTP streaming and bounded connection/read timeouts | Write response chunks directly to a temporary file |
| Parquet | Typed, compressed, column-oriented source storage | Read only four required columns for the independent money reference |
| PyArrow | Parquet metadata and batches without whole-month pandas loading | A 10,000-row batch becomes small Python row dictionaries |
| SHA-256 | Content identity and corruption detection | A changed byte changes the artifact checksum |
| YAML/Pydantic | Readable configuration and validated settings | Reject an invalid month or nonpositive batch size |
| Decimal | Exact recorded charge rounding and addition | `1.005` becomes `1.01`, `-1.005` becomes `-1.01` |
| DuckDB | Embedded local analytical database and transactions | Accepted/quarantine inserts and the committed audit publish together |
| dbt | SQL model dependencies, incremental fact, tests and model documentation | `ref('fct_trip')` establishes the mart dependency |
| pandas/Plotly/Streamlit | Read small aggregates, filter, chart and render the demo | A selected date range updates weighted averages |
| pytest/Ruff | Executable correctness checks and static code checks | Inject a failure before commit and assert targets are unchanged |
| Docker/Airflow | Prepared separate orchestration runtime | A manual, paused DAG calls the same stage CLI; runtime pending |
| Snowflake | Future user-run warehouse implementation | Streams and transactional promotion are templates, not verified local features |

The TLC source links were checked against the [official TLC page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
The source dictionary describes `total_amount` as a recorded passenger charge with incomplete
cash-tip information. It is not profit or complete business revenue. See the
[Yellow Taxi dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).

## 3. Files and entry points

| Path | Important entry point | Responsibility |
| --- | --- | --- |
| `config/project.yaml` | `load_project_config()` in `config.py` | Paths, month, batch size, retries/timeouts; no credentials |
| `config/contracts/yellow_taxi_v1.yaml` | `validate_required_columns()` | Required versus optional source fields |
| `src/mobilityops/cli.py` | `main()` | One command-line entry point |
| `scripts/run_local.ps1` | PowerShell wrapper | Uses the existing app Python and the separate local dbt Python |
| `.env.example`, `config/snowflake.example.yaml` | Reference examples only | Explicit local overrides and future manual object names; not automatic configuration loading |
| `download.py`, `catalog.py`, `real_source.py` | `download_artifact()`, `prepare_real_source()` | Official URLs, validated downloads, profiles and sample manifests |
| `profile.py`, `sample.py` | `profile_parquet()`, `create_deterministic_sample()` | Metadata/null counts and evenly spaced parent-row selection |
| `identity.py`, `manifest.py` | `sha256_file()`, `trip_record_id()`, `write_manifest_atomically()` | Stable identities, atomic metadata and append-only attempt events |
| `policy.py` | `classify_row()`, `decimal_value()`, `timestamp_value()` | Single pure Python classification policy |
| `classify.py` | `classify_artifact()` | Batches, typed outputs, immutable generations and publication pointer |
| `storage.py` | `ingest_classified()`, `connect_local()` | Stable-key local ingestion and rollback/replay behavior |
| `reference.py`, `reconcile.py` | `eligible_source_charge()`, `reconcile()` | Independent raw-source reference and per-artifact gates |
| `workflow.py` | `run_workflow()`, `run_dbt()` | Ordered stages, failing exit codes, versions, command and timing receipts |
| `dbt/models/`, `dbt/tests/` | SQL models and data tests | Curated sources → dimensions/fact/marts |
| `snapshot.py` | `export_snapshot()`, `load_snapshot()`, `filtered_metrics()` | Candidate validation, checksums, schemas and weighted metrics |
| `app/streamlit_app.py` | `main()` | Reads snapshots only; does not run a pipeline or contact a warehouse |
| `scripts/verify_local_scenarios.py` | `main()` | Isolated fixture rollback, retry, replay, incremental and backfill evidence |
| `scripts/package_demo.py` | `main()` | Copies a verified small bundle to `demo/published` for review/hosting |
| `airflow/` | `mobilityops_local` DAG | Prepared Docker runtime and sequential local stages |
| `sql/snowflake/` | Ordered SQL and shared-policy ZIP | User-run templates with explicit unverified status |
| `.github/workflows/offline-checks.yml` | Fixture job | Prepared CI with no cloud secrets and no real-month download |

Names without a directory in this table are under `src/mobilityops`. `tests/fixtures` and
`tests/expected` are the preserved tiny source and reviewed expectations. Raw monthly
files, classified files, private databases, environments and logs are ignored by Git.
`reports/evidence` and the small published demo are reviewable evidence artifacts.

## 4. Identity, source-row grain and manifests

**Grain** means what one row represents. The original source grain is one physical row in
one unchanged Parquet file. It does not mean one globally unique taxi journey: TLC supplies
no reliable physical-trip key for this design.

```text
artifact_id = sha256(original file bytes)
source_row_number = 1-based physical position in that file
trip_record_id = sha256(UTF-8(artifact_id + ':' + source_row_number))
```

For artifact `abc…` row 2, the identity input is `abc…:2`. Moving the file, changing the run
ID, or trying again cannot change the ID. Two equal payloads at positions 1 and 2 remain two
records. Replaying position 1 adds zero logical records; it is not fuzzy deduplication of
trips from different source files. Database keys include dataset scope. Fixture, sample
and real data use separate DuckDB databases, classification directories, manifests and
snapshots. The real database retains each month's artifact and source partition.

A manifest is a receipt describing an artifact: checksum, bytes, schema, dataset, source
URL/month, rows, contract and zone lookup checksum. Attempt history is a separate JSONL
file, with one JSON event per line. A later failed attempt does not rewrite a prior success.
Writes go to a same-directory temporary file, are flushed, and use atomic replacement.
Readers see either the old completed receipt or the new completed receipt.

The sample selects at most 10,000 evenly spaced physical rows. With 12 parent rows and a
limit of 3, it picks positions 1, 6, 12. Its own Parquet bytes produce a separate artifact
hash. Its local positions 1, 2, 3 generate sample IDs; parent artifact and parent physical
positions remain attached. It is a convenience sample, not a random sample or population
estimate. Combining it with its full source would count overlapping records twice.

If a month already has an active artifact and the source bytes change, preparation,
classification or ingestion stops with a revision-review error. Adopt a revision only in
a deliberate fresh scope after reviewing counts and money. Automatic replacement and
simultaneously active versions are not implemented.

## 5. Streaming, profiling and the contract

The downloader writes network chunks to disk, rather than collecting the whole response
in memory. It uses bounded timeouts and up to three attempts for transient connection,
timeout and retryable HTTP failures. A missing 404 is permanent and stops promptly.
The temporary artifact must be readable Parquet/CSV and satisfy the file contract before
publication. The checksum describes actual bytes, not a URL, filename or HTTP ETag.
An unsuccessful replacement preserves the previous valid file. On rerun the saved manifest,
local checksum and remote validators support verified unchanged reuse.

Some direct requests to the TLC listing return 403 here. `catalog.py` records that failure
and permits only the exact Jan/Feb/Mar/zone links independently reviewed on the official
page on 10 October 2026. It is not a general allowlist of invented months. New links need
fresh official verification.

Parquet profiling records rows, row groups, physical/logical schema, file size and relevant
null counts. Arrow batches cap processing memory. Full raw months never go through pandas;
pandas is reserved for small snapshot aggregates. The four required columns are pickup,
dropoff, distance and total amount. A missing column is a **file failure**, whereas a missing
value in an existing column is a **row reason**. Optional and additional columns survive in
the original file and the curated original-payload JSON.

## 6. Classification, types and disposition

`policy.classify_row()` returns canonical typed values, provenance, original payload and
sorted distinct reasons/warnings. Any blocking reason gives one quarantined record. Multiple
reasons do not make multiple rejected records. Warnings alone retain an accepted record.

| Blocking reasons | Examples |
| --- | --- |
| Missing/invalid pickup or dropoff | Null pickup or a malformed date string |
| Dropoff before pickup | A negative trip duration |
| Missing/invalid/negative distance | NaN, overflow, or even `-0.00001` before rounding |
| Missing/invalid total amount | Null, infinity, text, or decimal overflow |

Warnings cover unresolved zones, missing/zero passengers, negative charge, out-of-month
pickup, unknown payment code, and duration greater than 86,400 seconds. The last is a
project warning threshold, not an official TLC validity rule. Known payment codes are
1–6. Optional values are nullable, rather than guessed or filled with zero.

Money is `DECIMAL(18,2)` and distance `DECIMAL(18,3)`. Conversion uses `Decimal(str(value))`,
not `Decimal(binary_float)`, and `ROUND_HALF_UP`, whose ties go away from zero for either
sign. Both input and rounded result must fit precision 18. `9999999999999999.994` is
valid charge; rounding `9999999999999999.995` overflows and produces `INVALID_TOTAL_AMOUNT`.
Negative signed charges remain negative; they are warnings, not discarded revenue.
Distance negativity is checked before rounding, so a tiny negative cannot become valid zero.

Timestamps are naive New York wall-clock values, stored at microsecond precision. Timezone
offset inputs are invalid under this contract. No UTC offset is invented during ambiguous
daylight saving time hours. Duration is elapsed naive wall-clock difference, not a claim of
UTC accuracy across DST. Valid pickup dates outside the file month remain accepted with
a warning and appear in the date dimension and dashboard coverage.

Unrecognized source zone IDs are retained in `*_zone_source_id` and map to dimension key
0 (`Unresolved`). Official zone members 264/265 with an Unknown label remain valid lookup
members. Fixture zones and official real/sample zones have distinct lookup checksums.

Broken checksum/row identity or parent lineage is a processing failure, not a quarantined
business record. Output publication writes accepted/quarantine Parquet into a new immutable
generation. Only a completed, checksummed `classification.json` pointer exposes it. A failed
candidate is removed from its controlled generation directory; the prior success survives.

## 7. DuckDB publication and model grains

`ingest_classified()` checks checksums, complete row/key coverage, reason dispositions,
zone revision and conflicts. It opens one local transaction for accepted inserts,
quarantine inserts, zones, artifact receipt and successful ingestion audit. Only absent
stable keys are inserted. A failure rolls back all these writes and rethrows. Separate
attempt history records the failure. Table bootstrap is done before the transaction.
DuckDB is configured for two threads and a 1 GB SQL memory limit; this is not a hard limit
on the entire Python process. It may spill to local disk. One workflow writer runs at a time.

| Table/model | Grain and relationships |
| --- | --- |
| `curated.accepted_trips` | One accepted `(dataset_id, trip_record_id)` with original JSON and reason/warning arrays |
| `curated.quarantined_trips` | One rejected key; key set disjoint from accepted |
| `control.source_artifacts` | One active dataset/artifact receipt, source partition, counts and signed charge |
| `control.ingestion_audit` | One committed ingestion attempt, inserted dispositions and replay keys |
| `curated.zones` / `dim_zone` | One lookup zone key, including 0; pickup/dropoff are two references to this dimension |
| `stg_trips` | View of all accepted curated records |
| `dim_date` | One actual accepted pickup date, with calendar year/month/day |
| `fct_trip` | One stable accepted trip key in its isolated scope database; artifact/month retained |
| `mart_zone_hour` | Dataset/artifact/source partition + actual pickup date/hour + pickup zone |
| `mart_daily_reconciliation` | Dataset/artifact/source partition + actual date bucket, including `MISSING_OR_INVALID_PICKUP` |

`dbt` means data build tool. A model is a SQL SELECT that dbt turns into a view or table.
`source()` identifies curated inputs; `ref()` identifies other models and creates dependencies.
A target selects the adapter/profile. Local commands explicitly use `--target local` and
the tracked profile contains no Snowflake target or credentials.

`fct_trip` uses the supported `delete+insert` incremental strategy with a stable unique key
and selects unseen keys. It does not use a maximum event timestamp. February and March
fixtures can load first and an older January fixture can still be added. Real, fixture and
sample scopes never share a fact database. The small marts rebuild from the complete fact,
preserving historical group totals. Partition and artifact keys avoid hiding late or
out-of-partition rows. The missing-date bucket keeps quarantined rows in reconciliation.
See [dbt's incremental model documentation](https://docs.getdbt.com/docs/build/incremental-models).

dbt tests check dimensional and trip uniqueness/non-null fields, zone/date relationships,
disposition disjointness, artifact coverage, domains, mart grain, counts and charge equality.
`dbt docs generate` writes a manifest/catalog locally for model documentation; the workflow
stores these under that run's `reports/local/.../dbt_target`. Adapter-specific array length
uses `len` for DuckDB and `array_size` for Snowflake. The warehouse templates preserve
NUMBER, ARRAY, TIMESTAMP_NTZ and VARIANT types; warehouse compilation/execution remains manual.

## 8. Reconciliation and orchestration

For each active artifact the gates require:

```text
source = accepted + quarantined
all disposition keys are unique and accepted ∩ quarantine is empty
accepted count = fact count = sum(mart trip_count)
accepted charge = fact charge = mart charge = daily charge
independent eligible raw source charge = accepted charge, at zero cent difference
```

The independent reference re-reads only required original fields, using its own eligibility
and Decimal calculation instead of calling the classifier. This can catch a shared data-flow
mistake that comparing only two downstream tables would miss. It is not an independently
audited business rule: it implements the same written contract. Source hashes are checked
before and after this scan. Money/count comparisons are per artifact, so a positive error
in one month cannot cancel a negative error in another.

`workflow.run_workflow()` enforces prepare → classify → ingest → models → verify → snapshot.
Subprocess errors/timeouts produce a failing CLI exit code. Workflow JSON/JSONL records
versions, actual command, elapsed time, stage outcomes and errors. Rerunning starts a new
attempt and reuses verified work. SQL models can be rebuilt after an interrupted model stage;
a successful curated commit is not undone by a later dbt failure. The previously published
dashboard snapshot remains usable until the new complete candidate validates.

The prepared Airflow 3.3.2 DAG runs the same CLI stages, one active run/task at a time,
manual schedule and paused creation. Its XCom values are IDs and paths, not row data.
Prepare uses the downloader's transient retries; permanent contract errors are not repeatedly
retried by Airflow. Other stages allow one retry. Docker is unavailable, so imports/DAG
execution/image/Compose health are pending; Python orchestration is the demonstrated path.
No Snowflake operator or task group is enabled.

## 9. Snapshots and dashboard

The export contains `zone_hour.parquet`, `daily_reconciliation.csv`, `run_summary.json`
and a manifest. The manifest stores each file's SHA-256, bytes, relative path, origin,
scope, export time, contract and metric versions. The loader checks path containment,
bytes/checksum, schema/types, nulls, counts, grain, artifact coverage and money equality.
It uses no network, credentials or database. Missing/corrupt data gives an explicit error.
Checksums detect alteration; they are not cryptographic signatures proving who exported data.

The app's default bundle is `demo/published`; internal real exports live in `demo/snapshots`.
Fixture/sample bundles have their own directories. Operations filters dates/zones and shows
trip counts, signed charge, demand and weighted distance/duration. Reliability shows historical
per-artifact gates, warnings/reasons and actual committed replay plus isolated recovery evidence.
Origin engine, scope, source months, actual pickup coverage, units and export time remain visible.
The UI never invokes a download/pipeline/warehouse and has no cloud fallback.

Average duration = sum(seconds) / sum(duration_count) / 60. With one trip lasting 10 minutes
and nine trips lasting 1 minute, the average is `(10+9)/10=1.9` minutes, not `(10+1)/2=5.5`.
The same additive approach applies to distance after filters. Empty matches show zero *actual
filtered count* and unavailable averages, not invented source data. Snapshot exports and
the repository demo remain accessible after a future trial expires; hosting is optional and
has not been deployed. Local dashboards are sufficient for demonstration.

## 10. Four concrete journeys

**Accepted fixture row 1:** the original artifact hash and position 1 produce a stable key.
Required values parse, the known pickup/dropoff zones map to dimensions, and reasons are empty.
The classifier writes it to accepted Parquet. DuckDB inserts it once, dbt builds its fact and
date/zone references, and its measures enter the date/hour/zone mart. Its original fields
remain inspectable in curated JSON. Fixture row 2 has the same payload but a different key.

**Quarantined fixture row 5:** the pickup column exists but this row's pickup is null.
`MISSING_PICKUP` goes into its reason array. It appears once in quarantine and not in the fact.
Daily reconciliation puts it in `MISSING_OR_INVALID_PICKUP`, so the twelve-row source remains
fully accounted for. Fixing it later requires a reviewed new artifact/policy, not editing
the preserved original source in place.

**Replay:** checksum and output checks allow reuse. Ingestion sees existing accepted and
quarantine keys, inserts zero rows, and commits an audit with replay count twelve for the
fixture. The incremental fact sees no unseen keys. Rebuilt marts preserve totals. An actual
real-source replay is recorded separately in completion evidence; do not confuse it with
the fixture exercise.

**Failure/retry:** in an isolated fixture database the test hook raises after accepted
insertion but before quarantine/receipt/audit commit. Transaction rollback leaves all four
target/audit counts zero. The failure event remains in attempt history. Retry without the
hook inserts 8 accepted/4 quarantined and one successful audit. This is observed DuckDB
behavior; the analogous Snowflake procedure/Stream exercise awaits the user's execution.

## 11. Routine PowerShell commands

Start in the actual project folder. Existing environments need no reinstallation or activation:

```powershell
Set-Location 'C:\Users\purav\OneDrive\Desktop\Data Projects\MobilityOps'
.\scripts\run_local.ps1 -Scope fixture -Offline
.\scripts\run_local.ps1 -Scope sample -Month '2026-01' -Offline
.\scripts\run_local.ps1 -Scope real -Month '2026-01' -Offline
.\scripts\run_local.ps1 -Scope real -Month '2026-02' -Offline
.\scripts\run_local.ps1 -Scope real -Month '2026-03' -Offline
```

Without `-Offline`, a real/sample run verifies official links and downloads/prepares source
if needed. Months are processed sequentially. The sample is deliberately a separate database.
To rerun models after a failure, use `-Stage models`, then `-Stage verify`, then `-Stage snapshot`.
Those stages require prepared sources/classification and fail if the prerequisites are absent.

```powershell
$env:PYTHONPATH = "$PWD\src"
.\.venv\Scripts\python.exe -m pytest -q
node scripts/lint_wasm.cjs
.\.venv\Scripts\python.exe -I -m pip check
.\.venv-dbt-local\Scripts\python.exe -I -m pip check
.\.venv\Scripts\python.exe scripts/verify_local_scenarios.py
.\.venv\Scripts\python.exe -m mobilityops.cli validate-snapshot --path demo/published
.\.venv\Scripts\python.exe -m streamlit run app/streamlit_app.py --server.address 127.0.0.1 --browser.gatherUsageStats false
```

Use `Ctrl+C` to stop Streamlit. To inspect the fixture UI instead:

```powershell
$env:MOBILITYOPS_SNAPSHOT_DIR = "$PWD\demo\fixture"
.\.venv\Scripts\python.exe -m streamlit run app/streamlit_app.py --server.address 127.0.0.1 --browser.gatherUsageStats false
Remove-Item Env:MOBILITYOPS_SNAPSHOT_DIR
```

Activation is optional: `.\.venv\Scripts\Activate.ps1`, or the equivalent path in
`.venv-dbt-local` / `.venv-dbt-snowflake`; use `deactivate` before switching. The Snowflake
environment's installation does not authorize any connection. Use its manual guide only.

## 12. Evidence, limits and tradeoffs

`reports/evidence/completion.json` is the final audit; its supporting workflow, dbt and
per-artifact reports are retained as small evidence. Tests are offline and do not download
full TLC months. The preserved fixture expects 8/4 and $134.00; observed outcomes are separate
from the old expectation file's historical `expected_only` flags.
The requirement-level review is in `docs/completion_audit.md`: 65 offline tests, zero Ruff
diagnostics, six models/24 dbt data tests per scope, five consistent isolated environments,
and 11 additional dashboard checks in the clean application environment all passed.

Current measured source results and version/check gates are in `docs/progress.md`. Timings are
single observed local runs, not throughput benchmarks or savings. Streamlit startup/rendering
and screenshots show actual local behavior, not a deployed public site. CI is prepared but
cannot be called a successful GitHub run until uploaded and executed by the user.

Native Ruff/dbt executables are blocked by this Windows Application Control policy. The local
dbt runner uses the installed Python API; lint uses the official Ruff 0.16.10 WASM Node build,
with a verified integrity receipt and the same configured rules. It is an experimental API
fallback, not a bypass or change to the security policy. CI uses normal native Ruff on Linux.
Clean isolated app/dbt environments were used to exercise install/run instructions while
preserving the user's environments. Python 3.12 syntax compatibility is checked; actual
local runtime verification is Python 3.14.7, and the Airflow Python 3.12 image remains pending.

Single-writer transactions and full small-mart rebuilds favor simple correctness at this
scale. No distributed workers, fuzzy trip matching, automatic source revision adoption,
streaming live feed, production service monitoring or cloud automatic retries are claimed.

## 13. Glossary and interview questions

| Term | Meaning here |
| --- | --- |
| Artifact | An unchanged physical file identified by its bytes |
| Manifest | Metadata receipt for that artifact |
| Provenance / lineage | Which file/position/parent produced a record |
| Contract | Required fields and agreed type/quality policies |
| Quarantine | One preserved rejected source record with distinct blocking reasons |
| Idempotency | Rerunning the same source produces no extra logical records |
| Transaction | A group of database writes that all commit or all roll back |
| Incremental | Add only previously unseen accepted keys to the existing fact |
| Dimension / fact / mart | Lookup attributes / trip measurements / small aggregate summaries |
| DAG / XCom | Directed dependency graph / small Airflow task-to-task receipt |
| Snapshot | A fixed reviewed aggregate export for the offline dashboard |

**Why hash bytes rather than filenames?** Names and paths can change; identical bytes preserve
content identity. An altered file gets a different identity and needs revision review.

**Why not deduplicate equal rows?** Equal payloads can be distinct source records. The guarantee
is safe replay of a source row, not global matching of real-world trips.

**Why not load the whole file into pandas?** Batches keep memory bounded while preserving order.
SQL handles stored analytical tables; pandas handles only the small aggregate dashboard.

**Why separate reasons from warnings?** Missing essential measurements block fact creation;
unusual but interpretable trips remain visible instead of being silently dropped.

**Why not use float sums for charge?** Row-level Decimal rounding creates an explicit, testable
cent policy. Signed adjustments and half-cent ties cannot be hidden in binary arithmetic.

**Why is the fact incremental but the marts rebuilt?** Stable-key unseen inserts accept older
arrivals. Full aggregate rebuilds include all historical trips, avoiding partial group totals.

**What does a green reconciliation prove?** The preserved source is accounted for, the key sets
and model counts agree, and the independent written monetary policy matches. It does not prove
the provider's source is a perfect account of every real-world taxi journey.

**What have you demonstrated in Snowflake/Airflow?** Only prepared templates/DAGs and reviewed
syntax so far. Actual local DuckDB, dbt, replay/recovery and snapshot app results are verified.
The user must execute and save warehouse evidence before claiming Snowflake execution.

Your first warehouse step is the private context/privilege/budget inspection in
`docs/snowflake_implementation.md`, not loading a full month immediately.

