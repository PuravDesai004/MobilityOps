# Manual Snowflake implementation

**Execution status: UNVERIFIED for every warehouse step.** These are prepared templates,
reviewed against official documentation on 10 October 2026. Neither the agent, local
tests, dashboard, Airflow nor CI connected to Snowflake. You personally execute every
statement, upload, connection check and dbt Snowflake command. Save observed results in
`docs/evidence_template.md`; do not substitute a generated file for warehouse evidence.

Begin with the tiny fixture. The real months follow only after fixture, replay, recovery
and dbt gates pass. Use one promotion owner: manual calls while the Task is suspended,
or the Task while manual calls are stopped. No Airflow warehouse consumer is supplied.
Examples contain legitimate account identifiers, manifest values and private profile paths
that you must replace locally. Never send authentication material to the agent.

## Important differences from the verified local path

Snowflake compute is a warehouse; storage is a database/schema/table hierarchy. Role grants
control access. Stages hold files; COPY creates physical raw rows; an append-only Stream
tracks changes. Stream inspection by SELECT does not advance its offset, but a committed
DML consumption does. A rollback leaves the offset retryable. These are documented
behaviors awaiting this project's account tests. See [Streams](https://docs.snowflake.com/en/user-guide/streams-intro).

Standard Snowflake table unique/primary keys do not enforce deduplication. This procedure
deduplicates input and uses stable-key insert-only MERGE, with explicit overlap/provenance
checks and serialized promotion. Local DuckDB enforces indexes; it has no Stream.

The SQL transaction has pre-created work tables, DML consumption, both dispositions,
source receipts and committed audit in one boundary. No DDL occurs inside it. An exception
rolls back and rethrows. Snowflake DDL can commit a transaction, so bootstrap and function/
procedure creation happen separately. See [transaction rules](https://docs.snowflake.com/en/sql-reference/transactions).

The future Python UDF imports the **same** `policy.py` and `identity.py` as the local
classifier. SQL only casts its validated canonical outputs. The packaged syntax supports
Python 3.12; Snowflake return Decimals are deliberately strings before NUMBER casts.
VARIANT conversion can stringify embedded timestamps/numbers; inspect the fixture's actual
representation first. No epoch unit guessing is allowed. See [Python type mappings](https://docs.snowflake.com/en/en/developer-guide/udf-stored-procedure-data-type-mapping).

dbt's local array length is `len`; Snowflake uses `array_size`. Both use stable-key
`delete+insert`, CAST/EXTRACT and ref/source dependencies. Tables use NUMBER, ARRAY,
TIMESTAMP_NTZ and VARIANT rather than DuckDB list/decimal types. Adapter execution and
compiled SQL must be checked by you; local success is not cloud compilation evidence.

Object names are also listed in `config/snowflake.example.yaml`, a manual reference that
no runtime component reads. Local `.env.example` contains snapshot/interpreter examples
only; authentication belongs in your separate ignored private profile.

### COPY expression and payload comparison review

The supplied COPY transformations use source `$1`/CSV positions, casts, metadata columns,
literal strings and session variables. [COPY transformations](https://docs.snowflake.com/en/user-guide/data-load-transform)
document casts and column projection; [SQL variables](https://docs.snowflake.com/en/sql-reference/session-variables)
allow `$name` wherever literal constants are allowed. UUID_STRING is evaluated by SET before
COPY. The transformations contain no JOIN, aggregate, WHERE, ORDER BY or Python UDF, and
do not combine MATCH_BY_COLUMN_NAME or VALIDATION_MODE with a transformation. These are
documentation-reviewed constructs; account compilation and actual load behavior remain UNVERIFIED.

Duplicate conflict checks count distinct VARIANT payloads and compare parsed JSON values,
not serialized JSON string order. Snowflake documents that [TO_JSON](https://docs.snowflake.com/en/sql-reference/functions/to_json)
does not preserve predictable object-key order; [COUNT](https://docs.snowflake.com/en/sql-reference/functions/count)
supports distinct expressions and VARIANT values. Original values remain preserved. During
your fixture review also run `SELECT PARSE_JSON('{"a":1,"b":2}') =
PARSE_JSON('{"b":2,"a":1}') AS same_payload;` and expect TRUE; this account check is UNVERIFIED.

## Ordered steps

### 1. Inspect account context, privileges and budget

**Purpose/tool:** privately inspect Snowsight context and trial/billing balance before using
compute. **Prerequisite:** your own access and an approved spending budget.
**File:** `sql/snowflake/00_inspect_account.sql`; replace `<YOUR_USER_IDENTIFIER>` with your
SQL user identifier (quote identifiers appropriately if case-sensitive).

**Expected shape/verification:** one context row with account/user/role/warehouse/database;
grant and warehouse listings; metering rows if your monitoring role can read ACCOUNT_USAGE.
Use the file's `CURRENT_*`, `SHOW GRANTS`, and metering query; save non-secret results.
**Troubleshooting:** a denied usage query means missing monitoring privilege, not zero usage.
Trial dollars are not warehouse credits; a proposed $45 cap must be checked against your
actual credit price/balance. Resource-monitor limits are credits, not a dollar conversion.

### 2. Configure roles and XS warehouse

**Purpose/tool:** administrative Snowsight worksheet for narrowly scoped ordinary work.
**Prerequisite:** appropriate SECURITYADMIN/SYSADMIN/ACCOUNTADMIN privileges, reviewed existing
names and your own user placeholder. **File:** `01_bootstrap.sql` in `sql/snowflake`.

It creates `MOBILITYOPS_ENGINEER`, `MOBILITYOPS_WH` (XSMALL, auto-suspend 60 seconds,
statement timeout 600 seconds, initially suspended), and grants warehouse usage/operation
plus account EXECUTE TASK. Database/schema creation uses SYSADMIN; ordinary objects are
owned by the engineer role. The learning role has broad rights within this project's isolated
schemas, not access to unrelated databases.

**Verify:** `SHOW WAREHOUSES LIKE 'MOBILITYOPS_WH'; SHOW GRANTS TO ROLE MOBILITYOPS_ENGINEER;`
Expect role/warehouse properties and grants, not a success count of trips.
**Troubleshooting:** `IF NOT EXISTS` preserves existing settings; inspect them and deliberately
ALTER if they differ. Do not assume an existing warehouse is newly constrained by CREATE.

### 3. Create isolated scopes

**Purpose/tool:** same bootstrap worksheet. **Prerequisite:** step 2.
**File:** remaining database/schema statements in `01_bootstrap.sql`.
Scopes are `MOBILITYOPS_FIXTURE`, `MOBILITYOPS_FIXTURE_RECOVERY`, and `MOBILITYOPS_REAL`,
each with RAW, CONTROL, CURATED, WORK and ANALYTICS. Recovery starts empty and never alters
the real database.

**Verify:** `SHOW DATABASES LIKE 'MOBILITYOPS%'; SHOW SCHEMAS IN DATABASE MOBILITYOPS_FIXTURE;`
Expected three named scopes and their schemas. **Troubleshooting:** before every file, inspect
`SELECT CURRENT_ROLE(),CURRENT_DATABASE(),CURRENT_WAREHOUSE();`. Context is explicit in the
generated files; do not accidentally load real data into fixture scope.

### 4. Create raw, control, curated and work tables

**Purpose/tool:** engineer-role SQL worksheet. **Prerequisite:** empty fixture scope.
**File:** `02_tables.sql`.

RAW.TRIPS retains payload VARIANT, artifact, physical position, dataset/month, attempt and
stage filename. CONTROL tables record active artifacts, COPY receipts and committed promotion.
WORK.INCOMING is pre-created; CURATED has the typed policy output pair plus zone lookup.
**Verify:** `SHOW TABLES IN DATABASE MOBILITYOPS_FIXTURE; SELECT COUNT(*) FROM RAW.TRIPS;`
Expect the named tables and zero raw rows initially. **Troubleshooting:** CREATE IF NOT EXISTS
does not migrate an old schema. Inspect `DESCRIBE TABLE`; use a deliberately new isolated scope
for incompatible earlier work rather than silently replacing existing tables.

### 5. Create internal stages and file formats

**Purpose/tool:** same worksheet. **Prerequisite:** step 4.
**File:** `03_stage_and_formats.sql`.

RAW.SOURCE_STAGE holds original files; CONTROL.CODE_STAGE holds the pure-policy ZIP.
Parquet uses logical types and the non-vectorized scanner; CSV skips its header and handles
quoted fields. **Verify:** `SHOW STAGES; SHOW FILE FORMATS; DESCRIBE FILE FORMAT RAW.PARQUET_FORMAT;`
Expect the stages and configured formats. **Troubleshooting:** no integration/cloud bucket
is required; missing CREATE STAGE/format permission indicates wrong role/schema grants.
Scanner/logical type behavior is documented in [COPY](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table).

### 6. Create the Stream before the first raw load

**Purpose/tool:** same worksheet. **Prerequisite:** RAW.TRIPS is empty.
**File:** `04_stream_before_load.sql`.

**Verify:** `SHOW STREAMS LIKE 'TRIPS_STREAM' IN SCHEMA RAW; SELECT COUNT(*) FROM RAW.TRIPS_STREAM;`
Expect an append-only Stream and initially zero pending rows. Do not CREATE OR REPLACE an
active Stream on rerun. **Troubleshooting:** if raw rows already exist before Stream creation,
stop and use the isolated rebuild guidance in `operations_runbook.md`; a new Stream is not
proof that old pending data was recovered.

### 7. Upload unchanged fixture, zones and shared policy

**Purpose/tool:** a client on your laptop for PUT, or Snowsight's **Load files into a Stage**
upload UI. **Prerequisite:** steps 5–6, your privately configured connection if using a client.
**File:** `07a_upload_fixture.sql` (client-side PUT commands), followed by `05_shared_classifier.sql`
and `06_transactional_promotion.sql` in the fixture worksheet.

The exact immutable source paths are:

```text
C:/Users/purav/OneDrive/Desktop/Data Projects/MobilityOps/tests/fixtures/yellow_taxi_fixture.parquet
C:/Users/purav/OneDrive/Desktop/Data Projects/MobilityOps/tests/fixtures/taxi_zone_lookup_fixture.csv
C:/Users/purav/OneDrive/Desktop/Data Projects/MobilityOps/sql/snowflake/assets/mobilityops_policy.zip
```

With a privately configured Snowflake CLI connection called `mobilityops_manual`, you may
personally run `snow sql --connection mobilityops_manual --filename sql/snowflake/07a_upload_fixture.sql`.
Install/configure the client yourself through [official CLI guidance](https://docs.snowflake.com/en/en/developer-guide/snowflake-cli/connecting/configure-connections).
External-browser authentication requires account-supported SSO and a local browser; choose
your account's supported method privately if it is unavailable.

PUT reads the **client** filesystem. A browser SQL worksheet cannot upload a laptop path.
For the UI method select the correct stage and the same hash/zones prefixes; code goes to
CONTROL.CODE_STAGE root. The current UI limit is 250 MB/file; source artifacts here are below
that limit. See [stage upload UI](https://docs.snowflake.com/en/en/user-guide/data-load-local-file-system-stage-ui).

**Verify:** the upload file's LIST statements show the exact three filenames and prefixes;
`SHOW USER FUNCTIONS IN SCHEMA CONTROL; SHOW PROCEDURES IN SCHEMA CONTROL;` shows classifier
and promotion. **Troubleshooting:** preserve Parquet bytes and filenames, disable PUT automatic
compression, and use the correct stage. Re-encoding/splitting changes artifact identity and
physical position alignment. A Python import error calls for ZIP root/path inspection.

### 8. COPY with provenance and verify metadata alignment

**Purpose/tool:** fixture SQL worksheet. **Prerequisite:** stages, UDF/procedure and empty
artifact registration reviewed. **File:** `07_fixture_load.sql`, which intentionally stops
before promotion.

It records the actual local SHA, dataset `fixture`, partition, expected 12 rows and zone hash.
Zones load independently and include unknown member 0. COPY uses `$1` plus
`METADATA$FILE_ROW_NUMBER`/`METADATA$FILENAME`, one explicit file and ON_ERROR=ABORT_STATEMENT.
The COPY query ID/results are saved outside the subsequent promotion transaction.

**Verify:** file queries must show raw physical/distinct counts 12/12 and positions 1–12;
Stream pending count 12. Inspect every fixture payload in physical order: row 5 null pickup,
row 8 null total, duplicate payload at rows 1/2. Inspect TYPEOF/timestamp values and compare
the known fixture dates. UDF-call one ordinary payload and check its canonical timestamps
before continuing. **Troubleshooting:** raw parsing errors are file failures. Do not use
CONTINUE to silently discard records. Unexpected numeric timestamps require format/type
investigation, not guessing seconds/milliseconds; stop before promotion. See
[metadata columns](https://docs.snowflake.com/en/user-guide/querying-metadata).

### 9. Execute transactional promotion

**Purpose/tool:** manual fixture worksheet; Task remains suspended.
**Prerequisite:** complete step 8 alignment. **File/command:** review
`06_transactional_promotion.sql`, then personally execute `CALL CONTROL.PROMOTE(FALSE);`.

**Expected shape:** returned VARIANT with COMMITTED, batch ID and physical incoming rows.
The procedure permits one reviewed artifact per call, rejects inconsistent duplicates,
checks policy/zone version, inserts Stream rows into work through DML, deduplicates source
positions, MERGEs both dispositions, records audit, verifies coverage and commits.
**Verify:** `SELECT * FROM CONTROL.INGESTION_AUDIT; SELECT COUNT(*) FROM RAW.TRIPS_STREAM;`
Expected first audit 8/4 inserts, zero replay and zero pending rows. **Troubleshooting:** an
error rolls back/rethrows. Inspect query history, source registration and zones, keeping
pending Stream rows intact. Never label an exception as a committed promotion.

### 10. Verify fixture classification and reconciliation

**Purpose/tool:** worksheet. **Prerequisite:** successful promotion.
**Files:** `08_verify.sql`, `08a_policy_vectors.sql`, preserved
`tests/expected/yellow_taxi_fixture_v1.json`, and `assets/classification_vectors.json`.

**Verify:** expected accepted 8, quarantined 4, signed accepted charge 134.00, 12 unique keys,
zero overlap/count differences. Accepted physical rows are 1,2,3,4,9,10,11,12; rejected
rows 5,6,7,8 have missing pickup / backward time / negative distance / missing amount.
Policy-vector SQL returns **zero mismatches**, testing rounding, overflow, nonfinite values,
multiple reasons, tiny negative distance and official Unknown members.
**Troubleshooting:** save the actual mismatch row and inspect original payload/types; do not
modify reviewed expectations to make the warehouse agree. Reason totals are not row totals.
Local vector/package checks prove generated assets agree locally, not this warehouse result.

### 11. Demonstrate forced fixture replay

**Purpose/tool:** fixture worksheet only. **Prerequisite:** first fixture passed and Task suspended.
**File:** `09_fixture_replay_failure.sql`, selecting its FORCE COPY statements first.

**Expected/verify:** raw becomes 24 physical rows but still 12 stable source identities.
After the failure/retry portion (or an ordinary false promotion), curated remains 8/4,
charge 134.00; the committed audit reports zero inserts and 12 replay keys. Execute the
error-producing call separately so the client cannot obscure which statements ran.
**Troubleshooting:** ordinary COPY may skip a previously loaded file due to load history;
FORCE is deliberate **only** in this tiny fixture exercise. Do not use the real database to
manufacture a replay demonstration.

### 12. Prove fresh fixture rollback and retry

**Purpose/tool:** fresh `MOBILITYOPS_FIXTURE_RECOVERY` worksheet.
**Prerequisite:** main fixture passed; recovery raw/curated/audit empty.
**Files:** `recovery/02_tables.sql` through `06_transactional_promotion.sql`, upload the same
three files to this database's stages, then `recovery/07_load_before_failure.sql` and
`recovery/08_fail_then_retry.sql`.

**Verify:** select/run `CALL CONTROL.PROMOTE(TRUE)` alone; it must raise after an accepted
write. Next, targets/audit/work are 0/0/0/0 and Stream still has 12 pending rows. Retry false
then gives 8/4, charge 134.00, one committed audit and no pending rows. This fresh case
proves new accepted inserts roll back; the existing-data replay case proves offset/replay
recovery. **Troubleshooting:** never rerun fresh-scope setup over existing data and expect zero.
Use a new reviewed fixture scope or inspect what already ran. No real-data wipe is needed.

### 13. Create, manually execute and suspend a warehouse-backed Task

**Purpose/tool:** main fixture worksheet, engineer role.
**Prerequisite:** procedure/replay/recovery passed, account EXECUTE TASK and warehouse rights.
**File:** `10_task.sql`; creates the Task and explicitly suspends it.

To observe real work, deliberately FORCE-reload the small fixture once while the Task is
suspended, then personally select/run the commented EXECUTE TASK statement. Do not manually
call promotion at the same time. Inspect TASK_HISTORY until that execution terminates;
save query ID/state/errors and resulting audit. Suspend afterward and verify SHOW TASKS state.
**Expected:** warehouse `MOBILITYOPS_WH`, no overlapping execution, successful replay-only audit,
Stream drained. **Troubleshooting:** no pending rows can cause the WHEN condition to skip;
that is not demonstrated processing. Task-owner grants differ from your active worksheet role.
See [CREATE TASK](https://docs.snowflake.com/en/sql-reference/sql/create-task).

### 14. Configure private dbt profile and manually run checks/build

**Purpose/tool:** your PowerShell terminal, preserved `.venv-dbt-snowflake`.
**Prerequisite:** fixture curated gates passed; authentication configured privately.
**File:** copy `dbt/profiles.example.yml` into an ignored private folder such as
`.private/snowflake/profiles.yml`. Set account/user environment values privately; never paste
them into chat or commit profiles. The default example is local but every command below
explicitly selects snowflake. Change database to MOBILITYOPS_REAL only during step 15.

```powershell
# USER-RUN ONLY: all four commands connect to Snowflake.
.\.venv-dbt-snowflake\Scripts\python.exe scripts/dbt_user_snowflake.py debug --project-dir dbt --profiles-dir .private/snowflake --target snowflake
.\.venv-dbt-snowflake\Scripts\python.exe scripts/dbt_user_snowflake.py compile --project-dir dbt --profiles-dir .private/snowflake --target snowflake
# Inspect dbt/target/compiled/mobilityops before proceeding.
.\.venv-dbt-snowflake\Scripts\python.exe scripts/dbt_user_snowflake.py build --project-dir dbt --profiles-dir .private/snowflake --target snowflake
.\.venv-dbt-snowflake\Scripts\python.exe scripts/dbt_user_snowflake.py docs generate --project-dir dbt --profiles-dir .private/snowflake --target snowflake
```

**Verify:** connection checks and every model/test must succeed; retain actual run_results and
catalog, then query fact/mart counts and charge. **Troubleshooting:** private account/SSO issues
are yours to resolve with account guidance; missing database grants are not a package issue.
`scripts/dbt_local.py` refuses a cloud target and must not be repurposed. Native dbt.exe is
blocked on this laptop, hence the explicit user-only Python API wrapper. See
[dbt Snowflake profile guidance](https://docs.getdbt.com/docs/local/connect-data-platform/snowflake-setup).

### 15. Load real months sequentially and verify incremental/backfill

**Purpose/tool:** REAL worksheet, client uploads and private dbt target.
**Prerequisite:** complete fixture gates, supervised budget and timeout review.
**Files:** `real/02_tables.sql` through `06_transactional_promotion.sql`,
`12a_real_zones.sql`, a reviewed copy of `12_real_month.sql` for each artifact.

Upload the unchanged official zone lookup into REAL's stage/zones prefix and verify 266
members including key 0. Upload each month under its manifest artifact hash. Replace dataset,
partition, count, SHA, filename and stage prefix together from local manifests. The template
uses February only as an example. Process Feb → Mar → Jan to demonstrate older-month arrival;
COPY/promote/verify/dbt build and save totals before loading the next. Expected added accepted
keys come from each local verification report. Use a normal no-new-Stream promotion/repeated
dbt build to show zero added facts; real FORCE loads are unnecessary.

**Verify:** run the queries from step 10 with REAL context (do not execute a file that switches
back to fixture); per-artifact counts, unique/disjoint keys, fact/mart counts and signed
charge must match saved local reports. **Troubleshooting:** source revisions require explicit
fresh-scope review; do not register two active checksums for one month. XS/UDF performance and
cost are unmeasured: supervise the first real run, honor timeout, and stop on a failed gate.

### 16. Export snapshots and compare relevant results offline

**Purpose/tool:** worksheet/export UI then local Python, without a new cloud connection.
**Prerequisite:** successful cloud dbt build/tests and fixture/real verification.
**File:** `13_export.sql`, in a deliberate copy with the appropriate database context.

Export zone/hour and daily marts, source/control audits and the exact `artifact_metrics.csv`
query with headers enabled. Keep exports in `.private/snowflake/exports`. Export time, origin
`snowflake_user_export`, query IDs, actual scope and policy/version receipts must remain attached.
Retain fixture source row/key/reason comparison outputs as well. This project's dashboard
defaults to validated **local** snapshots; raw cloud CSVs must not replace that bundle or be
relabelled as verified local data.

```powershell
# OFFLINE: compares a user-supplied fixture CSV, never connects.
$env:PYTHONPATH = "$PWD\src"
.\.venv\Scripts\python.exe scripts/compare_manual_exports.py --export .private/snowflake/exports/artifact_metrics.csv --local-report reports/local/fixture/fixture/verification.json --output .private/snowflake/export_comparison.json
```

For real scope add one `--local-report reports/local/real/<month>/verification.json` per
exported month. **Expected/verify:** PASSED and zero differences for all counts/charges,
matching dataset/artifact identities and complete coverage. Compare date/hour/zone mart groups
and sum numerators/denominators as well using saved query outputs. The comparator verifies the
CSV's contents against local evidence; it does not authenticate its origin or prove you ran
every warehouse gate. **Troubleshooting:** preserve NUMBER charge strings, exact headers and
artifact identities; don't round exports via a spreadsheet.

### 17. Record evidence, inspect usage and suspend resources

**Purpose/tool:** your worksheet/monitoring UI. **Prerequisite:** all actual results saved.
**Files:** `14_suspend_and_inspect.sql`, `docs/evidence_template.md`.

**Verify:** suspend any fixture/real Tasks, inspect Task history, Stream STALE_AFTER, warehouse
metering/balance and warehouse state. Expect SUSPENDED Tasks/warehouse after shutdown; save
actual cost/credit output with its time range and any ACCOUNT_USAGE latency. Suspension does
not halt Stream retention or eliminate stored-data charges.
**Troubleshooting:** a suspended Task is not a recovered stale Stream. Keep immutable files/
manifests and follow the isolated rebuild runbook. Do not use broad DROP statements to tidy
up unrelated account objects. Upgrade resume wording only after you have execution evidence.

## References and evidence boundary

Technical review covered the official [PUT](https://docs.snowflake.com/en/sql-reference/sql/put),
[COPY](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table),
[metadata](https://docs.snowflake.com/en/user-guide/querying-metadata),
[procedure syntax](https://docs.snowflake.com/en/sql-reference/sql/create-procedure),
[Python UDF creation](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-creating),
[Task alteration](https://docs.snowflake.com/en/sql-reference/sql/alter-task), and dbt adapter
documentation linked above. Documentation review reduces avoidable errors; actual account
privileges, compilation, COPY timestamp representation, Stream transaction behavior, Task
execution, cross-engine totals and cost remain **UNVERIFIED until you run them**.
