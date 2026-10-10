# MobilityOps completion audit

This records the original completed-build audit. The later SQL/business-story/dashboard
extension and actual remote CI observations are recorded in [progress](progress.md),
[business findings](business_findings.md) and `reports/evidence/presentation_verification.json`.
Original evidence and the original pre-publication statuses below remain historical.

Reviewed 10 October 2026 against the full-build request in the referenced pasted text and
the original architecture guide. The later request supersedes the old one-month calendar,
milestone stops, SQL-classifier design and mandatory cloud completion. The required outcome
is a verified local system plus prepared manual Snowflake and conditional Airflow deliverables.
At this initial audit, no Snowflake execution, hosted app deployment, GitHub push or message
was authorized or claimed. A subsequent user request authorized repository publication;
docs/progress.md records that later status. The original local audit evidence is preserved.

This is a requirement-level review of current source, tests, logs, reports, preserved bytes,
database results and rendered screenshots. `reports/evidence/completion.json` collects the
machine evidence; a status string in that file alone is not the proof of every requirement.

## Local implementation and verification

All paths below are relative to the project root. PASSED refers to observed local behavior.
PREPARED refers to inspected source/configuration that has not run in its future platform.

| Requirement | Inspected implementation / authoritative evidence | Result |
| --- | --- | --- |
| Inspect existing workspace/instructions before extending it | Actual config, original guide, README, progress, package/tests; no applicable AGENTS.md found | PASSED |
| Preserve existing environments, locks, fixture expectations and sources | Existing three venvs retained; lock SHA receipts in completion.json; fixture expected_only file remains; final actual byte hashes match all five manifests | PASSED |
| No global packages or paid infrastructure | Project-local venvs, clean checks and official Ruff WASM under ignored .tools; no warehouse/runtime installation | PASSED |
| One integrated package/CLI and correct-environment PowerShell command | cli.main → workflow.run_workflow; scripts/run_local.ps1; run_dbt explicitly target local and strips app PYTHONPATH | PASSED: full workflows saved |
| Real stages, dependency failures, useful logs/history and nonzero errors | workflow.STAGES prepare/classify/ingest/models/verify/snapshot; CLI error exit; per-stage dbt logs and JSONL attempts | PASSED: success and controlled failed attempts observed |
| Official URLs, streaming download, bounded timeout/retry | catalog.py/download.py/real_source.py; official Jan/Feb/Mar links reviewed; download tests mock interrupted/transient/404/invalid/reuse cases | PASSED: three real downloads plus offline tests |
| Validate temporary content before atomic publication; preserve prior success | download.py validators and replace; test_failed_replacement_preserves_existing_valid_artifact, invalid-content and interrupted-stream tests | PASSED |
| Required-column gate; optional/additional fields retained | contract.py + preserved YAML; tests remove required columns/add optional fields; raw payload tests preserve extra values | PASSED |
| Bounded Parquet profile and processing without full-month pandas | profile.py Arrow batches; classify.py 10,000-row batches; reference.py required-column batches; actual profile/null-count/schema reports | PASSED |
| Original byte checksum and physical-order one-based source identity | identity.sha256_file/trip_record_id; classify.iter_batches(use_threads=False); final actual five-artifact checksum/metadata recheck | PASSED |
| Run/attempt/path do not alter IDs; equal physical rows stay distinct | test_source_row_identity_depends_on_artifact_bytes_and_physical_row_not_context; fixture rows 1/2 produce distinct IDs; twelve total IDs | PASSED |
| Atomic manifests and append-only histories | manifest.py and tests for replace failure / later failed attempts preserving success; per-source distinct manifests | PASSED |
| Separate deterministic sample and lineage | sample.py evenly spaced selection; 10,000-row independent hash; parent positions stored; test_sample_rows_have_separate_identity_and_parent_lineage | PASSED |
| Fixture/sample/real separation | Separate classification/report/manifest/snapshot directories and DuckDB databases; snapshot scope/coverage gates | PASSED |
| Detect changed active month and require revision review | Downloader, classification pointer and storage receipt revision gates; changed-valid-month and provenance-drift tests; operations revision procedure | PASSED; automatic adoption deliberately absent |
| One Python classifier; no DuckDB SQL classifier or Stream emulation | policy.classify_row reused by classify.py and packaged future Python UDF; storage accepts already classified records | PASSED |
| Blocking reasons, distinct multi-reason quarantine, warnings retained | policy.py; fixture 8/4 exact; malformed, multiple-reason, warning-only and unknown-zone tests | PASSED |
| Signed Decimal precision/rounding/nonfinite/overflow handling | DECIMAL(18,2)/(18,3), ROUND_HALF_UP, pre/post bounds; twelve numeric boundary cases including extreme exponents | PASSED |
| Preserve original values/provenance/optional fields | Typed raw_payload_json tags; output schema and source IDs; sample parent fields; schema/extra-field tests | PASSED |
| File/provenance failures distinct from row quarantine | Required-column and ProvenanceError gates before reuse/publication; manifest drift and changed-source ingestion tests | PASSED |
| Failed classification does not expose partial output or destroy success | Immutable output generations + atomic pair pointer; controlled mid-batch failure leaves prior pointer/bytes intact | PASSED |
| Stable scoped DuckDB keys, no replay duplicates, disjoint dispositions | storage.ingest_classified checks IDs/counts/checksums/conflicts; unique indexes and absent-key inserts; observed fixture and real replay | PASSED |
| Accepted/quarantine/zone/receipt/success audit transaction; rollback/retry | test_local_transaction_rollback_retry_and_replay; local_scenarios.json rollback all four counts zero, retry 8/4/1/1, replay inserts zero | PASSED: DuckDB only |
| All six named dbt models with documented grains | stg_trips, dim_zone, dim_date, fct_trip, mart_zone_hour, mart_daily_reconciliation; schema.yml/data_dictionary/architecture | PASSED: six models and 24 tests per scope |
| Official zones plus unknown 0; preserve source IDs and valid Unknown labels | read_zones, curated.zones, dim_zone; real 265+1 members; official 264/265 classifier tests | PASSED |
| Actual accepted dates, including outside source month | dim_date from accepted timestamps; relationship tests and live database/snapshot coverage 2008-12-31→2026-04-01 | PASSED |
| Incremental stable unseen keys, older backfill, complete aggregate rebuilds | fct_trip NOT EXISTS stable key; marts full fact; isolated Feb→Mar→Jan scenario produces 2→4→6 facts then replay stays 6 | PASSED |
| Artifact/partition and missing-date reconciliation bucket | mart_daily_reconciliation SQL coalesces missing date; fixture missing pickup retained in source count; dbt coverage/money tests | PASSED |
| Actual dbt tests and generated documentation | Per-scope dbt_execution/run_results/catalog/manifest files; local target commands and successful 6 models/24 tests recorded | PASSED |
| Run fixture → sample → January → two consecutive official months sequentially | Workflow history and original reports; three verified hashes/monthly inserts; detailed portable fixture/sample/real reports | PASSED |
| New monthly accepted keys add exactly expected count | real_incremental.json: 3,724,888 then 3,399,864 then 3,952,449; fact 3,724,888→7,124,752→11,077,201 | PASSED |
| Actual completed-artifact replay adds zero logical records | Last real January replay: 0/0 inserts, 3,724,889 replay keys, all before/after curated/fact counts unchanged; observed 205.092 seconds | PASSED |
| Isolate destructive/failure demonstrations; preserve completed real data | verify_local_scenarios unique .checks/scenarios scopes; no real wipe; real replay preserves counts | PASSED |
| Per-artifact source=A+Q, unique/disjoint, A=fact=mart counts | reconcile.py queries and saved verification differences all zero across fixture/sample/three real months | PASSED |
| Accepted/fact/mart/daily charge matches independent eligible source | reference.py independently parses original four fields; reconcile.py pre/post source checksums; all Decimal differences 0.00 | PASSED |
| Reason occurrences can exceed rejected rows; honest metric meaning | Distinct-row count vs Counter reasons; docs and app label signed recorded passenger charge, units and cash-tip limitation | PASSED |
| Machine evidence includes identities/versions/commands/outcomes/counts/differences/timings | completion.json plus five portable detailed reports, local_scenarios.json, real_incremental.json and dashboard_smoke.json | PASSED |
| Complete local workflow without Docker; same tested stage functions | Actual Windows Python-orchestrated fixture/sample/Jan/Feb/Mar + real replay; no Docker dependency | PASSED |
| Operations and Reliability dashboard with filters, demand, weighted measures and historical recovery | streamlit_app.py + AppTest date/zone/filter/error checks; actual startup/render and three inspected screenshots | PASSED |
| Small validated snapshots only; checksums/schema/grains/coverage/units visible | snapshot.py candidate validation/atomic pointer, strict loader; published data files 4,680,078 bytes with checksummed manifest | PASSED |
| No credential requirement/cloud fallback/network loading/visitor-triggered pipeline | App imports only snapshot reader/functions; blocked socket/requests loader test; clean app 11 tests passed | PASSED |
| Missing/corrupt/semantically inconsistent snapshots fail clearly | AppTest error view; loader corruption/updated-checksum invalid type/count/money/grain/partition tests | PASSED |
| Full existing/new tests, lint and dependency consistency | Latest pytest 65 passed in 8.18s; official Ruff WASM 0.16.10 zero diagnostics; all five python -I pip checks passed | PASSED |
| Clean reproducible install/run without replacing user environments | Exact-lock .checks/clean-app/.checks/clean-dbt installs, full fixture log SUCCEEDED; extra 11 dashboard checks in 4.38s | PASSED |
| GitHub-ready exclusions and portable demo/evidence | Actual git candidate inspection; raw/full data, classified data, private profiles/logs, DBs, .tools/.checks and venvs excluded; only manifest-referenced public generation allowed | PASSED locally; no staging/push |
| CI fixture/lint/local dbt/snapshot without real downloads/cloud secrets | .github/workflows/offline-checks.yml inspected; local equivalent gates observed | PREPARED; GitHub execution not claimed |
| Exact routine commands, walkthrough, public-hosting guidance and resume wording | README, architecture, operations, demo_walkthrough and resume_and_handoff; local launch remains usable after warehouse trial | COMPLETE documentation; no public deployment |

## Conditional orchestration deliverable

`airflow/Dockerfile` pins official `apache/airflow:3.3.2-python3.12`; its matching official
constraints were reviewed. Application/local dbt dependencies are separately locked venvs
inside the prepared image, rather than mixed into Airflow packages. The DAG uses Airflow 3
SDK APIs, paused manual scheduling, one active run/task, bounded subprocess timeouts/retries
and receipt paths/IDs in XCom. It calls the same local CLI stages and has no Snowflake path.

Docker and Compose remain unavailable in the actual environment. The task explicitly permits
Python local completion with Airflow runtime pending in this case. Image build/pull, Airflow
imports, DAG execution and Compose health are **PENDING**, not passed from static inspection.

## Manual Snowflake preparation: all execution UNVERIFIED

The required preparation is complete; account actions remain exclusively the user's future
work. Every SQL file labels execution UNVERIFIED. The 17-step guide includes purpose/tool,
prerequisites, exact files/commands, expected shapes, verification and troubleshooting.

| Required ordered stage | Inspected template/instructions | Execution status |
| --- | --- | --- |
| 1 Context, grants and budget | 00_inspect_account.sql, guide step 1 | UNVERIFIED |
| 2 Dedicated role/XS/suspension/timeouts | 01_bootstrap.sql, step 2 | UNVERIFIED |
| 3 Isolated fixture/real/recovery | Bootstrap schemas/databases, step 3 | UNVERIFIED |
| 4 Raw/control/curated/work tables | 02_tables.sql plus real/recovery copies, step 4 | UNVERIFIED |
| 5 Internal stages and Parquet/CSV formats | 03_stage_and_formats.sql, step 5 | UNVERIFIED |
| 6 Append-only Stream before raw COPY | 04_stream_before_load.sql without replacement, step 6 | UNVERIFIED |
| 7 Unchanged client-side fixture/zone/policy upload | 07a_upload_fixture.sql with quoted Windows file URIs; stage UI alternative, step 7 | UNVERIFIED |
| 8 COPY provenance/explicit file/results/metadata and type inspection | 07_fixture_load.sql, step 8; no automatic promotion call | UNVERIFIED |
| 9 Explicit transactional promotion/shared classifier | 05_shared_classifier.sql/06_transactional_promotion.sql, step 9 | UNVERIFIED |
| 10 Reviewed fixture and 19 policy vectors | 08_verify.sql/08a_policy_vectors.sql/assets, step 10 | UNVERIFIED |
| 11 Forced tiny fixture physical replay, logical 8/4 | 09_fixture_replay_failure.sql, step 11 | UNVERIFIED |
| 12 Fresh fixture failure/rollback/retry | recovery/02…08 templates, step 12 | UNVERIFIED |
| 13 Warehouse-backed Task/manual real-work run/suspension | 10_task.sql with manual commented trigger, step 13 | UNVERIFIED |
| 14 Private local dbt profile/user checks/build/docs | profiles.example.yml, dbt_user_snowflake.py commands, step 14 | UNVERIFIED; agent never invoked wrapper |
| 15 Sequential real-month loading/incremental/backfill | real/ templates, 12a_real_zones.sql/12_real_month.sql, step 15 | UNVERIFIED |
| 16 User exports and offline local comparison | 13_export.sql/compare_manual_exports.py, step 16 | UNVERIFIED origin; comparator tested with explicitly synthetic CSV |
| 17 Save evidence, inspect usage and suspend | evidence_template.md/14_suspend_and_inspect.sql, step 17 | UNVERIFIED |

Procedure review confirms pre-created work, INSERT-from-Stream DML, stable-key deduplication,
conflict guards, both disposition MERGEs, receipt/audit/work updates within explicit transaction,
no DDL in that transaction, rollback/rethrow, fixture-only failure gate and one consumer owner.
Shared-policy ZIP bytes and 19 JSON-native expected vectors pass local checks. Future account
compilation, privilege behavior, timestamp representation, file-row alignment, Stream offsets,
Task execution, adapter SQL, parity and costs remain **UNVERIFIED**.

Official review sources and differences are documented in the manual guide, including COPY
supported expressions/session constants, unordered VARIANT/JSON, Python mappings, MERGE,
transactions, Streams, Tasks, PUT and adapter behavior. Documentation review is preparation,
not a successful warehouse execution test.

## Named artifact map and scope amendments

The implementation keeps one package entry point and shared functions. Equivalent filenames
replace obsolete guide examples rather than adding duplicate logic:

| Original guide responsibility | Actual file(s) |
| --- | --- |
| local_pipeline / local raw-classify-promote SQL | workflow.py + classify.py/policy.py + storage.py; a second SQL classifier is explicitly prohibited by the later request |
| fixture/sample helper scripts | cli generate-fixture / prepare-real-source and fixture.py/sample.py functions |
| app snapshot_reader | shared src/mobilityops/snapshot.py, used directly by the app |
| docs/architecture | requested root architecture.md |
| manual_snowflake_runbook | requested docs/snowflake_implementation.md |
| old numbered Snowflake filenames | Complete ordered sql/snowflake files listed by the 17-stage guide, with separate recovery/real contexts |
| manual handoff/receipt responsibility | 17-stage manual guide, blank evidence_template, user-only wrapper and offline export comparator; no automatic account path |
| identity/contract/replay/recovery/snapshot tests | test_profile_identity, test_download, test_classification, test_storage and test_snapshot_dashboard, plus scenario scripts |
| non-secret examples | .env.example, config/snowflake.example.yaml, dbt/profiles.example.yml |
| dependency inputs/locks | requirements/core, app, dev and separate dbt adapters; original exact locks preserved |

The completed root architecture contains the business problem, Mermaid implementation,
technology responsibilities, function/file map, grains/keys/relationships, identities/manifests/
lineage/revisions, rule/type/decimal/timestamp policy, transactional local ingestion, dbt,
failure/resume flow, snapshots/weighted metrics, measured evidence/tradeoffs, exact PowerShell,
accepted/quarantine/replay/retry journeys, glossary and interview questions.

The README/progress, data dictionary, operations runbook, learning log, demonstration/hosting
walkthrough, resume/handoff, blank warehouse evidence template and this audit are present.
The final UTF-8 Markdown link check covered 12 files and resolved all 10 local links;
reports/evidence/documentation_links.json records the observed result.
Local results and conditional Snowflake/Airflow wording remain separate. A video/public host
is an optional later user presentation action; actual screenshots and a walkthrough satisfy
the amended local demonstration request. No claim is made that the learner has answered
questions or personally executed warehouse work.

## Completion decision

The required local workflow, saved evidence, startup/rendering checks and documentation pass.
The permitted environment limitation is Docker/Airflow runtime unavailable. Future Snowflake
execution, GitHub CI execution and public hosting remain unverified/prepared as explicitly
required boundaries, not secretly executed completion gates. No required authorized local
implementation remains pending.
