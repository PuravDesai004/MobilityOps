# Local operations runbook

All local commands run from `C:\Users\purav\OneDrive\Desktop\Data Projects\MobilityOps`.
The PowerShell wrapper uses existing environments and explicit local dbt. Cloud actions
belong exclusively to the separate manual Snowflake guide.

## Routine execution and replay

```powershell
.\scripts\run_local.ps1 -Scope fixture -Offline
.\scripts\run_local.ps1 -Scope sample -Month 2026-01 -Offline
.\scripts\run_local.ps1 -Scope real -Month 2026-01 -Offline
```

Use the source month explicitly and process months sequentially. A replay should report
zero accepted/quarantined inserts, all source rows replayed, and unchanged fact counts.
Preparation still validates the source's actual checksum/contract, classification validates
its output pair, and modeling/reconciliation runs again. Offline means no source HTTP call;
it does not skip integrity checks. The app never triggers this command.

Latest result: `reports/local/<scope>/<month-or-fixture>/latest_workflow.json`.
History: `reports/local/workflow_attempts.jsonl`. Classified pointer:
`data/classified/<scope>/<month-or-fixture>/classification.json`. DuckDB ingestion history:
`data/db/<scope>.attempts.jsonl`. Logs may contain local paths but no credentials.

## Resume after a failure

| Failure | State to inspect | Safe action |
| --- | --- | --- |
| HTTP timeout/503 | Download history and sibling temporary artifact cleanup | Rerun preparation; bounded transient retries already apply |
| HTTP 404 or unlisted month | Official TLC link, actual HTTP status | Verify an official link; don't repeatedly retry a permanent error |
| Required column absent | Contract/profile schema | Stop; inspect provider schema before adopting a new contract |
| Source checksum/provenance mismatch | Source manifest versus actual bytes/parent | Stop; preserve both source and diagnostics; revision review |
| Classification fails partway | Latest successful pointer and FAILED attempt | Correct cause and rerun; prior output pair remains intact |
| DuckDB ingestion error | FAILED history and committed audit/target counts | Rerun after correction; accepted/quarantine/audit rollback together |
| dbt model/test fails | `dbt_build.log`, execution receipt and run_results | Inspect the build log first; docs generation can replace run_results; rebuild and reconcile |
| Reconciliation differs | Per-artifact differences and independent source result | Stop export; investigate the exact artifact rather than cancel errors across months |
| Snapshot corrupt/missing | Manifest bytes/checksums/path/schema | Regenerate from passed model/verification data, preserving prior bundle |

```powershell
.\scripts\run_local.ps1 -Scope real -Month 2026-01 -Offline -Stage models
.\scripts\run_local.ps1 -Scope real -Month 2026-01 -Offline -Stage verify
.\scripts\run_local.ps1 -Scope real -Month 2026-01 -Offline -Stage snapshot
```

Single-stage commands require previous stages' artifacts. A failure in dbt does not undo a
completed curated transaction; models and final snapshots are later publication boundaries.
Do not run two writers against one DuckDB file. Stop the active workflow before investigating
writer contention. Streamlit reads files only and does not hold a database lock.

## Source/policy/zone revision

Detection is implemented; automatic adoption is deliberately not implemented. Preserve the
old source and manifest. Review the new provider link/bytes/schema, classification policy
and zone checksum. Use a new explicitly isolated source/database/classification scope,
rerun fixture and reconciliation, compare totals, and choose one active version deliberately.
Do not alter an active original file or edit its manifest hash to silence the error.
Do not append both month revisions into the same active fact. A full model rebuild does
not by itself replace a curated source revision safely.

## Controlled failure, incremental and backfill demonstrations

```powershell
$env:PYTHONPATH = "$PWD\src"
.\.venv\Scripts\python.exe scripts/verify_local_scenarios.py
# Optional supervised repeat of completed real replay; rechecks all active months:
.\.venv\Scripts\python.exe scripts/verify_real_replay.py
```

The scenario script creates unique databases/sources under ignored `.checks/scenarios`.
Rollback must leave accepted/quarantine/artifact/audit counts 0/0/0/0; retry gives 8/4/1/1;
replay inserts 0/0. Separate February/March fixtures each add two keys, then older January
adds two; the incremental fact finishes with six and replay keeps six. These are deterministic
fixtures, not more real months or Snowflake tests. Saved real increments/replay are a separate
report and never require wiping completed monthly data.

## Snapshot review, packaging and launch

```powershell
$env:PYTHONPATH = "$PWD\src"
.\.venv\Scripts\python.exe -m mobilityops.cli validate-snapshot --path demo/snapshots
.\.venv\Scripts\python.exe scripts/package_demo.py
.\.venv\Scripts\python.exe -m mobilityops.cli validate-snapshot --path demo/published
.\.venv\Scripts\python.exe -m streamlit run app/streamlit_app.py --server.address 127.0.0.1 --browser.gatherUsageStats false
```

Packaging copies only a validated small bundle into a new generation and atomically
publishes its manifest. Review origin/date coverage/export time before sharing it. A prior
generation is historical evidence, not automatically current. No public deployment occurs.
Stop Streamlit with Ctrl+C. Delete old controlled demo/check generations only after you
have reviewed which ones are needed; no routine cleanup here recursively deletes user data.

## Environment and lint diagnostics

```powershell
.\.venv\Scripts\python.exe -I -m pip check
.\.venv-dbt-local\Scripts\python.exe -I -m pip check
.\.venv\Scripts\python.exe -m pytest -q
node scripts/lint_wasm.cjs
```

Windows Application Control blocked native Ruff/dbt launch; installed Python API dbt and
official WASM Ruff work. Do not disable security policy. The existing Node runtime supports
the optional lint fallback. If Node is absent on a new machine and native Ruff is allowed,
use `.venv\Scripts\python.exe -m ruff check src tests app scripts airflow/dags`.
This workspace's sandbox blocked pytest temporary-directory access and dbt process pipes;
the agent's authorized elevated local runs succeeded. Ordinary user PowerShell does not have
that agent sandbox. Keep dependencies isolated and preserve exact lock files.

## Retained local dbt documentation and historical outputs

All five completed scopes retain verification, workflow and dbt execution receipts, build/docs
console logs and the original run_results. Build logs prove the recorded model/test pass counts;
run_results may describe the later docs-generation command. Canonical SQL/YAML/macros/tests
remain under dbt/. Compiled SQL, execution wrappers and parser/graph/unused semantic outputs
are regenerable and were removed. Earlier duplicate documentation, detailed dbt logs, terminal
logs and inactive snapshot history are preserved in the sibling local archive; its index maps
original paths and checksums. Active source manifests, histories, databases and snapshot pointers
remain at their working paths. Only historical inactive generations were archived.

The latest full-source docs keep `index.html`, `catalog.json` and `manifest.json` together at
`reports/local/real/2026-03/dbt_target`. To regenerate local documentation explicitly:

```powershell
$env:MOBILITYOPS_DB_PATH = "$PWD\data\db\real.duckdb"
.\.venv-dbt-local\Scripts\python.exe scripts/dbt_local.py docs generate --project-dir dbt --profiles-dir dbt --target local --target-path reports/local/real/2026-03/dbt_target --log-path reports/local/real/2026-03/dbt_logs
Remove-Item Env:MOBILITYOPS_DB_PATH
```

To inspect that retained bundle locally, serve its generated files, open localhost:8080,
and stop the server with Ctrl+C:

```powershell
.\.venv-dbt-local\Scripts\python.exe scripts/dbt_local.py docs serve --project-dir dbt --profiles-dir dbt --target local --target-path reports/local/real/2026-03/dbt_target --port 8080
```

This requires the existing local real database. A new clone can run the bounded fixture workflow
to generate fixture dbt docs instead. Documentation generation does not establish a model/test
build pass. Historical diagnostic paths inside saved immutable receipts remain historical and
are recoverable from the archive or earlier Git commits; current public claims link retained
receipts, SQL exports, screenshots and actual CI. No Snowflake command is involved here.

## Future Snowflake Stream recovery (user only, unverified)

Suspend the Task before a manual recovery and keep one consumer. Inspect STALE_AFTER,
actual raw artifact/position keys, curated keys and manifests. A stale Stream cannot be
recreated and called recovered: its old offset may be lost. Preserve raw data and create
a new isolated recovery database using the supplied bootstrap/work/procedure templates.
Register reviewed original manifests and re-upload unchanged files into that isolated scope,
with Stream created before COPY. Verify classifications, key coverage and money, then
deliberately choose the recovered active scope. Reestablish a future-load Stream only after
accounting for prior source records. This is a reviewed rebuild plan, not an executed test.

Tasks remain suspended outside supervised sessions. Suspend warehouse compute afterward,
inspect actual metering/trial balance, and remember stored-data charges and retention continue.
See the 17-step manual guide for exact user-run queries and evidence requirements.

