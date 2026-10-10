# Recruiter demonstration and optional hosting

## Three-minute recruiter walkthrough

1. **0:00–0:40 — Operations.** Start Streamlit from README. Explain that the three source months
   are January–March 2026; actual valid pickup dates also include out-of-partition rows.
   The app visibly reports source months, date coverage, origin and export time.
2. **0:40–1:00 — Filters and units.** Point to 11,077,201 accepted source records and $330,026,139.73 signed recorded passenger
   charge. This is neither physical-trip global deduplication nor profit. Select a smaller
   date range and one zone; count/charge and weighted distance/duration update together.
3. Show observed zone/hour pickup activity. The chart displays the 12 busiest selected pickup zones while
   headline metrics cover all selected zones. Units are miles, minutes and USD.
4. **1:00–1:50 — Business Insights and one SQL finding.** Open the fixed all-month view.
   Upper East Side South contributes 479,826 accepted pickups (4.33%), peaking at 17:00–18:00.
   Explain that this is observed Yellow Taxi activity, not all transport demand or a proven
   staffing benefit. Open `sql/analysis/monthly_changes.sql`: named CTEs build monthly
   measures, `LAG` reads the previous month, and `NULLIF` protects a zero denominator.
   February's monthly count falls 8.73%, but its in-month trips/calendar day rise 1.05%.
   Thirty-one versus twenty-eight days explains why the measures tell different stories;
   this does not establish the cause of activity changes. Mention the 42 retained records
   excluded only from daily normalization. Show `docs/business_findings.md` for evidence.
5. **1:50–2:40 — Reliability.** 11,077,206 source = 11,077,201 accepted + 5 quarantined. Show artifact
   identities and zero count/charge differences, and the actual real January replay audit
   with zero inserts. Expand the separate isolated fixture rollback/retry/backfill evidence.
6. **2:40–3:00 — Architecture and scope.** Show `architecture.md` and machine reports. Explain that original files are immutable,
   identity uses hash plus physical position, models load unseen keys, and aggregates rebuild
   from the complete fact. Original payloads and warning/reason arrays remain preserved.
7. State boundaries clearly: DuckDB/dbt/Python, SQL findings, the local app and observed CI
   were demonstrated. Cite the actual passing run from `docs/progress.md`. Airflow's
   Docker runtime is pending; Snowflake execution is your later manual implementation.

For a live pipeline demo use the tiny fixture, which is fast and has interpretable expected
8/4 dispositions. Avoid rerunning a full month while a recruiter waits. Show preserved
fixture rows 1/2 (same payload, different physical keys) and row 5 (null pickup, one quarantine).
The 10,000-row sample is a distinct convenience artifact, not an estimate of all taxi trips.

## Screenshot evidence

Current README captures are `assets/screenshots/operations.jpg`, `reliability.jpg` and
`business_insights.jpg`. They show the actual local app with the verified real snapshot,
all dates/zones for Operations and fixed Jan–Mar scope for Insights. The existing
`docs/screenshots/replay_recovery.jpg` is reused as historical zero-insert replay evidence;
the other earlier screenshots are preserved. New capture metadata is
`reports/evidence/presentation_dashboard.json`; the original receipt remains
`reports/evidence/dashboard_smoke.json`. No images or metric values were edited.

For replacement captures, launch the actual app, wait until the running indicator disappears
and charts/axes render, then capture each tab. Review headline values, complete chart/legend,
quality/reconciliation table, filters, origin and dates before saving. For desktop layout
verification, the captured viewport was 1280×1024 and the temporary override was reset.
Scroll to replay/recovery for the optional fourth view. Keep captions/provenance together.

## Launch commands

```powershell
Set-Location 'C:\Users\purav\OneDrive\Desktop\Data Projects\MobilityOps'
$env:PYTHONPATH = "$PWD\src"
.\.venv\Scripts\python.exe -m mobilityops.cli validate-snapshot --path demo/published
.\.venv\Scripts\python.exe -m streamlit run app/streamlit_app.py --server.address 127.0.0.1 --server.port 8502 --browser.gatherUsageStats false
```

Default: small verified `demo/published` bundle. To show fixture instead, set
`$env:MOBILITYOPS_SNAPSHOT_DIR = "$PWD\demo\fixture"` before launch. Remove that variable
afterward. Stop the server with Ctrl+C. Local screenshots/video plus this repository can
demonstrate the project without a public hosting account or ongoing warehouse trial.

## Public hosting preparation — not deployed

Optional platform: Streamlit Community Cloud. Read its current
[deployment guide](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app)
and [dependency guidance](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/app-dependencies)
before personally deploying. The app does not need Snowflake secrets or a live warehouse.
No public app deployment was performed. Repository publication and any observed GitHub CI
status are recorded separately in docs/progress.md.

1. Review the final small bundle with `validate-snapshot --path demo/published`. Review the
   public evidence for private local paths and only share non-secret files.
2. Inspect Git's candidate list; `.venv*`, raw source, samples, classified outputs, databases,
   private profiles/logs and `.checks` must remain excluded. Keep source code, config, tests,
   locks, docs, reviewed fixture and the snapshot manifest's referenced published generation.
3. Once the repository is on GitHub, personally select that repository/branch in your hosting
   UI and choose entry point **app/streamlit_app.py**.
4. The root **requirements.txt** includes the exact app lock and editable package; do not add
   a competing dependency file beside the app. Choose the supported Python runtime matching
   the verified 3.14 environment. If the platform lacks that version, verify a compatible
   runtime in isolation before changing the choice; 3.12 syntax checks alone are not runtime
   verification of every dependency.
5. Leave secrets empty and snapshot path at its default. Visitors get a fixed historical
   aggregate bundle, not raw trip data, private logs or a pipeline execution button.
6. Personally verify all three tabs, filter behavior, fixed Insights scope, origin/date labels and corrupt-data errors
   after deployment. Record the actual URL/build result separately. Hosting availability,
   quotas and future service changes are not guaranteed by local smoke tests.

The fixture offline CI is included; only an observed GitHub run establishes its remote result.
Warehouse trial expiry does not remove the local aggregate demo. An actual public hosting
test remains future user work, and is not required to launch the verified local app.
