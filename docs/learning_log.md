# Learning map

Read the root architecture guide after the build, then use its concrete walkthroughs.
This file records the concept map; it does not claim the learner answered questions or
personally completed cloud exercises.

| Concept | Understand through |
| --- | --- |
| Parquet and Arrow batches | Metadata profile and 10,000-row scans, without full-month pandas |
| File identity versus row identity | Original checksum plus one-based position; fixture rows 1/2 |
| Manifest versus attempt history | Stable artifact receipt and successive success/failure events |
| Contract versus quarantine | Missing required column stops the file; null row value gets a reason |
| Warnings versus reasons | Signed negative charge and unknown zone remain accepted |
| Decimal rounding | Positive/negative half-cent and rounding-overflow test vectors |
| Atomic publication | A completed generation pointer preserves prior results on failure |
| Transaction and replay | Isolated before-commit rollback; retry 8/4; rerun inserts zero |
| dbt model and grain | Staging → dimensions/fact → two marts with explicit key definitions |
| Incremental backfill | Older January fixture arrives after February/March; unseen keys still load |
| Reconciliation | Original eligibility/reference equals accepted/fact/mart counts and charge |
| Snapshot metrics | Weighted sums/denominators after filters; schema/checksum gates |
| Orchestration | Same functions behind CLI, Python workflow and prepared Airflow stages |
| Cloud evidence boundary | Manual fixture Stream/Task checks remain distinct from local tests |

For interview practice, explain why a filename is a weak key, how a transaction protects both
dispositions, why average-of-averages is wrong, and why a max timestamp cutoff loses late data.
Your first manual Snowflake exercise is context/grants/budget inspection, followed by a tiny
fixture with actual timestamp and physical-row alignment checks.
