# Resume wording, interview preparation and handoff

## Verified local resume bullets

- Built a local Python/PyArrow → DuckDB → dbt pipeline for **11,077,206 NYC TLC Yellow Taxi
  source records across January–March 2026**, with immutable SHA-256 artifacts, bounded
  batch processing and preserved source-row lineage.
- Implemented deterministic classification and transactional stable-key ingestion; verified
  **11,077,201 accepted / 5 quarantined records** and **$330,026,139.73 recorded passenger
  charge**, with zero per-artifact count/Decimal reconciliation differences against an
  independently calculated eligible-source reference.
- Demonstrated zero-insert replay of a completed real artifact, isolated local rollback/retry,
  and older-month fixture backfill; built six local dbt models with tested dimensions,
  incremental fact, rebuilt aggregate marts and generated documentation.
- Built and locally smoke-tested a credential-free Streamlit dashboard using checksummed
  aggregate snapshots, date/zone filters, weighted metrics and historical reliability evidence;
  verified offline fixture CI and reproducible isolated dependency locks. Exact observed
  remote run URLs and commits are linked from [README](../README.md#reliability-and-executed-validation).
- Created seven reusable DuckDB analysis queries covering zone/hour activity, normalized
  month changes and distinct-record quality rates; reconciled full-source analytical totals
  and added a validated, snapshot-only Business Insights view.

Use the current test count from README and its linked verification evidence if adding that metric;
`reports/evidence/completion.json` preserves the original build's historical count.
Timings are observed individual runs, not benchmark improvements. No production uptime,
business savings, real-time feed, global trip deduplication, public deployment, Airflow run
or executed Snowflake experience is established by these bullets.

## Snowflake wording only after your manual verification

Do **not** use this as completed experience yet:

> Implemented and manually verified Snowflake staged Parquet/COPY provenance, an append-only
> Stream, a transactional shared-policy promotion procedure and a warehouse-backed Task;
> demonstrated fixture replay, rollback/retry and dbt model reconciliation against local results.

Replace/add counts, credit usage, Task query IDs and timings only from your real saved outputs.
Use “prepared Snowflake templates” in the meantime. Likewise, say “prepared Airflow DAG/Docker
configuration, runtime pending” until you have actual Docker/DAG execution evidence.

## Interview explanations

**Business story:** repeated or malformed monthly deliveries must not inflate trip/charge
reports. The project preserves the original delivery, classifies each physical record once,
verifies the stored/modelled totals, and serves a fixed auditable demo without cloud costs.

**Reliability story:** filename/run ID are not identity inputs. Transactions cover related
curated writes and successful audits. Partial classified/snapshot candidates do not become
visible successes. Independently recalculated money prevents downstream copies from masking
the same upstream mistake. Source revisions stop for review.

**Incremental story:** facts select unseen stable keys rather than timestamps, so older-month
arrivals work. Aggregate marts rebuild from all facts to retain historical totals. The real
monthly increments and replay have their own evidence; the deliberately out-of-order backfill
exercise uses small labeled fixtures in an isolated database.

**Tradeoff story:** single-writer local DuckDB and rebuilt small marts are enough for this
portfolio scale. No unnecessary distributed infrastructure is introduced. Missing essential
measurements block facts; warning-only unusual records are retained. Source month differs
from actual pickup date; 2008–2026 outliers remain visible under the agreed warning policy.

**Boundary story:** local DuckDB cannot prove a Snowflake Stream offset, Task permission,
account cost or cloud transaction. Prepared templates reuse the pure Python policy but require
your own fixture/type/alignment and account execution checks before claiming cloud parity.

## Handoff to the prompt designer

MobilityOps's local end-to-end implementation has been built and verified on the user's
Windows/PowerShell workspace with preserved environments, locks and fixture expectations.
Three official 2026 monthly artifacts were processed sequentially, with 11,077,206 source,
11,077,201 accepted and 5 quarantined records, $330,026,139.73 accepted recorded charge,
and zero per-artifact reconciliation differences. Real replay and isolated fixture rollback,
retry, incremental/backfill checks are saved as machine evidence. Six local dbt models,
tests/docs, validated portable snapshots and a locally launched three-view Streamlit demo exist.
The presentation extension runs seven read-only SQL files against the existing verified real
DB, exports actual CSV/JSON results, preserves source-file versus pickup-month meaning and
reconciles full-scope counts/charges. Findings include 479,826 Upper East Side South pickups
(4.33%), February −8.73% monthly count versus +1.05% in-month daily rate, and 28.77% distinct
accepted warning coverage. Actual unedited screenshots and a three-minute walkthrough are saved.
SQL/formula/scope/snapshot tests are offline; the app has no DB/network query path.

Start from root `architecture.md`, `README.md` and
`reports/evidence/completion.json`; inspect actual files/evidence before making new claims.
The repository was locally prepared at build completion; the later user-authorized GitHub
publication status is recorded in README. Docker/Airflow runtime is pending because
Docker is absent. All Snowflake account/SQL/dbt/Task execution is manual,
unverified and prohibited for the agent; ordered templates are in `docs/snowflake_implementation.md`.
Future work should guide the user's own warehouse fixture checks, preserve local verified data,
and keep conditional resume wording separate from demonstrated local achievements.
