# MobilityOps progress — authoritative local state

Updated 10 October 2026, Asia/Kolkata. The later full-build objective supersedes the original
milestone stopping/calendar instructions. All Snowflake actions remain user-only and unverified.

## Confirmed implementation

The local pipeline exists and runs:
official TLC download → immutable manifests/profile/contracts → single shared Python classifier
→ atomic accepted/quarantine Parquet generations → transactional DuckDB → six dbt models/tests/docs
→ independent per-artifact reconciliation → strict checksummed snapshots → Streamlit.

The fixture, source bytes, expected-results JSON, existing environments and dependency locks
are preserved. Fixture/sample/real remain separate. Sample has its own artifact IDs and parent lineage.
Source revisions, broken provenance and missing required columns stop processing.

| Scope | Source rows | Accepted | Quarantined | Accepted recorded passenger charge USD |
| --- | ---: | ---: | ---: | ---: |
| Fixture | 12 | 8 | 4 | 134.00 |
| Separate January sample | 10,000 | 10,000 | 0 | 293,899.66 |
| January 2026 | 3,724,889 | 3,724,888 | 1 | 108,686,701.73 |
| February 2026 | 3,399,866 | 3,399,864 | 2 | 102,381,010.56 |
| March 2026 | 3,952,451 | 3,952,449 | 2 | 118,958,427.44 |
| Three real months total | 11,077,206 | 11,077,201 | 5 | 330,026,139.73 |

Each real artifact passed source/disposition/fact/mart coverage, unique/disjoint keys and
independent eligible-source Decimal charge equality at zero count/cent differences.
Source months are Jan–Mar; actual accepted pickup coverage is **2008-12-31 through 2026-04-01**.
Valid out-of-partition timestamps are retained with warnings, not silently filtered.
The recorded charge metric is signed passenger charge, not profit or complete business revenue.

## Artifact identities

- Preserved fixture: 19d61b096876bd9739a6d09a601f096d76f2186ffac87b9e281e087dc20668a8
- January: 8b3933fe6f0d7b6d8826613c0dd724edc680ff7c49e2bd4c7635c05102728637
- February: 5352ca800f29c40a221ed5b0c6392c451fde7fcd236a73651bd5aea74713a26c
- March: d7af794a7ac06cbbb5afb4d1df9e86d84f2e878584de708ff2ffca4bd7d234d9
- January sample: 108744581071d56e9d3f17d85279012c96a70b4d6684495030920117847522aa
- Official zones: 1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed

## Observed verification

- The first full fixture/sample/Jan/Feb/Mar workflows passed; each built six dbt models and
  passed 24 dbt data tests, with docs generated and valid snapshots.
- reports/evidence/local_scenarios.json: fixture-only rollback leaves accepted/quarantine/
  artifact/audit counts all zero; retry yields 8/4 and one audit; replay inserts zero.
  Separate fixtures loaded Feb → Mar → older Jan; incremental fact became six and replay stayed six.
- reports/evidence/real_incremental.json: all three actual monthly accepted-key increments
  equal expected artifact accepted counts. January replay inserted 0 accepted/0 quarantined,
  replayed 3,724,889 keys and retained 11,077,201 facts.
- Final offline suite passed **65 tests in 8.18 seconds**, including numeric extreme-exponent,
  provenance, source/payload consistency and prepared-template checks.
  The final run output is reports/local/pytest-final.log.
- Native Ruff.exe/dbt.exe are blocked by Windows Application Control. Local dbt uses dbtRunner
  through its installed Python; lint uses the official Ruff 0.16.10 WASM Node build with
  verified integrity receipt. Final Ruff recheck passed with **zero diagnostics**.
- Main/local dbt/Snowflake dbt and both clean isolated environments passed **python -I -m pip check**.
  The Snowflake environment check inspected package metadata only, not any connection.
- Clean exact-lock app/dbt installs under .checks passed the complete fixture workflow after
  fixing a summed count exported as decimal. Current snapshots enforce integer count types.
  The clean app environment additionally passed **11 offline dashboard checks in 4.38 seconds**.
- Existing app PYTHONPATH leaked source distribution metadata into ordinary dbt pip checks.
  The environments themselves are consistent; isolated -I checks passed, and dbt subprocesses
  now remove PYTHONPATH rather than adding app packages to dbt environments.
- Streamlit actually started on localhost:8501 and rendered both three-month tabs in the
  in-app browser. Operations showed 11,077,201 accepted/$330,026,139.73 and weighted metrics.
  Operations, Reliability and replay/recovery screenshots are saved in docs/screenshots;
  their checksums and snapshot provenance are in reports/evidence/dashboard_smoke.json.
- Local Git repository initialized on main; raw data, environments, databases, .private and
  private logs were confirmed ignored. No commit or push occurred during that initial build.

## Final local completion / resume here

The final fixture/sample workflows and unchanged real January replay completed successfully.
The real replay inserted 0 accepted/0 quarantined, replayed 3,724,889 source keys, retained
11,077,201 facts and took 205.092 seconds for the observed complete replay workflow.
No pipeline run remains pending. The final source checksum recheck matched all five preserved
fixture/sample/real artifacts; a fresh read-only real fact query confirmed 11,077,201 rows,
signed charge 330,026,139.73 and actual dates 2008-12-31 through 2026-04-01.

The app's localhost:8501 health endpoint returned HTTP 200/ok during the final audit.
The published aggregate files total 4,680,078 bytes. Their manifest SHA-256 is
ac2c1285d92dc4c585f1fa25224cfd69894382ab2b0be20327cd0e75a27a29cb.
The Streamlit server was left available for local review; a future session must check its
actual health or launch it from README, rather than assuming the prior process is live.

docs/completion_audit.md maps the full amended request to inspected code and observed evidence.
reports/evidence/completion.json and per-scope artifact reports contain the final machine
results, exact versions, commands, preserved source metadata and environmental limitations.
The final documentation link check inspected 12 Markdown files and all 10 local Markdown
links resolved; reports/evidence/documentation_links.json records the result. The check
uses explicit UTF-8 after Windows' default CP1252 rejected a Unicode documentation character.

README/root architecture.md and operations/dictionary/demo/learning/resume/evidence docs exist.
Manual Snowflake ordered SQL, separate real/recovery contexts, shared-policy ZIP, 19 policy
vectors, user-only dbt wrapper and offline export comparator exist. All warehouse execution,
type/alignment, Streams/Task, privileges and cost checks remain UNVERIFIED.
Final official-documentation review confirmed COPY projection/casts/metadata/session-variable
usage and corrected unordered JSON comparisons in the three prepared procedure scopes.
Local static tests passed; they do not establish warehouse compilation or execution.

## Environment

Workspace: C:\Users\purav\OneDrive\Desktop\Data Projects\MobilityOps.
Windows 10 Pro (registry reported 25H2/build 26200), PowerShell 7.6.5, 64-bit Python 3.14.7.
RAM approximately 16.85 GB total / 3.2 GB available at inspection; disk approximately 281 GB free.
Existing .venv/.venv-dbt-local/.venv-dbt-snowflake preserved. No global packages installed.

Main: requests 2.34.2, pyarrow 25.0.1, duckdb 1.5.6, PyYAML 6.0.3, pydantic 2.13.5,
streamlit 1.65.0, pandas 3.0.6, plotly 7.1.0, pytest 9.1.1, ruff 0.16.10.
Local dbt: dbt-core 1.12.5/dbt-duckdb 1.11.0.
Preserved manual dbt: dbt-core 1.12.5/dbt-snowflake 1.12.1.
Clean installation checks preserve original environments and original locks.

## Prepared / pending / prohibited

- Docker/Compose absent. Airflow pinned to apache/airflow:3.3.2-python3.12 with matching
  official constraints reviewed/retrieved; container/DAG runtime is PENDING.
- Offline CI is prepared; no GitHub execution result is claimed.
- Optional public-hosting instructions prepared; no deployment.
- Every Snowflake action is the user's: no agent account access, read-only connection,
  upload, SQL, dbt Snowflake target, Task, credential request or warehouse operation.
- No hosted app deployment, message, production SLA, savings or benchmark claim.

## GitHub publication

The user subsequently authorized pushing the completed project to
https://github.com/PuravDesai004/MobilityOps. The public repository was verified empty with
default branch main. The publishable candidate set is about 5.6 MB; excluded runtime paths
and common credential patterns were checked. A repository-local GitHub noreply author
identity is configured; no global Git settings changed. The initial project push succeeded
on 10 October 2026. Local and independently read remote main both matched project commit
136c657a1c9d29689158eaca090f5961bf4e5a74. That commit contains 157 files, with every staged
file verified byte-identical to the reviewed local file before publication.
Git's default line-ending conversion changed staged bytes, including the snapshot CSV and
locks. Repository .gitattributes disables that conversion to preserve exact reviewed bytes.
The original completion.json is a historical local-build receipt from before this push.
reports/evidence/github_publication.json records the verified first push and the observed
GitHub Actions run. CI was in progress at 15:01:21 UTC (20:31:21 Asia/Kolkata); no remote
passing result is claimed from that observation. Later documentation commits can follow the
initial project commit without changing its saved publication identity.
All Snowflake execution remains unverified/user-only; hosted app deployment is not authorized.

## Preserved milestone history

Milestone 1 produced the 12-row/3-row-group/8,836-byte fixture, stable physical-row IDs,
profile/manifest/append-only history and expected-only 8/4 documentation.
Milestone 2 on 6 October downloaded January (64,165,080 bytes/4 row groups/20 fields),
official zones (265 rows/12,331 bytes), and separate 10,000-row sample (1,428,141 bytes),
then verified unchanged reuse. Its 23 offline tests passed. The old no-classification/no-dbt
milestone statuses were true then and have been superseded by the verified build above.
The expected JSON remains unchanged and is a historical expectation file; observed classifier
outcomes are stored separately in classification/reconciliation evidence.
