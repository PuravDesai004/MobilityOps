# Manual warehouse evidence template

All sections below start **UNVERIFIED**. Fill only after personally executing the matching
step. Keep private copies under `.private/snowflake`; remove account/user identifiers or
other private information before sharing selected evidence. Do not include credentials.

| Field | Observed value |
| --- | --- |
| Executed by / observation time | |
| Stage / SQL filename / command | |
| Database/scope and role | |
| Dataset / artifact SHA / original bytes / source rows | |
| Zone SHA / policy ZIP SHA / policy version | |
| COPY query ID / actual COPY result | |
| Timestamp TYPEOF/value and physical row alignment checks | |
| Promotion query/batch ID / result / physical and logical rows | |
| Accepted / quarantine / distinct keys / overlap | |
| Accepted charge / fact charge / mart charge / local reference differences | |
| Replay physical rows / zero inserts / replay keys | |
| Failure exception / pre-post target and audit counts / pending Stream / retry result | |
| Task warehouse / state / query ID / history / suspension check | |
| dbt version / command / run_results / test/model results | |
| Export filenames / time / hashes / origin / comparison result | |
| Warehouse metering time range / credits / latency / balance review | |
| Task/warehouse shutdown results / Stream STALE_AFTER | |
| Result status and unresolved errors | UNVERIFIED |

Attach actual non-secret query CSV/JSON outputs and terminal logs. Expected output copied
from the guide is not evidence. Offline CSV comparison is user-supplied evidence, not an
independent account audit. Upgrade README/resume claims only when the required results exist.
