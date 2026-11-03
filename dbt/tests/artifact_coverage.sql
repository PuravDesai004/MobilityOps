with counts as (
    select dataset_id, artifact_id, sum(source_count) as n,
        sum(accepted_count) as a, sum(quarantined_count) as q,
        sum(recorded_charge_sum) as money
    from {{ ref('mart_daily_reconciliation') }} group by dataset_id, artifact_id
)
select coalesce(s.dataset_id, c.dataset_id) as dataset_id,
    coalesce(s.artifact_id, c.artifact_id) as artifact_id,
    s.* exclude (dataset_id, artifact_id)
from {{ source('control', 'source_artifacts') }} s
full outer join counts c on s.dataset_id=c.dataset_id and s.artifact_id=c.artifact_id
where s.dataset_id is null or s.artifact_id is null
    or coalesce(c.n, 0)<>s.source_rows or coalesce(c.a, 0)<>s.accepted_rows
    or coalesce(c.q, 0)<>s.quarantined_rows or coalesce(c.money, 0)<>s.recorded_charge
