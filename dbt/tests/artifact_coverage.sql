with counts as (
    select dataset_id, artifact_id, sum(source_count) as n,
        sum(accepted_count) as a, sum(quarantined_count) as q,
        sum(recorded_charge_sum) as money
    from {{ ref('mart_daily_reconciliation') }} group by dataset_id, artifact_id
)
select s.* from {{ source('control', 'source_artifacts') }} s
left join counts c on s.dataset_id=c.dataset_id and s.artifact_id=c.artifact_id
where c.n is null or c.n<>s.source_rows or c.a<>s.accepted_rows
    or c.q<>s.quarantined_rows or c.money<>s.recorded_charge
