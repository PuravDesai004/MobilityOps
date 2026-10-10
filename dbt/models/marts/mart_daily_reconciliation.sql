-- Grain: dataset/artifact/source partition/actual pickup date bucket, including missing date.
with records as (
    select dataset_id, artifact_id, source_partition, pickup_at, recorded_charge,
        1 as accepted, 0 as quarantined from {{ ref('stg_trips') }}
    union all
    select dataset_id, artifact_id, source_partition, pickup_at, recorded_charge,
        0 as accepted, 1 as quarantined from {{ source('curated', 'quarantined_trips') }}
)
select dataset_id, artifact_id, source_partition,
    coalesce(cast(cast(pickup_at as date) as varchar), 'MISSING_OR_INVALID_PICKUP')
        as pickup_date_bucket,
    count(*) as source_count, sum(accepted) as accepted_count,
    sum(quarantined) as quarantined_count,
    sum(case when accepted=1 then recorded_charge else 0 end) as recorded_charge_sum
from records
group by dataset_id, artifact_id, source_partition, pickup_date_bucket
