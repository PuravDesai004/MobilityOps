with facts as (
    select dataset_id, artifact_id, count(*) as n, sum(recorded_charge) as money
    from {{ ref('fct_trip') }} group by dataset_id, artifact_id
), accepted as (
    select dataset_id, artifact_id, count(*) as n, sum(recorded_charge) as money
    from {{ ref('stg_trips') }} group by dataset_id, artifact_id
), marts as (
    select dataset_id, artifact_id, sum(trip_count) as n, sum(recorded_charge_sum) as money
    from {{ ref('mart_zone_hour') }} group by dataset_id, artifact_id
)
select coalesce(a.dataset_id, f.dataset_id, m.dataset_id) as dataset_id,
    coalesce(a.artifact_id, f.artifact_id, m.artifact_id) as artifact_id
from accepted a
full outer join facts f on a.dataset_id=f.dataset_id and a.artifact_id=f.artifact_id
full outer join marts m on coalesce(a.dataset_id, f.dataset_id)=m.dataset_id
    and coalesce(a.artifact_id, f.artifact_id)=m.artifact_id
where a.n is null or f.n is null or m.n is null or a.n<>f.n or f.n<>m.n
    or a.money<>f.money or f.money<>m.money
