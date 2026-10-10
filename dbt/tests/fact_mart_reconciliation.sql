with facts as (
    select artifact_id, count(*) as n, sum(recorded_charge) as money
    from {{ ref('fct_trip') }} group by artifact_id
), accepted as (
    select artifact_id, count(*) as n, sum(recorded_charge) as money
    from {{ ref('stg_trips') }} group by artifact_id
), marts as (
    select artifact_id, sum(trip_count) as n, sum(recorded_charge_sum) as money
    from {{ ref('mart_zone_hour') }} group by artifact_id
)
select a.artifact_id from accepted a
left join facts f on a.artifact_id=f.artifact_id
left join marts m on a.artifact_id=m.artifact_id
where f.n is null or m.n is null or a.n<>f.n or f.n<>m.n
    or a.money<>f.money or f.money<>m.money
