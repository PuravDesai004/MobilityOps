-- Grain: dataset/artifact/partition + actual pickup date/hour + pickup zone.
-- Full rebuild from complete fact preserves historical groups after each new artifact.
select dataset_id, artifact_id, source_partition, pickup_date, pickup_hour, pickup_zone_key,
    count(*) as trip_count, sum(recorded_charge) as recorded_charge_sum,
    sum(distance_miles) as distance_miles_sum, count(distance_miles) as distance_count,
    sum(duration_seconds) as duration_seconds_sum,
    count(duration_seconds) as duration_count,
    cast(sum(case when warning_count > 0 then 1 else 0 end) as bigint) as warning_trip_count
from {{ ref('fct_trip') }}
group by dataset_id, artifact_id, source_partition, pickup_date, pickup_hour, pickup_zone_key
