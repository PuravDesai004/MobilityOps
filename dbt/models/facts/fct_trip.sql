{{ config(materialized='incremental', unique_key='trip_record_id',
          incremental_strategy='delete+insert') }}
-- Grain: one stable accepted source-row key. Unseen keys allow older-month backfills.
select trip_record_id, dataset_id, artifact_id, source_partition, source_row_number,
    pickup_at, dropoff_at, cast(pickup_at as date) as pickup_date,
    extract(hour from pickup_at) as pickup_hour,
    pickup_zone_source_id, dropoff_zone_source_id, pickup_zone_key, dropoff_zone_key,
    passenger_count, payment_type, distance_miles, duration_seconds, recorded_charge,
    {{ array_length('warning_codes') }} as warning_count
from {{ ref('stg_trips') }} s
{% if is_incremental() %}
where not exists (select 1 from {{ this }} f where f.trip_record_id=s.trip_record_id)
{% endif %}
