select trip_record_id from {{ ref('fct_trip') }}
where distance_miles<0 or duration_seconds<0 or recorded_charge is null
