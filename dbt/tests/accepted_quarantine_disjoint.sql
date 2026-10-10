select a.trip_record_id from {{ source('curated', 'accepted_trips') }} a
join {{ source('curated', 'quarantined_trips') }} q
on a.dataset_id=q.dataset_id and a.trip_record_id=q.trip_record_id
