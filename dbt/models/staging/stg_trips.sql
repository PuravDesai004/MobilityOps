-- Grain: one accepted source record. Original payload and source IDs remain in curated.
select * from {{ source('curated', 'accepted_trips') }}
