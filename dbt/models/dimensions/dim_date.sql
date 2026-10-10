-- Grain: every actual accepted pickup date, including out-of-partition dates.
select distinct cast(pickup_at as date) as pickup_date,
    extract(year from pickup_at) as calendar_year,
    extract(month from pickup_at) as calendar_month,
    extract(day from pickup_at) as calendar_day
from {{ ref('stg_trips') }}
