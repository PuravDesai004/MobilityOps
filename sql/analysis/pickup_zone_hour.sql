-- All accepted rows in the isolated database, including out-of-partition pickups.
-- Grain: pickup zone. Peak-hour ties choose the earliest New York wall-clock hour.
WITH hourly AS (
    SELECT pickup_zone_key, pickup_hour, sum(trip_count)::BIGINT AS hour_trips,
        sum(recorded_charge_sum) AS charge_usd,
        sum(duration_seconds_sum) AS duration_seconds,
        sum(duration_count) AS duration_count
    FROM analytics.mart_zone_hour
    GROUP BY pickup_zone_key, pickup_hour
), zones AS (
    SELECT pickup_zone_key, sum(hour_trips)::BIGINT AS accepted_trips,
        sum(charge_usd) AS recorded_charge_usd,
        sum(duration_seconds) / nullif(sum(duration_count), 0) / 60 AS avg_duration_minutes
    FROM hourly GROUP BY pickup_zone_key
), peaks AS (
    SELECT *, row_number() OVER (
        PARTITION BY pickup_zone_key ORDER BY hour_trips DESC, pickup_hour
    ) AS hour_rank FROM hourly
)
SELECT row_number() OVER (ORDER BY z.accepted_trips DESC, z.pickup_zone_key) AS volume_rank,
    z.pickup_zone_key, d.zone_name, d.borough, z.accepted_trips,
    100.0 * z.accepted_trips / nullif(sum(z.accepted_trips) OVER (), 0) AS accepted_share_pct,
    p.pickup_hour AS peak_pickup_hour, p.hour_trips AS peak_hour_trips,
    100.0 * p.hour_trips / nullif(z.accepted_trips, 0) AS peak_hour_zone_share_pct,
    z.recorded_charge_usd, z.avg_duration_minutes
FROM zones z JOIN analytics.dim_zone d ON z.pickup_zone_key = d.zone_key
JOIN peaks p ON z.pickup_zone_key = p.pickup_zone_key AND p.hour_rank = 1
ORDER BY volume_rank;
