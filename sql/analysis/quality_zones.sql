-- Grain: source month x pickup zone. Keep unresolved 0 and official Unknown members.
-- Flags require >= 10,000 records in the relevant denominator and exceed the month rate.
WITH records AS (
    SELECT source_partition AS source_month, pickup_zone_key, 1 AS accepted,
        CASE WHEN len(warning_codes) > 0 THEN 1 ELSE 0 END AS warned, 0 AS quarantined
    FROM curated.accepted_trips
    UNION ALL
    SELECT source_partition, pickup_zone_key, 0, 0, 1 FROM curated.quarantined_trips
), groups AS (
    SELECT source_month, pickup_zone_key, count(*)::BIGINT AS source_trips,
        sum(accepted)::BIGINT AS accepted_trips, sum(warned)::BIGINT AS any_warning_trips,
        sum(quarantined)::BIGINT AS quarantined_trips
    FROM records GROUP BY source_month, pickup_zone_key
), rates AS (
    SELECT *, 100.0 * any_warning_trips / nullif(accepted_trips, 0) AS warning_rate_pct,
        100.0 * quarantined_trips / nullif(source_trips, 0) AS quarantine_rate_pct,
        100.0 * sum(any_warning_trips) OVER w / nullif(sum(accepted_trips) OVER w, 0)
            AS month_warning_rate_pct,
        100.0 * sum(quarantined_trips) OVER w / nullif(sum(source_trips) OVER w, 0)
            AS month_quarantine_rate_pct
    FROM groups WINDOW w AS (PARTITION BY source_month)
)
SELECT r.*, z.zone_name, z.borough, 10000 AS minimum_volume,
    accepted_trips >= 10000 AND warning_rate_pct > month_warning_rate_pct
        AS elevated_warning_rate,
    source_trips >= 10000 AND quarantine_rate_pct > month_quarantine_rate_pct
        AS elevated_quarantine_rate
FROM rates r JOIN analytics.dim_zone z ON r.pickup_zone_key = z.zone_key
ORDER BY source_month, warning_rate_pct DESC NULLS LAST, pickup_zone_key;
