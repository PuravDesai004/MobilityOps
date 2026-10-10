-- Distinct record metrics are computed before exploding warning/reason arrays.
WITH accepted AS (
    SELECT source_partition AS source_month,
        count(DISTINCT (dataset_id, trip_record_id))::BIGINT AS accepted_trips,
        count(DISTINCT (dataset_id, trip_record_id)) FILTER (
            WHERE len(warning_codes) > 0
        )::BIGINT AS any_warning_trips
    FROM curated.accepted_trips GROUP BY source_partition
), quarantine AS (
    SELECT source_partition AS source_month,
        count(DISTINCT (dataset_id, trip_record_id))::BIGINT AS quarantined_trips
    FROM curated.quarantined_trips GROUP BY source_partition
), source AS (
    SELECT source_partition AS source_month, sum(source_rows)::BIGINT AS source_trips
    FROM control.source_artifacts GROUP BY source_partition
)
SELECT s.source_month, s.source_trips, coalesce(a.accepted_trips, 0) AS accepted_trips,
    coalesce(a.any_warning_trips, 0) AS any_warning_trips,
    100.0 * coalesce(a.any_warning_trips, 0) / nullif(a.accepted_trips, 0) AS warning_rate_pct,
    coalesce(q.quarantined_trips, 0) AS quarantined_trips,
    100.0 * coalesce(q.quarantined_trips, 0) / nullif(s.source_trips, 0) AS quarantine_rate_pct
FROM source s LEFT JOIN accepted a USING (source_month)
LEFT JOIN quarantine q USING (source_month) ORDER BY source_month;
