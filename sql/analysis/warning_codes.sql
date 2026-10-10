-- Grain: source month x warning code. One record can appear under several codes.
WITH codes AS (
    SELECT a.source_partition AS source_month, u.code,
        count(DISTINCT (a.dataset_id, a.trip_record_id))::BIGINT AS warning_trips
    FROM curated.accepted_trips a, unnest(a.warning_codes) AS u(code)
    GROUP BY a.source_partition, u.code
), denominators AS (
    SELECT source_partition AS source_month, sum(accepted_rows)::BIGINT AS accepted_trips
    FROM control.source_artifacts GROUP BY source_partition
)
SELECT c.*, d.accepted_trips,
    100.0 * c.warning_trips / nullif(d.accepted_trips, 0) AS warning_rate_pct
FROM codes c JOIN denominators d USING (source_month) ORDER BY source_month, code;
