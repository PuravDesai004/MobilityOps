-- Grain: source month x blocking reason. Multi-reason records can overlap between codes.
SELECT q.source_partition AS source_month, u.code,
    count(DISTINCT (q.dataset_id, q.trip_record_id))::BIGINT AS quarantined_trips
FROM curated.quarantined_trips q, unnest(q.reason_codes) AS u(code)
GROUP BY q.source_partition, u.code ORDER BY source_month, code;
