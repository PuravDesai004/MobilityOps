-- Grain: source-file month x actual pickup month. No date exclusions here.
SELECT source_partition AS source_month, strftime(pickup_date, '%Y-%m') AS pickup_month,
    count(*)::BIGINT AS accepted_trips, sum(recorded_charge) AS recorded_charge_usd
FROM analytics.fct_trip
GROUP BY source_partition, strftime(pickup_date, '%Y-%m')
ORDER BY source_month, pickup_month;
