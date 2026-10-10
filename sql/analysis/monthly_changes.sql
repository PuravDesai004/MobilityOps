-- Grain: source-file month, not actual pickup month. Keep every accepted row in totals.
-- Comparable per-calendar-day measures exclude pickups outside their own file month.
WITH monthly AS (
    SELECT a.source_partition AS source_month,
        strptime(a.source_partition || '-01', '%Y-%m-%d')::DATE AS month_start,
        count(f.trip_record_id)::BIGINT AS accepted_trips,
        coalesce(sum(f.recorded_charge), 0)::DECIMAL(38,2) AS recorded_charge_usd,
        count(f.trip_record_id) FILTER (
            WHERE strftime(f.pickup_date, '%Y-%m') = a.source_partition
        )::BIGINT AS in_partition_trips,
        coalesce(sum(f.recorded_charge) FILTER (
            WHERE strftime(f.pickup_date, '%Y-%m') = a.source_partition
        ), 0)::DECIMAL(38,2) AS in_partition_charge_usd,
        count(DISTINCT f.pickup_date) FILTER (
            WHERE strftime(f.pickup_date, '%Y-%m') = a.source_partition
        )::BIGINT AS observed_in_partition_days,
        min(f.pickup_date) AS actual_pickup_min, max(f.pickup_date) AS actual_pickup_max
    FROM control.source_artifacts a LEFT JOIN analytics.fct_trip f
        USING (dataset_id, artifact_id, source_partition)
    GROUP BY a.source_partition
), measures AS (
    SELECT *, day(last_day(month_start)) AS calendar_days,
        accepted_trips - in_partition_trips AS excluded_out_of_partition_trips,
        recorded_charge_usd - in_partition_charge_usd AS excluded_charge_usd,
        recorded_charge_usd / nullif(accepted_trips, 0) AS avg_charge_usd,
        in_partition_trips * 1.0 / day(last_day(month_start)) AS trips_per_calendar_day,
        in_partition_trips * 1.0 / nullif(observed_in_partition_days, 0)
            AS trips_per_observed_day
    FROM monthly
), previous AS (
    SELECT *, lag(source_month) OVER w AS previous_source_month,
        lag(month_start) OVER w AS previous_month_start,
        lag(accepted_trips) OVER w AS previous_trips,
        lag(recorded_charge_usd) OVER w AS previous_charge,
        lag(avg_charge_usd) OVER w AS previous_avg_charge,
        lag(trips_per_calendar_day) OVER w AS previous_trips_per_day
    FROM measures WINDOW w AS (ORDER BY month_start)
)
SELECT source_month, accepted_trips, recorded_charge_usd, avg_charge_usd, calendar_days,
    observed_in_partition_days, actual_pickup_min, actual_pickup_max, in_partition_trips,
    in_partition_charge_usd, excluded_out_of_partition_trips, excluded_charge_usd,
    trips_per_calendar_day, trips_per_observed_day, previous_source_month,
    CASE WHEN date_diff('month', previous_month_start, month_start) = 1
        THEN accepted_trips - previous_trips END AS trips_change,
    CASE WHEN date_diff('month', previous_month_start, month_start) = 1
        THEN 100.0 * (accepted_trips - previous_trips) / nullif(previous_trips, 0)
        END AS trips_change_pct,
    CASE WHEN date_diff('month', previous_month_start, month_start) = 1
        THEN recorded_charge_usd - previous_charge END AS charge_change_usd,
    CASE WHEN date_diff('month', previous_month_start, month_start) = 1
        THEN 100.0 * (recorded_charge_usd - previous_charge) / nullif(previous_charge, 0)
        END AS charge_change_pct,
    CASE WHEN date_diff('month', previous_month_start, month_start) = 1
        THEN avg_charge_usd - previous_avg_charge END AS avg_charge_change_usd,
    CASE WHEN date_diff('month', previous_month_start, month_start) = 1
        THEN 100.0 * (avg_charge_usd - previous_avg_charge) / nullif(previous_avg_charge, 0)
        END AS avg_charge_change_pct,
    CASE WHEN date_diff('month', previous_month_start, month_start) = 1
        THEN trips_per_calendar_day - previous_trips_per_day END AS trips_per_day_change,
    CASE WHEN date_diff('month', previous_month_start, month_start) = 1
        THEN 100.0 * (trips_per_calendar_day - previous_trips_per_day)
            / nullif(previous_trips_per_day, 0) END AS trips_per_day_change_pct
FROM previous ORDER BY source_month;
