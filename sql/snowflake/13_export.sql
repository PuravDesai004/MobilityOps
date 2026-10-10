-- USER RUN ONLY. Snowflake execution is UNVERIFIED.
USE ROLE MOBILITYOPS_ENGINEER;
USE WAREHOUSE MOBILITYOPS_WH;
-- Change this line only when deliberately moving to the isolated real database.
USE DATABASE MOBILITYOPS_FIXTURE;
ALTER SESSION SET TIMEZONE='America/New_York', STATEMENT_TIMEOUT_IN_SECONDS=600,
  TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6', QUERY_TAG='mobilityops_manual';

-- Run AFTER your dbt build/tests. Export reviewed SELECT results yourself.
SELECT m.*,z.zone_name,z.borough FROM ANALYTICS.MART_ZONE_HOUR m JOIN ANALYTICS.DIM_ZONE z
 ON m.pickup_zone_key=z.zone_key ORDER BY dataset_id,pickup_date,pickup_hour,pickup_zone_key;
SELECT * FROM ANALYTICS.MART_DAILY_RECONCILIATION
 ORDER BY dataset_id,artifact_id,pickup_date_bucket;
SELECT * FROM CONTROL.SOURCE_ARTIFACTS;
SELECT * FROM CONTROL.INGESTION_AUDIT ORDER BY committed_at;
SELECT a.artifact_id,COUNT(*) accepted,SUM(a.recorded_charge) charge,
 (SELECT COUNT(*) FROM ANALYTICS.FCT_TRIP f WHERE f.artifact_id=a.artifact_id) facts
FROM CURATED.ACCEPTED_TRIPS a GROUP BY a.artifact_id;
SELECT r.artifact_id,COUNT(*) physical,COUNT(DISTINCT r.source_row_number) source_rows
FROM RAW.TRIPS r GROUP BY r.artifact_id;
-- Export this exact shape as artifact_metrics.csv, with CSV headers enabled.
SELECT c.dataset_id AS "dataset_id",c.artifact_id AS "artifact_id",
 c.source_rows AS "source_rows",c.accepted_rows AS "accepted_rows",
 c.quarantined_rows AS "quarantined_rows",
 (SELECT COUNT(*) FROM ANALYTICS.FCT_TRIP f WHERE f.dataset_id=c.dataset_id
  AND f.artifact_id=c.artifact_id) AS "fact_rows",
 (SELECT SUM(trip_count) FROM ANALYTICS.MART_ZONE_HOUR m WHERE m.dataset_id=c.dataset_id
  AND m.artifact_id=c.artifact_id) AS "mart_trip_count",
 c.recorded_charge AS "recorded_charge_usd"
FROM CONTROL.SOURCE_ARTIFACTS c WHERE c.active ORDER BY c.dataset_id;
-- Compare counts and signed USD charge to reports/local/real/<month>/verification.json.
