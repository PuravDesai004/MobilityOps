-- USER RUN ONLY. Snowflake execution is UNVERIFIED.
USE ROLE MOBILITYOPS_ENGINEER;
USE WAREHOUSE MOBILITYOPS_WH;
-- Change this line only when deliberately moving to the isolated real database.
USE DATABASE MOBILITYOPS_FIXTURE;
ALTER SESSION SET TIMEZONE='America/New_York', STATEMENT_TIMEOUT_IN_SECONDS=600,
  TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6', QUERY_TAG='mobilityops_manual';

SELECT 'accepted' disposition,COUNT(*) n,SUM(recorded_charge) charge FROM CURATED.ACCEPTED_TRIPS
UNION ALL SELECT 'quarantined',COUNT(*),NULL FROM CURATED.QUARANTINED_TRIPS;
-- Fixture expectations: accepted=8, quarantined=4, accepted charge=134.00.
SELECT source_row_number,reason_codes FROM CURATED.QUARANTINED_TRIPS ORDER BY source_row_number;
SELECT source_row_number,pickup_zone_source_id,pickup_zone_key,warning_codes,recorded_charge
FROM CURATED.ACCEPTED_TRIPS ORDER BY source_row_number;
SELECT trip_record_id,COUNT(*) FROM (
 SELECT trip_record_id FROM CURATED.ACCEPTED_TRIPS UNION ALL
 SELECT trip_record_id FROM CURATED.QUARANTINED_TRIPS) GROUP BY trip_record_id HAVING COUNT(*)<>1;
SELECT a.trip_record_id FROM CURATED.ACCEPTED_TRIPS a JOIN CURATED.QUARANTINED_TRIPS q
 ON a.dataset_id=q.dataset_id AND a.trip_record_id=q.trip_record_id;
SELECT * FROM CONTROL.SOURCE_ARTIFACTS WHERE source_rows<>accepted_rows+quarantined_rows;
SELECT * FROM CONTROL.INGESTION_AUDIT ORDER BY committed_at;
SELECT zone_key,COUNT(*) FROM CURATED.ZONES GROUP BY zone_key HAVING COUNT(*)<>1;
