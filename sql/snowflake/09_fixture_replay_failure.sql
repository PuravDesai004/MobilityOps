-- USER RUN ONLY. Snowflake execution is UNVERIFIED.
USE ROLE MOBILITYOPS_ENGINEER;
USE WAREHOUSE MOBILITYOPS_WH;
-- Change this line only when deliberately moving to the isolated real database.
USE DATABASE MOBILITYOPS_FIXTURE;
ALTER SESSION SET TIMEZONE='America/New_York', STATEMENT_TIMEOUT_IN_SECONDS=600,
  TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6', QUERY_TAG='mobilityops_manual';

ALTER TASK IF EXISTS CONTROL.PROMOTE_TASK SUSPEND;
-- Confirm CURRENT_DATABASE()='MOBILITYOPS_FIXTURE'. Never run FORCE on real source for this demo.
SET attempt_id=UUID_STRING();
COPY INTO RAW.TRIPS(payload,artifact_id,source_row_number,dataset_id,source_partition,
 attempt_id,stage_filename)
FROM (SELECT $1,'19d61b096876bd9739a6d09a601f096d76f2186ffac87b9e281e087dc20668a8',METADATA$FILE_ROW_NUMBER,'fixture','2026-01',
 $attempt_id,METADATA$FILENAME FROM @RAW.SOURCE_STAGE/19d61b096876bd9739a6d09a601f096d76f2186ffac87b9e281e087dc20668a8/)
FILES=('yellow_taxi_fixture.parquet') FILE_FORMAT=(FORMAT_NAME=RAW.PARQUET_FORMAT)
ON_ERROR=ABORT_STATEMENT FORCE=TRUE;
SELECT COUNT(*) FROM RAW.TRIPS; -- 24 physical, still 12 distinct source identities.
SELECT COUNT(*) FROM CONTROL.INGESTION_AUDIT; -- Save before the failed call.
CALL CONTROL.PROMOTE(TRUE); -- Expected controlled exception; STOP batch execution here.
-- In a separate execution after inspecting the error:
SELECT COUNT(*) FROM CURATED.ACCEPTED_TRIPS; -- Still 8.
SELECT COUNT(*) FROM CURATED.QUARANTINED_TRIPS; -- Still 4.
SELECT COUNT(*) FROM CONTROL.INGESTION_AUDIT; -- Unchanged from before failure.
SELECT COUNT(*) FROM WORK.INCOMING; -- Failed transaction's work absent.
SELECT COUNT(*) FROM RAW.TRIPS_STREAM; -- 12 pending source physical rows remain retryable.
CALL CONTROL.PROMOTE(FALSE);
SELECT * FROM CONTROL.INGESTION_AUDIT ORDER BY committed_at;
-- Retry commits with zero inserted accepted/quarantined keys and 12 replay keys.
