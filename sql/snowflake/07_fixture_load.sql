-- USER RUN ONLY. Snowflake execution is UNVERIFIED.
USE ROLE MOBILITYOPS_ENGINEER;
USE WAREHOUSE MOBILITYOPS_WH;
-- Change this line only when deliberately moving to the isolated real database.
USE DATABASE MOBILITYOPS_FIXTURE;
ALTER SESSION SET TIMEZONE='America/New_York', STATEMENT_TIMEOUT_IN_SECONDS=600,
  TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6', QUERY_TAG='mobilityops_manual';

-- User uploads unchanged fixture under this exact prefix first.
SET artifact_id='19d61b096876bd9739a6d09a601f096d76f2186ffac87b9e281e087dc20668a8';
SET zone_sha='7d2f34d513df6285a2d2e1c861fe3012ae2ef9ccd6bbfc74fa5c4dcabb5e6db8';
SET dataset_id='fixture';
SET source_partition='2026-01';
SET attempt_id=UUID_STRING();
-- INSERT once after inspecting any existing active row. Snowflake standard-table keys
-- are not enforced: duplicate registration is an error, not an idempotency mechanism.
INSERT INTO CONTROL.SOURCE_ARTIFACTS
SELECT $dataset_id,$artifact_id,$source_partition,'fixture',12,NULL,NULL,NULL,
 'python_v1_half_up',$zone_sha,TRUE
WHERE NOT EXISTS(SELECT 1 FROM CONTROL.SOURCE_ARTIFACTS WHERE dataset_id=$dataset_id);
-- This zone CSV load is separate from the Stream and must be verified unique before promotion.
COPY INTO CURATED.ZONES(zone_key,borough,zone_name,service_zone,zone_sha256)
FROM (SELECT $1::NUMBER,$2,$3,$4,'7d2f34d513df6285a2d2e1c861fe3012ae2ef9ccd6bbfc74fa5c4dcabb5e6db8' FROM @RAW.SOURCE_STAGE/zones/)
FILES=('taxi_zone_lookup_fixture.csv') FILE_FORMAT=(FORMAT_NAME=RAW.ZONE_FORMAT)
ON_ERROR=ABORT_STATEMENT;
INSERT INTO CURATED.ZONES SELECT 0,'Unresolved','Unresolved','Unresolved',$zone_sha
WHERE NOT EXISTS(SELECT 1 FROM CURATED.ZONES WHERE zone_key=0);
COPY INTO RAW.TRIPS(payload,artifact_id,source_row_number,dataset_id,source_partition,
 attempt_id,stage_filename)
FROM (SELECT $1,$artifact_id,METADATA$FILE_ROW_NUMBER,$dataset_id,$source_partition,
 $attempt_id,METADATA$FILENAME FROM @RAW.SOURCE_STAGE/19d61b096876bd9739a6d09a601f096d76f2186ffac87b9e281e087dc20668a8/)
FILES=('yellow_taxi_fixture.parquet') FILE_FORMAT=(FORMAT_NAME=RAW.PARQUET_FORMAT)
ON_ERROR=ABORT_STATEMENT;
SET copy_query_id=LAST_QUERY_ID();
INSERT INTO CONTROL.LOAD_ATTEMPTS(attempt_id,dataset_id,artifact_id,copy_query_id,copy_result)
SELECT $attempt_id,$dataset_id,$artifact_id,$copy_query_id,OBJECT_CONSTRUCT(*)
FROM TABLE(RESULT_SCAN($copy_query_id));
-- Check these BEFORE promotion. No epoch-unit guessing is permitted.
SELECT source_row_number,TYPEOF(payload:tpep_pickup_datetime) AS timestamp_type,
 payload:tpep_pickup_datetime, payload FROM RAW.TRIPS ORDER BY source_row_number;
SELECT COUNT(*) physical_rows,COUNT(DISTINCT source_row_number) source_rows,
 MIN(source_row_number),MAX(source_row_number) FROM RAW.TRIPS;
SELECT COUNT(*) pending_rows FROM RAW.TRIPS_STREAM; -- Read-only inspection.
-- Expected: 12 source positions, 1..12; reviewed null pickup at 5 and null amount at 8.
-- Stop if timestamp representation or file metadata alignment does not match the fixture.
-- Step 9 in docs/snowflake_implementation.md explicitly calls CONTROL.PROMOTE(FALSE).
