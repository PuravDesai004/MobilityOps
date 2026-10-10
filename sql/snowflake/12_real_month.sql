-- USER RUN ONLY. Snowflake execution is UNVERIFIED.
USE ROLE MOBILITYOPS_ENGINEER;
USE WAREHOUSE MOBILITYOPS_WH;
-- Change this line only when deliberately moving to the isolated real database.
USE DATABASE MOBILITYOPS_REAL;
ALTER SESSION SET TIMEZONE='America/New_York', STATEMENT_TIMEOUT_IN_SECONDS=600,
  TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6', QUERY_TAG='mobilityops_manual';

-- First create this database's tables, formats, Stream, UDF and procedure via files 02..06.
-- Upload official zone CSV once; same COPY pattern as file 07 with official zone hash.
-- Replace every placeholder from the local manifest and observed source row count.
SET artifact_id='<64_LOWERCASE_HEX_SHA256>';
SET dataset_id='real-yellow-taxi-2026-02';
SET source_partition='2026-02';
SET source_rows=<ACTUAL_MANIFEST_ROW_COUNT>;
SET zone_sha='<OFFICIAL_ZONE_SHA256>';
SET attempt_id=UUID_STRING();
-- Inspect active rows first; changed checksum for a dataset/month REQUIRES explicit review.
SELECT * FROM CONTROL.SOURCE_ARTIFACTS WHERE dataset_id=$dataset_id;
INSERT INTO CONTROL.SOURCE_ARTIFACTS SELECT $dataset_id,$artifact_id,$source_partition,'real',
 $source_rows,NULL,NULL,NULL,'python_v1_half_up',$zone_sha,TRUE
WHERE NOT EXISTS(SELECT 1 FROM CONTROL.SOURCE_ARTIFACTS WHERE dataset_id=$dataset_id);
-- Replace the literal prefix and filename; use a single explicit file, never a wildcard.
COPY INTO RAW.TRIPS(payload,artifact_id,source_row_number,dataset_id,source_partition,
 attempt_id,stage_filename)
FROM (SELECT $1,$artifact_id,METADATA$FILE_ROW_NUMBER,$dataset_id,$source_partition,
 $attempt_id,METADATA$FILENAME FROM @RAW.SOURCE_STAGE/<ARTIFACT_SHA256>/)
FILES=('yellow_tripdata_2026-02.parquet') FILE_FORMAT=(FORMAT_NAME=RAW.PARQUET_FORMAT)
ON_ERROR=ABORT_STATEMENT;
SET copy_query_id=LAST_QUERY_ID();
INSERT INTO CONTROL.LOAD_ATTEMPTS(attempt_id,dataset_id,artifact_id,copy_query_id,copy_result)
SELECT $attempt_id,$dataset_id,$artifact_id,$copy_query_id,OBJECT_CONSTRUCT(*)
FROM TABLE(RESULT_SCAN($copy_query_id));
SELECT COUNT(*),COUNT(DISTINCT source_row_number),MIN(source_row_number),MAX(source_row_number)
FROM RAW.TRIPS WHERE dataset_id=$dataset_id AND artifact_id=$artifact_id;
CALL CONTROL.PROMOTE(FALSE);
-- Run file 08 in the REAL database and your dbt Snowflake target after each artifact.
-- Suggested manual order: Feb, Mar, Jan backfill. Save fact counts and artifact totals each time.
