-- USER RUN ONLY. Snowflake execution is UNVERIFIED.
USE ROLE MOBILITYOPS_ENGINEER;
USE WAREHOUSE MOBILITYOPS_WH;
-- Change this line only when deliberately moving to the isolated real database.
USE DATABASE MOBILITYOPS_FIXTURE_RECOVERY;
ALTER SESSION SET TIMEZONE='America/New_York', STATEMENT_TIMEOUT_IN_SECONDS=600,
  TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6', QUERY_TAG='mobilityops_manual';

-- Execute BEFORE the first RAW.TRIPS COPY. Never CREATE OR REPLACE an active stream.
CREATE STREAM IF NOT EXISTS RAW.TRIPS_STREAM ON TABLE RAW.TRIPS APPEND_ONLY=TRUE;
SHOW STREAMS LIKE 'TRIPS_STREAM' IN SCHEMA RAW;
-- If raw already exists, stop and follow the manifest/key rebuild runbook.
