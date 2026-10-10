-- USER RUN ONLY. Snowflake execution is UNVERIFIED.
USE ROLE MOBILITYOPS_ENGINEER;
USE WAREHOUSE MOBILITYOPS_WH;
-- Change this line only when deliberately moving to the isolated real database.
USE DATABASE MOBILITYOPS_REAL;
ALTER SESSION SET TIMEZONE='America/New_York', STATEMENT_TIMEOUT_IN_SECONDS=600,
  TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6', QUERY_TAG='mobilityops_manual';

-- Upload the immutable official lookup first. Run once, inspect any existing data.
SET zone_sha='1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed';
SELECT COUNT(*),COUNT(DISTINCT zone_key) FROM CURATED.ZONES;
COPY INTO CURATED.ZONES(zone_key,borough,zone_name,service_zone,zone_sha256)
FROM (SELECT $1::NUMBER,$2,$3,$4,
 '1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed'
 FROM @RAW.SOURCE_STAGE/zones/)
FILES=('taxi_zone_lookup.csv') FILE_FORMAT=(FORMAT_NAME=RAW.ZONE_FORMAT)
ON_ERROR=ABORT_STATEMENT;
INSERT INTO CURATED.ZONES SELECT 0,'Unresolved','Unresolved','Unresolved',$zone_sha
WHERE NOT EXISTS(SELECT 1 FROM CURATED.ZONES WHERE zone_key=0);
SELECT COUNT(*),COUNT(DISTINCT zone_key) FROM CURATED.ZONES; -- Expected 266/266.
SELECT * FROM CURATED.ZONES WHERE zone_key IN (0,264,265);
