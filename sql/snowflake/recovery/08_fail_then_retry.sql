-- USER RUN ONLY. Snowflake execution is UNVERIFIED.

USE ROLE MOBILITYOPS_ENGINEER;
USE WAREHOUSE MOBILITYOPS_WH;
USE DATABASE MOBILITYOPS_FIXTURE_RECOVERY;
-- Fresh isolated scope: raw=12, pending=12, targets=0, audit=0 before this call.
CALL CONTROL.PROMOTE(TRUE); -- Select/run this statement alone; expected controlled exception.
-- Select/run these statements separately after observing that exception.
SELECT COUNT(*) FROM CURATED.ACCEPTED_TRIPS; -- 0, accepted insert rolled back.
SELECT COUNT(*) FROM CURATED.QUARANTINED_TRIPS; -- 0.
SELECT COUNT(*) FROM CONTROL.INGESTION_AUDIT; -- 0.
SELECT COUNT(*) FROM WORK.INCOMING; -- 0.
SELECT COUNT(*) FROM RAW.TRIPS_STREAM; -- 12, pending offset did not commit.
CALL CONTROL.PROMOTE(FALSE); -- Retry.
SELECT COUNT(*),SUM(recorded_charge) FROM CURATED.ACCEPTED_TRIPS; -- 8,134.00.
SELECT COUNT(*) FROM CURATED.QUARANTINED_TRIPS; -- 4.
SELECT * FROM CONTROL.INGESTION_AUDIT; -- One committed audit with 8/4 insertions.
SELECT COUNT(*) FROM WORK.INCOMING; -- 0.
SELECT COUNT(*) FROM RAW.TRIPS_STREAM; -- 0.
