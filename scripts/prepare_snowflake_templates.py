"""Generate user-run SQL and a pure-policy ZIP locally. Never executes Snowflake."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pyarrow as pa

from mobilityops.classify import OUTPUT_SCHEMA
from mobilityops.fixture import fixture_rows
from mobilityops.identity import sha256_file
from mobilityops.policy import _json_value, classify_row

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql/snowflake"
HEADER = "-- USER RUN ONLY. Snowflake execution is UNVERIFIED.\n"
CONTEXT = """USE ROLE MOBILITYOPS_ENGINEER;
USE WAREHOUSE MOBILITYOPS_WH;
-- Change this line only when deliberately moving to the isolated real database.
USE DATABASE MOBILITYOPS_FIXTURE;
ALTER SESSION SET TIMEZONE='America/New_York', STATEMENT_TIMEOUT_IN_SECONDS=600,
  TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6', QUERY_TAG='mobilityops_manual';
"""


def write(name, content):
    (SQL / name).write_text(HEADER + content.strip() + "\n", encoding="utf-8")


def sql_type(field):
    t = field.type
    if pa.types.is_string(t):
        return "VARCHAR"
    if pa.types.is_integer(t):
        return "NUMBER(19,0)"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP_NTZ(6)"
    if pa.types.is_decimal(t):
        return f"NUMBER({t.precision},{t.scale})"
    if pa.types.is_floating(t):
        return "DOUBLE"
    if pa.types.is_list(t):
        return "ARRAY"
    raise ValueError(t)


def main():
    SQL.mkdir(parents=True, exist_ok=True)
    assets = SQL / "assets"
    assets.mkdir(exist_ok=True)
    policy_zip = assets / "mobilityops_policy.zip"
    with zipfile.ZipFile(policy_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("mobilityops/__init__.py", "")
        for name in ["identity.py", "policy.py"]:
            archive.write(ROOT / "src/mobilityops" / name, f"mobilityops/{name}")
    write(
        "00_inspect_account.sql",
        """
SELECT CURRENT_ACCOUNT(),CURRENT_USER(),CURRENT_ROLE(),CURRENT_WAREHOUSE(),CURRENT_DATABASE();
SHOW GRANTS TO USER <YOUR_USER_IDENTIFIER>;
SHOW WAREHOUSES;
-- Review the remaining trial/billing balance privately in Snowsight before proceeding.
-- Historical usage needs appropriate monitoring privileges and can have latency.
SELECT WAREHOUSE_NAME,START_TIME,CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(day,-7,CURRENT_TIMESTAMP()) ORDER BY START_TIME DESC;
""",
    )
    write(
        "01_bootstrap.sql",
        """
USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS MOBILITYOPS_ENGINEER;
GRANT ROLE MOBILITYOPS_ENGINEER TO ROLE SYSADMIN;
GRANT ROLE MOBILITYOPS_ENGINEER TO USER <YOUR_USER_IDENTIFIER>;
USE ROLE SYSADMIN;
CREATE WAREHOUSE IF NOT EXISTS MOBILITYOPS_WH WAREHOUSE_SIZE='XSMALL'
 AUTO_SUSPEND=60 AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE
 STATEMENT_TIMEOUT_IN_SECONDS=600;
GRANT USAGE,OPERATE ON WAREHOUSE MOBILITYOPS_WH TO ROLE MOBILITYOPS_ENGINEER;
CREATE DATABASE IF NOT EXISTS MOBILITYOPS_FIXTURE;
CREATE DATABASE IF NOT EXISTS MOBILITYOPS_REAL;
CREATE DATABASE IF NOT EXISTS MOBILITYOPS_FIXTURE_RECOVERY;
"""
        + "\n".join(
            f"""CREATE SCHEMA IF NOT EXISTS {db}.{schema};
GRANT USAGE ON DATABASE {db} TO ROLE MOBILITYOPS_ENGINEER;
GRANT ALL PRIVILEGES ON SCHEMA {db}.{schema} TO ROLE MOBILITYOPS_ENGINEER;"""
            for db in ["MOBILITYOPS_FIXTURE", "MOBILITYOPS_REAL", "MOBILITYOPS_FIXTURE_RECOVERY"]
            for schema in ["RAW", "CONTROL", "CURATED", "WORK", "ANALYTICS"]
        )
        + """
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE MOBILITYOPS_ENGINEER;
-- Optional: assign an existing, privately reviewed warehouse resource monitor yourself.
-- ALTER WAREHOUSE MOBILITYOPS_WH SET RESOURCE_MONITOR=<YOUR_EXISTING_MONITOR>;
""",
    )
    columns = ",\n".join(f"  {f.name} {sql_type(f)}" for f in OUTPUT_SCHEMA)
    write(
        "02_tables.sql",
        CONTEXT
        + """
CREATE TABLE IF NOT EXISTS RAW.TRIPS (
 raw_id VARCHAR DEFAULT UUID_STRING(), payload VARIANT, artifact_id VARCHAR NOT NULL,
 source_row_number NUMBER(19,0) NOT NULL, dataset_id VARCHAR NOT NULL,
 source_partition VARCHAR NOT NULL, attempt_id VARCHAR, stage_filename VARCHAR,
 loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP());
CREATE TABLE IF NOT EXISTS CONTROL.SOURCE_ARTIFACTS (
 dataset_id VARCHAR, artifact_id VARCHAR, source_partition VARCHAR, scope VARCHAR,
 source_rows NUMBER(19,0), accepted_rows NUMBER(19,0), quarantined_rows NUMBER(19,0),
 recorded_charge NUMBER(38,2), policy_version VARCHAR, zone_sha256 VARCHAR,
 active BOOLEAN DEFAULT TRUE);
CREATE TABLE IF NOT EXISTS CONTROL.LOAD_ATTEMPTS (
 attempt_id VARCHAR, dataset_id VARCHAR, artifact_id VARCHAR, copy_query_id VARCHAR,
 copy_result VARIANT, recorded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP());
CREATE TABLE IF NOT EXISTS CONTROL.INGESTION_AUDIT (
 attempt_id VARCHAR, dataset_id VARCHAR, artifact_id VARCHAR, accepted_inserted NUMBER,
 quarantined_inserted NUMBER, replay_records NUMBER, committed_at VARCHAR,
 elapsed_seconds DOUBLE, physical_incoming_rows NUMBER);
CREATE TABLE IF NOT EXISTS WORK.INCOMING (batch_id VARCHAR, raw_id VARCHAR, classified VARIANT);
CREATE TABLE IF NOT EXISTS CURATED.ZONES (
 zone_key NUMBER(19,0), borough VARCHAR, zone_name VARCHAR, service_zone VARCHAR,
 zone_sha256 VARCHAR);
"""
        + f"CREATE TABLE IF NOT EXISTS CURATED.ACCEPTED_TRIPS (\n{columns}\n);\n"
        + f"CREATE TABLE IF NOT EXISTS CURATED.QUARANTINED_TRIPS (\n{columns}\n);\n",
    )
    write(
        "03_stage_and_formats.sql",
        CONTEXT
        + """
CREATE STAGE IF NOT EXISTS RAW.SOURCE_STAGE;
CREATE FILE FORMAT IF NOT EXISTS RAW.PARQUET_FORMAT TYPE=PARQUET
 USE_LOGICAL_TYPE=TRUE USE_VECTORIZED_SCANNER=FALSE;
CREATE FILE FORMAT IF NOT EXISTS RAW.ZONE_FORMAT TYPE=CSV SKIP_HEADER=1
 FIELD_OPTIONALLY_ENCLOSED_BY='"' EMPTY_FIELD_AS_NULL=FALSE;
CREATE STAGE IF NOT EXISTS CONTROL.CODE_STAGE;
""",
    )
    write(
        "04_stream_before_load.sql",
        CONTEXT
        + """
-- Execute BEFORE the first RAW.TRIPS COPY. Never CREATE OR REPLACE an active stream.
CREATE STREAM IF NOT EXISTS RAW.TRIPS_STREAM ON TABLE RAW.TRIPS APPEND_ONLY=TRUE;
SHOW STREAMS LIKE 'TRIPS_STREAM' IN SCHEMA RAW;
-- If raw already exists, stop and follow the manifest/key rebuild runbook.
""",
    )
    write(
        "05_shared_classifier.sql",
        CONTEXT
        + """
-- First upload assets/mobilityops_policy.zip to CONTROL.CODE_STAGE using the manual guide.
-- The UDF imports the SAME pure Python classifier tested locally; no second SQL classifier.
CREATE OR REPLACE FUNCTION CONTROL.CLASSIFY_TRIP(
 payload VARIANT, artifact_id VARCHAR, row_number NUMBER(19,0), dataset_id VARCHAR,
 partition_month VARCHAR, zone_ids ARRAY)
RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.12'
IMPORTS=('@CONTROL.CODE_STAGE/mobilityops_policy.zip') HANDLER='classify'
AS $$
from datetime import datetime
from decimal import Decimal
from mobilityops.policy import classify_row
def classify(payload, artifact_id, row_number, dataset_id, partition_month, zone_ids):
    result = classify_row(payload, artifact_id=artifact_id, source_row_number=int(row_number),
        dataset_id=dataset_id, source_partition=partition_month,
        zone_ids={int(k) for k in zone_ids if k is not None})
    # Serialize already validated canonical values; SQL casts do no business classification.
    for key, value in list(result.items()):
        if isinstance(value, datetime): result[key] = value.isoformat()
        elif isinstance(value, Decimal): result[key] = str(value)
    return result
$$;
"""
        + "CREATE OR REPLACE VIEW WORK.CLASSIFIED AS SELECT batch_id, raw_id,\n"
        + ",\n".join(
            (
                f"TRY_TO_TIMESTAMP_NTZ(classified:{f.name}::VARCHAR) AS {f.name}"
                if pa.types.is_timestamp(f.type)
                else f"classified:{f.name}::{sql_type(f)} AS {f.name}"
            )
            for f in OUTPUT_SCHEMA
        )
        + "\nFROM WORK.INCOMING;\n",
    )
    fields = ",".join(OUTPUT_SCHEMA.names)
    values = ",".join(f"s.{name}" for name in OUTPUT_SCHEMA.names)
    merges = ""
    for table, predicate in [
        ("ACCEPTED_TRIPS", "ARRAY_SIZE(reason_codes)=0"),
        ("QUARANTINED_TRIPS", "ARRAY_SIZE(reason_codes)>0"),
    ]:
        merges += f"""MERGE INTO CURATED.{table} t USING (
 SELECT {fields} FROM WORK.CLASSIFIED WHERE batch_id=:v_batch AND {predicate}) s
 ON t.dataset_id=s.dataset_id AND t.trip_record_id=s.trip_record_id
 WHEN NOT MATCHED THEN INSERT ({fields}) VALUES ({values});\n"""
        if table == "ACCEPTED_TRIPS":
            merges += "IF (inject_failure) THEN RAISE controlled_failure; END IF;\n"
    write(
        "06_transactional_promotion.sql",
        CONTEXT
        + """
CREATE OR REPLACE PROCEDURE CONTROL.PROMOTE(inject_failure BOOLEAN DEFAULT FALSE)
RETURNS VARIANT LANGUAGE SQL EXECUTE AS OWNER AS
$$
DECLARE
 v_batch VARCHAR DEFAULT UUID_STRING();
 v_physical NUMBER; v_bad NUMBER;
 controlled_failure EXCEPTION(-20001,'Controlled fixture failure before commit');
 invalid_provenance EXCEPTION(-20002,'Invalid provenance, overlap or unreviewed artifact');
BEGIN
 IF (inject_failure AND CURRENT_DATABASE()
     NOT IN ('MOBILITYOPS_FIXTURE','MOBILITYOPS_FIXTURE_RECOVERY')) THEN
   RAISE invalid_provenance;
 END IF;
 BEGIN TRANSACTION;
 SELECT COUNT(*) INTO :v_physical FROM RAW.TRIPS_STREAM;
 SELECT COUNT(DISTINCT dataset_id||artifact_id) INTO :v_bad FROM RAW.TRIPS_STREAM;
 IF (v_bad>1) THEN RAISE invalid_provenance; END IF; -- One reviewed artifact per promotion.
 SELECT COUNT(*) INTO :v_bad FROM RAW.TRIPS_STREAM s WHERE
   NOT REGEXP_LIKE(s.artifact_id,'^[0-9a-f]{64}$') OR s.source_row_number<1 OR
   (SELECT COUNT(*) FROM CONTROL.SOURCE_ARTIFACTS a WHERE a.active AND
    a.dataset_id=s.dataset_id AND a.artifact_id=s.artifact_id AND
    a.source_partition=s.source_partition AND s.source_row_number<=a.source_rows)<>1;
 IF (v_bad>0) THEN RAISE invalid_provenance; END IF;
 SELECT COUNT(*) INTO :v_bad FROM (
  SELECT dataset_id,artifact_id,source_row_number FROM RAW.TRIPS_STREAM
  GROUP BY dataset_id,artifact_id,source_row_number HAVING COUNT(DISTINCT payload)>1);
 IF (v_bad>0) THEN RAISE invalid_provenance; END IF;
 SELECT COUNT(*) INTO :v_bad FROM CURATED.ZONES
  WHERE zone_key IS NULL OR zone_sha256 IS NULL;
 IF (v_bad>0) THEN RAISE invalid_provenance; END IF;
 SELECT COUNT(*) INTO :v_bad FROM (
  SELECT zone_key FROM CURATED.ZONES GROUP BY zone_key HAVING COUNT(*)<>1);
 IF (v_bad>0) THEN RAISE invalid_provenance; END IF;
 SELECT COUNT(*) INTO :v_bad FROM CONTROL.SOURCE_ARTIFACTS WHERE active AND
  (policy_version<>'python_v1_half_up' OR zone_sha256 IS NULL OR
   (SELECT COUNT(DISTINCT zone_sha256) FROM CURATED.ZONES)<>1 OR
   zone_sha256<>(SELECT MIN(zone_sha256) FROM CURATED.ZONES));
 IF (v_bad>0) THEN RAISE invalid_provenance; END IF;
 INSERT INTO WORK.INCOMING(batch_id,raw_id,classified)
 SELECT :v_batch,raw_id,CONTROL.CLASSIFY_TRIP(payload,artifact_id,source_row_number,
  dataset_id,source_partition,(SELECT ARRAY_AGG(zone_key) FROM CURATED.ZONES WHERE zone_key<>0))
 FROM RAW.TRIPS_STREAM WHERE METADATA$ACTION='INSERT'
 QUALIFY ROW_NUMBER() OVER(PARTITION BY dataset_id,artifact_id,source_row_number ORDER BY raw_id)=1;
 SELECT COUNT(*) INTO :v_bad FROM WORK.CLASSIFIED s WHERE batch_id=:v_batch AND
  (s.trip_record_id IS NULL OR s.trip_record_id<>SHA2(s.artifact_id||':'||s.source_row_number,256)
   OR (ARRAY_SIZE(s.reason_codes)=0 AND EXISTS(SELECT 1 FROM CURATED.QUARANTINED_TRIPS q
       WHERE q.dataset_id=s.dataset_id AND q.trip_record_id=s.trip_record_id))
   OR (ARRAY_SIZE(s.reason_codes)>0 AND EXISTS(SELECT 1 FROM CURATED.ACCEPTED_TRIPS a
       WHERE a.dataset_id=s.dataset_id AND a.trip_record_id=s.trip_record_id)));
 IF (v_bad>0) THEN RAISE invalid_provenance; END IF;
 SELECT COUNT(*) INTO :v_bad FROM WORK.CLASSIFIED s JOIN (
  SELECT dataset_id,trip_record_id,raw_payload_json FROM CURATED.ACCEPTED_TRIPS UNION ALL
  SELECT dataset_id,trip_record_id,raw_payload_json FROM CURATED.QUARANTINED_TRIPS) t
  ON s.dataset_id=t.dataset_id AND s.trip_record_id=t.trip_record_id
  WHERE s.batch_id=:v_batch AND
   PARSE_JSON(s.raw_payload_json)<>PARSE_JSON(t.raw_payload_json);
 IF (v_bad>0) THEN RAISE invalid_provenance; END IF;
 -- Save inserted/replayed counts BEFORE the merges, inside the same transaction.
 INSERT INTO CONTROL.INGESTION_AUDIT
 SELECT :v_batch,s.dataset_id,s.artifact_id,
  SUM(IFF(ARRAY_SIZE(s.reason_codes)=0 AND a.trip_record_id IS NULL,1,0)),
  SUM(IFF(ARRAY_SIZE(s.reason_codes)>0 AND q.trip_record_id IS NULL,1,0)),
  SUM(IFF(a.trip_record_id IS NOT NULL OR q.trip_record_id IS NOT NULL,1,0)),
  CURRENT_TIMESTAMP()::VARCHAR,NULL,:v_physical
 FROM WORK.CLASSIFIED s LEFT JOIN CURATED.ACCEPTED_TRIPS a
  ON a.dataset_id=s.dataset_id AND a.trip_record_id=s.trip_record_id
 LEFT JOIN CURATED.QUARANTINED_TRIPS q
  ON q.dataset_id=s.dataset_id AND q.trip_record_id=s.trip_record_id
 WHERE batch_id=:v_batch GROUP BY s.dataset_id,s.artifact_id;
"""
        + merges
        + """
 UPDATE CONTROL.SOURCE_ARTIFACTS a SET
  accepted_rows=(SELECT COUNT(*) FROM CURATED.ACCEPTED_TRIPS t
   WHERE t.dataset_id=a.dataset_id AND t.artifact_id=a.artifact_id),
  quarantined_rows=(SELECT COUNT(*) FROM CURATED.QUARANTINED_TRIPS t
   WHERE t.dataset_id=a.dataset_id AND t.artifact_id=a.artifact_id),
  recorded_charge=(SELECT COALESCE(SUM(recorded_charge),0) FROM CURATED.ACCEPTED_TRIPS t
   WHERE t.dataset_id=a.dataset_id AND t.artifact_id=a.artifact_id)
 WHERE active;
 SELECT COUNT(*) INTO :v_bad FROM CONTROL.SOURCE_ARTIFACTS
  WHERE active AND source_rows<>accepted_rows+quarantined_rows;
 IF (v_bad>0) THEN RAISE invalid_provenance; END IF;
 DELETE FROM WORK.INCOMING WHERE batch_id=:v_batch;
 COMMIT;
 RETURN OBJECT_CONSTRUCT('status','COMMITTED','batch_id',v_batch,'physical_rows',v_physical);
EXCEPTION WHEN OTHER THEN
 ROLLBACK;
 RAISE;
END;
$$;
""",
    )
    fixture_sha = sha256_file(ROOT / "tests/fixtures/yellow_taxi_fixture.parquet")
    zone_sha = sha256_file(ROOT / "tests/fixtures/taxi_zone_lookup_fixture.csv")
    write(
        "07_fixture_load.sql",
        CONTEXT
        + f"""
-- User uploads unchanged fixture under this exact prefix first.
SET artifact_id='{fixture_sha}';
SET zone_sha='{zone_sha}';
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
FROM (SELECT $1::NUMBER,$2,$3,$4,'{zone_sha}' FROM @RAW.SOURCE_STAGE/zones/)
FILES=('taxi_zone_lookup_fixture.csv') FILE_FORMAT=(FORMAT_NAME=RAW.ZONE_FORMAT)
ON_ERROR=ABORT_STATEMENT;
INSERT INTO CURATED.ZONES SELECT 0,'Unresolved','Unresolved','Unresolved',$zone_sha
WHERE NOT EXISTS(SELECT 1 FROM CURATED.ZONES WHERE zone_key=0);
COPY INTO RAW.TRIPS(payload,artifact_id,source_row_number,dataset_id,source_partition,
 attempt_id,stage_filename)
FROM (SELECT $1,$artifact_id,METADATA$FILE_ROW_NUMBER,$dataset_id,$source_partition,
 $attempt_id,METADATA$FILENAME FROM @RAW.SOURCE_STAGE/{fixture_sha}/)
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
""",
    )
    write(
        "08_verify.sql",
        CONTEXT
        + """
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
""",
    )
    write(
        "09_fixture_replay_failure.sql",
        CONTEXT
        + f"""
ALTER TASK IF EXISTS CONTROL.PROMOTE_TASK SUSPEND;
-- Confirm CURRENT_DATABASE()='MOBILITYOPS_FIXTURE'. Never run FORCE on real source for this demo.
SET attempt_id=UUID_STRING();
COPY INTO RAW.TRIPS(payload,artifact_id,source_row_number,dataset_id,source_partition,
 attempt_id,stage_filename)
FROM (SELECT $1,'{fixture_sha}',METADATA$FILE_ROW_NUMBER,'fixture','2026-01',
 $attempt_id,METADATA$FILENAME FROM @RAW.SOURCE_STAGE/{fixture_sha}/)
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
""",
    )
    write(
        "10_task.sql",
        CONTEXT
        + """
CREATE TASK IF NOT EXISTS CONTROL.PROMOTE_TASK WAREHOUSE=MOBILITYOPS_WH
 SCHEDULE='5 MINUTES' ALLOW_OVERLAPPING_EXECUTION=FALSE USER_TASK_TIMEOUT_MS=600000
 WHEN SYSTEM$STREAM_HAS_DATA('RAW.TRIPS_STREAM') AS CALL CONTROL.PROMOTE(FALSE);
ALTER TASK CONTROL.PROMOTE_TASK SUSPEND;
SHOW TASKS LIKE 'PROMOTE_TASK' IN SCHEMA CONTROL;
-- At your own supervised session, after ordinary procedure verification:
-- EXECUTE TASK CONTROL.PROMOTE_TASK;
-- SELECT NAME,STATE,QUERY_ID,ERROR_MESSAGE,SCHEDULED_TIME,COMPLETED_TIME
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME=>'PROMOTE_TASK',RESULT_LIMIT=>10));
-- A run needs pending rows for the WHEN condition. Use one controlled fixture reload first.
-- ALTER TASK CONTROL.PROMOTE_TASK SUSPEND;
""",
    )
    write(
        "12_real_month.sql",
        CONTEXT.replace("MOBILITYOPS_FIXTURE", "MOBILITYOPS_REAL")
        + """
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
""",
    )
    write(
        "13_export.sql",
        CONTEXT
        + """
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
""",
    )
    write(
        "12a_real_zones.sql",
        CONTEXT.replace("MOBILITYOPS_FIXTURE", "MOBILITYOPS_REAL")
        + """
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
""",
    )
    write(
        "14_suspend_and_inspect.sql",
        CONTEXT
        + """
ALTER TASK IF EXISTS CONTROL.PROMOTE_TASK SUSPEND;
ALTER TASK IF EXISTS MOBILITYOPS_FIXTURE.CONTROL.PROMOTE_TASK SUSPEND;
ALTER TASK IF EXISTS MOBILITYOPS_REAL.CONTROL.PROMOTE_TASK SUSPEND;
SHOW TASKS LIKE 'PROMOTE_TASK' IN SCHEMA CONTROL;
SHOW STREAMS LIKE 'TRIPS_STREAM' IN SCHEMA RAW;
-- Save STALE_AFTER; suspension does not freeze retention.
SELECT NAME,STATE,QUERY_ID,ERROR_MESSAGE FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
 TASK_NAME=>'PROMOTE_TASK',RESULT_LIMIT=>20));
ALTER WAREHOUSE MOBILITYOPS_WH SUSPEND;
SHOW WAREHOUSES LIKE 'MOBILITYOPS_WH';
-- Repeat Task suspension in both fixture and real databases if both Tasks were created.
-- Inspect warehouse metering with your monitoring role; do not issue broad DROP statements.
""",
    )
    # Complete explicit contexts avoid accidentally bootstrapping real tables in fixture scope.
    for scope, database in [
        ("real", "MOBILITYOPS_REAL"),
        ("recovery", "MOBILITYOPS_FIXTURE_RECOVERY"),
    ]:
        directory = SQL / scope
        directory.mkdir(exist_ok=True)
        for name in (
            "02_tables.sql",
            "03_stage_and_formats.sql",
            "04_stream_before_load.sql",
            "05_shared_classifier.sql",
            "06_transactional_promotion.sql",
        ):
            (directory / name).write_text(
                (SQL / name)
                .read_text()
                .replace("USE DATABASE MOBILITYOPS_FIXTURE;", f"USE DATABASE {database};"),
                encoding="utf-8",
            )
    recovery_load = (
        (SQL / "07_fixture_load.sql")
        .read_text()
        .replace("USE DATABASE MOBILITYOPS_FIXTURE;", "USE DATABASE MOBILITYOPS_FIXTURE_RECOVERY;")
    )
    recovery_load += (
        "\n-- First promotion is intentionally failed in recovery/08_fail_then_retry.sql.\n"
    )
    (SQL / "recovery/07_load_before_failure.sql").write_text(recovery_load, encoding="utf-8")
    (SQL / "recovery/08_fail_then_retry.sql").write_text(
        HEADER
        + """
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
""",
        encoding="utf-8",
    )
    vectors = []
    base = fixture_rows()[0]
    for label, row in [(f"fixture_row_{i}", r) for i, r in enumerate(fixture_rows(), 1)] + [
        ("positive_half_cent", base | {"total_amount": "1.005"}),
        ("negative_half_cent", base | {"total_amount": "-1.005"}),
        ("rounding_overflow", base | {"total_amount": "9999999999999999.995"}),
        ("nonfinite_required", base | {"trip_distance": "NaN"}),
        ("multiple_reasons", base | {"tpep_pickup_datetime": "bad", "total_amount": None}),
        ("negative_before_rounding", base | {"trip_distance": "-0.00001"}),
        ("official_unknown_member", base | {"PULocationID": 264}),
    ]:
        # JSON-native payloads make these vectors directly runnable through PARSE_JSON.
        payload = {
            key: value.isoformat() if hasattr(value, "isoformat") else value
            for key, value in row.items()
        }
        result = classify_row(
            payload,
            artifact_id="a" * 64,
            source_row_number=1,
            dataset_id="fixture",
            source_partition="2026-01",
            zone_ids={132, 138, 161, 162, 264, 265},
        )
        vectors.append({"label": label, "payload": payload, "expected": _json_value(result)})
    vector_values = []
    for vector in vectors:
        expected = {
            key: vector["expected"][key]
            for key in ("reason_codes", "warning_codes", "pickup_zone_key", "dropoff_zone_key")
        }
        expected["recorded_charge"] = (
            vector["expected"]["recorded_charge"]["value"]
            if vector["expected"]["recorded_charge"] is not None
            else None
        )
        escaped = [
            json.dumps(value, separators=(",", ":")).replace("'", "''")
            for value in (vector["payload"], expected)
        ]
        vector_values.append(f"('{vector['label']}','{escaped[0]}','{escaped[1]}')")
    write(
        "08a_policy_vectors.sql",
        CONTEXT
        + """
-- Manual execution only. Zero returned mismatches expected; this is not account verification yet.
WITH vectors AS (
 SELECT column1::VARCHAR label,PARSE_JSON(column2::VARCHAR) payload,
 PARSE_JSON(column3::VARCHAR) expected FROM VALUES
"""
        + ",\n".join(vector_values)
        + """
), actual AS (
 SELECT label,expected,CONTROL.CLASSIFY_TRIP(payload,REPEAT('a',64),1,'fixture','2026-01',
 ARRAY_CONSTRUCT(132,138,161,162,264,265)) result FROM vectors
)
SELECT label,result,expected FROM actual WHERE
 TO_JSON(result:reason_codes)<>TO_JSON(expected:reason_codes) OR
 TO_JSON(result:warning_codes)<>TO_JSON(expected:warning_codes) OR
 result:pickup_zone_key::NUMBER<>expected:pickup_zone_key::NUMBER OR
 result:dropoff_zone_key::NUMBER<>expected:dropoff_zone_key::NUMBER OR
 NOT EQUAL_NULL(result:recorded_charge::VARCHAR,expected:recorded_charge::VARCHAR);
""",
    )
    (assets / "classification_vectors.json").write_text(json.dumps(vectors, indent=2) + "\n")
    (assets / "policy_package_receipt.json").write_text(
        json.dumps(
            {
                "sha256": sha256_file(policy_zip),
                "classifier": "src/mobilityops/policy.py",
                "snowflake_execution": "UNVERIFIED",
            },
            indent=2,
        )
        + "\n"
    )
    print("Prepared manual Snowflake SQL and shared-policy assets. No account operation occurred.")


if __name__ == "__main__":
    main()
