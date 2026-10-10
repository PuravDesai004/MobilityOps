-- USER RUN ONLY. Snowflake execution is UNVERIFIED.
USE ROLE MOBILITYOPS_ENGINEER;
USE WAREHOUSE MOBILITYOPS_WH;
-- Change this line only when deliberately moving to the isolated real database.
USE DATABASE MOBILITYOPS_REAL;
ALTER SESSION SET TIMEZONE='America/New_York', STATEMENT_TIMEOUT_IN_SECONDS=600,
  TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6', QUERY_TAG='mobilityops_manual';

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
CREATE TABLE IF NOT EXISTS CURATED.ACCEPTED_TRIPS (
  dataset_id VARCHAR,
  artifact_id VARCHAR,
  source_partition VARCHAR,
  source_row_number NUMBER(19,0),
  trip_record_id VARCHAR,
  parent_artifact_id VARCHAR,
  parent_source_row_number NUMBER(19,0),
  pickup_at TIMESTAMP_NTZ(6),
  dropoff_at TIMESTAMP_NTZ(6),
  pickup_zone_source_id NUMBER(19,0),
  dropoff_zone_source_id NUMBER(19,0),
  pickup_zone_key NUMBER(19,0),
  dropoff_zone_key NUMBER(19,0),
  passenger_count NUMBER(19,0),
  payment_type NUMBER(19,0),
  distance_miles NUMBER(18,3),
  recorded_charge NUMBER(18,2),
  fare_amount NUMBER(18,2),
  tip_amount NUMBER(18,2),
  cbd_congestion_fee NUMBER(18,2),
  duration_seconds DOUBLE,
  warning_codes ARRAY,
  reason_codes ARRAY,
  raw_payload_json VARCHAR
);
CREATE TABLE IF NOT EXISTS CURATED.QUARANTINED_TRIPS (
  dataset_id VARCHAR,
  artifact_id VARCHAR,
  source_partition VARCHAR,
  source_row_number NUMBER(19,0),
  trip_record_id VARCHAR,
  parent_artifact_id VARCHAR,
  parent_source_row_number NUMBER(19,0),
  pickup_at TIMESTAMP_NTZ(6),
  dropoff_at TIMESTAMP_NTZ(6),
  pickup_zone_source_id NUMBER(19,0),
  dropoff_zone_source_id NUMBER(19,0),
  pickup_zone_key NUMBER(19,0),
  dropoff_zone_key NUMBER(19,0),
  passenger_count NUMBER(19,0),
  payment_type NUMBER(19,0),
  distance_miles NUMBER(18,3),
  recorded_charge NUMBER(18,2),
  fare_amount NUMBER(18,2),
  tip_amount NUMBER(18,2),
  cbd_congestion_fee NUMBER(18,2),
  duration_seconds DOUBLE,
  warning_codes ARRAY,
  reason_codes ARRAY,
  raw_payload_json VARCHAR
);
