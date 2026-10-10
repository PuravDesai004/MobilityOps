-- USER RUN ONLY. Snowflake execution is UNVERIFIED.
USE ROLE MOBILITYOPS_ENGINEER;
USE WAREHOUSE MOBILITYOPS_WH;
-- Change this line only when deliberately moving to the isolated real database.
USE DATABASE MOBILITYOPS_FIXTURE_RECOVERY;
ALTER SESSION SET TIMEZONE='America/New_York', STATEMENT_TIMEOUT_IN_SECONDS=600,
  TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6', QUERY_TAG='mobilityops_manual';

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
MERGE INTO CURATED.ACCEPTED_TRIPS t USING (
 SELECT dataset_id,artifact_id,source_partition,source_row_number,trip_record_id,parent_artifact_id,parent_source_row_number,pickup_at,dropoff_at,pickup_zone_source_id,dropoff_zone_source_id,pickup_zone_key,dropoff_zone_key,passenger_count,payment_type,distance_miles,recorded_charge,fare_amount,tip_amount,cbd_congestion_fee,duration_seconds,warning_codes,reason_codes,raw_payload_json FROM WORK.CLASSIFIED WHERE batch_id=:v_batch AND ARRAY_SIZE(reason_codes)=0) s
 ON t.dataset_id=s.dataset_id AND t.trip_record_id=s.trip_record_id
 WHEN NOT MATCHED THEN INSERT (dataset_id,artifact_id,source_partition,source_row_number,trip_record_id,parent_artifact_id,parent_source_row_number,pickup_at,dropoff_at,pickup_zone_source_id,dropoff_zone_source_id,pickup_zone_key,dropoff_zone_key,passenger_count,payment_type,distance_miles,recorded_charge,fare_amount,tip_amount,cbd_congestion_fee,duration_seconds,warning_codes,reason_codes,raw_payload_json) VALUES (s.dataset_id,s.artifact_id,s.source_partition,s.source_row_number,s.trip_record_id,s.parent_artifact_id,s.parent_source_row_number,s.pickup_at,s.dropoff_at,s.pickup_zone_source_id,s.dropoff_zone_source_id,s.pickup_zone_key,s.dropoff_zone_key,s.passenger_count,s.payment_type,s.distance_miles,s.recorded_charge,s.fare_amount,s.tip_amount,s.cbd_congestion_fee,s.duration_seconds,s.warning_codes,s.reason_codes,s.raw_payload_json);
IF (inject_failure) THEN RAISE controlled_failure; END IF;
MERGE INTO CURATED.QUARANTINED_TRIPS t USING (
 SELECT dataset_id,artifact_id,source_partition,source_row_number,trip_record_id,parent_artifact_id,parent_source_row_number,pickup_at,dropoff_at,pickup_zone_source_id,dropoff_zone_source_id,pickup_zone_key,dropoff_zone_key,passenger_count,payment_type,distance_miles,recorded_charge,fare_amount,tip_amount,cbd_congestion_fee,duration_seconds,warning_codes,reason_codes,raw_payload_json FROM WORK.CLASSIFIED WHERE batch_id=:v_batch AND ARRAY_SIZE(reason_codes)>0) s
 ON t.dataset_id=s.dataset_id AND t.trip_record_id=s.trip_record_id
 WHEN NOT MATCHED THEN INSERT (dataset_id,artifact_id,source_partition,source_row_number,trip_record_id,parent_artifact_id,parent_source_row_number,pickup_at,dropoff_at,pickup_zone_source_id,dropoff_zone_source_id,pickup_zone_key,dropoff_zone_key,passenger_count,payment_type,distance_miles,recorded_charge,fare_amount,tip_amount,cbd_congestion_fee,duration_seconds,warning_codes,reason_codes,raw_payload_json) VALUES (s.dataset_id,s.artifact_id,s.source_partition,s.source_row_number,s.trip_record_id,s.parent_artifact_id,s.parent_source_row_number,s.pickup_at,s.dropoff_at,s.pickup_zone_source_id,s.dropoff_zone_source_id,s.pickup_zone_key,s.dropoff_zone_key,s.passenger_count,s.payment_type,s.distance_miles,s.recorded_charge,s.fare_amount,s.tip_amount,s.cbd_congestion_fee,s.duration_seconds,s.warning_codes,s.reason_codes,s.raw_payload_json);

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
