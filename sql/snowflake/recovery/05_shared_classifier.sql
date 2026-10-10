-- USER RUN ONLY. Snowflake execution is UNVERIFIED.
USE ROLE MOBILITYOPS_ENGINEER;
USE WAREHOUSE MOBILITYOPS_WH;
-- Change this line only when deliberately moving to the isolated real database.
USE DATABASE MOBILITYOPS_FIXTURE_RECOVERY;
ALTER SESSION SET TIMEZONE='America/New_York', STATEMENT_TIMEOUT_IN_SECONDS=600,
  TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6', QUERY_TAG='mobilityops_manual';

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
CREATE OR REPLACE VIEW WORK.CLASSIFIED AS SELECT batch_id, raw_id,
classified:dataset_id::VARCHAR AS dataset_id,
classified:artifact_id::VARCHAR AS artifact_id,
classified:source_partition::VARCHAR AS source_partition,
classified:source_row_number::NUMBER(19,0) AS source_row_number,
classified:trip_record_id::VARCHAR AS trip_record_id,
classified:parent_artifact_id::VARCHAR AS parent_artifact_id,
classified:parent_source_row_number::NUMBER(19,0) AS parent_source_row_number,
TRY_TO_TIMESTAMP_NTZ(classified:pickup_at::VARCHAR) AS pickup_at,
TRY_TO_TIMESTAMP_NTZ(classified:dropoff_at::VARCHAR) AS dropoff_at,
classified:pickup_zone_source_id::NUMBER(19,0) AS pickup_zone_source_id,
classified:dropoff_zone_source_id::NUMBER(19,0) AS dropoff_zone_source_id,
classified:pickup_zone_key::NUMBER(19,0) AS pickup_zone_key,
classified:dropoff_zone_key::NUMBER(19,0) AS dropoff_zone_key,
classified:passenger_count::NUMBER(19,0) AS passenger_count,
classified:payment_type::NUMBER(19,0) AS payment_type,
classified:distance_miles::NUMBER(18,3) AS distance_miles,
classified:recorded_charge::NUMBER(18,2) AS recorded_charge,
classified:fare_amount::NUMBER(18,2) AS fare_amount,
classified:tip_amount::NUMBER(18,2) AS tip_amount,
classified:cbd_congestion_fee::NUMBER(18,2) AS cbd_congestion_fee,
classified:duration_seconds::DOUBLE AS duration_seconds,
classified:warning_codes::ARRAY AS warning_codes,
classified:reason_codes::ARRAY AS reason_codes,
classified:raw_payload_json::VARCHAR AS raw_payload_json
FROM WORK.INCOMING;
