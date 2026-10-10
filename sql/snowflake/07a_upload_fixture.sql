-- USER RUN ONLY. Snowflake execution is UNVERIFIED.
-- PUT must run in a laptop client, not a browser worksheet. Edit paths if moved.
USE ROLE MOBILITYOPS_ENGINEER;
USE WAREHOUSE MOBILITYOPS_WH;
USE DATABASE MOBILITYOPS_FIXTURE;
PUT 'file://C:/Users/purav/OneDrive/Desktop/Data Projects/MobilityOps/tests/fixtures/yellow_taxi_fixture.parquet'
 @RAW.SOURCE_STAGE/19d61b096876bd9739a6d09a601f096d76f2186ffac87b9e281e087dc20668a8/
 AUTO_COMPRESS=FALSE OVERWRITE=FALSE;
PUT 'file://C:/Users/purav/OneDrive/Desktop/Data Projects/MobilityOps/tests/fixtures/taxi_zone_lookup_fixture.csv'
 @RAW.SOURCE_STAGE/zones/ AUTO_COMPRESS=FALSE OVERWRITE=FALSE;
PUT 'file://C:/Users/purav/OneDrive/Desktop/Data Projects/MobilityOps/sql/snowflake/assets/mobilityops_policy.zip'
 @CONTROL.CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=FALSE;
LIST @RAW.SOURCE_STAGE;
LIST @CONTROL.CODE_STAGE;
