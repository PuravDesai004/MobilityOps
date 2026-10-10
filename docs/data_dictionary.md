# Data dictionary and metric meaning

Source: [TLC trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
[Yellow Taxi dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).
Links reviewed 10 October 2026. Required columns are declared in the preserved YAML contract;
the actual schema/types/null counts are in source profile reports. Additional source fields
are retained, not removed because this model doesn't use them.

| Source field | Curated field/type | Policy |
| --- | --- | --- |
| tpep_pickup_datetime | pickup_at TIMESTAMP microseconds | Required, naive New York wall clock |
| tpep_dropoff_datetime | dropoff_at TIMESTAMP microseconds | Required; not before pickup |
| trip_distance | distance_miles DECIMAL(18,3) | Required, finite, nonnegative before rounding |
| total_amount | recorded_charge DECIMAL(18,2) | Required, finite; signed negative adjustment retained |
| PULocationID / DOLocationID | pickup/dropoff_zone_source_id BIGINT nullable | Original integer IDs; lookup failures map canonical keys to 0 |
| passenger_count | passenger_count BIGINT nullable | Missing/zero warning; no mandatory-positive rule |
| payment_type | payment_type BIGINT nullable | 1–6 known; other/missing codes warn |
| fare_amount / tip_amount | DECIMAL(18,2) nullable | Optional; recorded tips are not all tips |
| cbd_congestion_fee | DECIMAL(18,2) nullable | Optional missing remains null, not zero |
| Other provider fields | raw_payload_json and unchanged source | Preserved original fields, with typed JSON tags for datetime/Decimal/nonfinite values |

Provenance columns are dataset_id, artifact_id, source_partition, source_row_number,
trip_record_id, parent_artifact_id and parent_source_row_number. Sample parent positions differ
from the sample's own positions. The canonical zone keys reference dim_zone, including key 0.
`duration_seconds` is the naive timestamp difference and may have fractional seconds.
Reason/warning lists are sorted distinct codes. Every curated record retains source values.

The official lookup contributes borough, zone_name and service_zone. Its 265 source members
plus one unresolved member yield 266 real dimension members. Official Unknown labels remain
valid members; they are distinct from a source ID not in the lookup.

## Analytical grains

| Model | One row means |
| --- | --- |
| stg_trips | One accepted physical source record |
| dim_zone | One lookup zone key |
| dim_date | One actual accepted pickup date, calendar year/month/day |
| fct_trip | One stable accepted record key in its isolated database scope |
| mart_zone_hour | Dataset/artifact/partition + date/hour/pickup zone |
| mart_daily_reconciliation | Dataset/artifact/partition + date bucket, including invalid/missing pickup |

The zone/hour snapshot adds zone_name and borough to mart measures:

| Measure | Meaning / units |
| --- | --- |
| trip_count | Accepted source records |
| recorded_charge_sum | Sum of row-rounded signed recorded passenger charge, USD |
| distance_miles_sum / distance_count | Additive miles and eligible accepted record denominator |
| duration_seconds_sum / duration_count | Additive wall-clock seconds and accepted record denominator |
| warning_trip_count | Accepted records having at least one warning; not summed warning occurrences |
| Daily source/accepted/quarantined counts | Logical source accounting per artifact/date bucket |

Distance average is sum(distance_miles_sum)/sum(distance_count); duration minutes is
sum(duration_seconds_sum)/sum(duration_count)/60 after filters. Reason occurrence totals may
exceed quarantined records. Negative signed charges are included. None of these metrics is
operator profit, fare-card settlement, all cash tips, or a random-sample population estimate.
Source month and actual pickup coverage are separate; DST ambiguity is not resolved into UTC.
