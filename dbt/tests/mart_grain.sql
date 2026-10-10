select dataset_id, artifact_id, source_partition, pickup_date, pickup_hour, pickup_zone_key
from {{ ref('mart_zone_hour') }}
group by dataset_id, artifact_id, source_partition, pickup_date, pickup_hour, pickup_zone_key
having count(*)<>1
