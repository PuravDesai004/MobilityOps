-- Grain: one official LocationID plus unresolved member 0.
select zone_key, borough, zone_name, service_zone, zone_sha256
from {{ source('curated', 'zones') }}
