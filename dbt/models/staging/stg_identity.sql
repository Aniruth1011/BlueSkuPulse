select
    event_id, did, event_kind, operation,
    cast(event_time as timestamp) as event_time,
    cast(ingested_at as timestamp) as ingested_at,
    payload_json, cast(event_date as date) as event_date
from {{ source('silver', 'silver_identity') }}
qualify row_number() over (partition by event_id order by ingested_at desc) = 1
