select
    event_id, at_uri, actor_did, subject_did, operation,
    cast(event_time as timestamp) as event_time,
    cast(ingested_at as timestamp) as ingested_at,
    cast(record_created_at as timestamp) as record_created_at,
    cast(event_date as date) as event_date
from {{ source('silver', 'silver_graph_edges') }}
qualify row_number() over (partition by event_id order by ingested_at desc) = 1
