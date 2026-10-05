select
    event_id,
    event_kind,
    collection,
    cast(ingested_at as timestamp) as ingested_at,
    cast(event_date as date) as event_date,
    quality_errors
from {{ source('silver', 'silver_quarantine') }}
