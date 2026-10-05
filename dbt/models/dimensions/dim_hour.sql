select distinct
    extract(hour from event_time) as hour_number
from {{ ref('stg_events') }}
