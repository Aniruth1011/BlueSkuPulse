select event_id, event_time
from {{ ref('stg_events') }}
where event_time < timestamp '2000-01-01'
   or event_time > {{ maximum_allowed_event_time() }}
