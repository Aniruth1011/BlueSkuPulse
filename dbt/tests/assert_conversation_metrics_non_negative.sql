select root_uri
from {{ ref('mart_conversation_health') }}
where reply_count < 0
   or unique_participants < 0
   or coalesce(max_reply_depth, 0) < 0
   or orphan_reply_count < 0
   or observed_duration_seconds < 0
   or coalesce(time_to_first_reply_seconds, 0) < 0
