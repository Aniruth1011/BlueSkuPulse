{{ incremental_config('at_uri', 'event_date', ['subject_uri', 'actor_did', 'is_deleted']) }}

select
    event_id, at_uri, actor_did, subject_uri, subject_cid,
    event_time, ingested_at, record_created_at, operation, is_deleted, event_date
from {{ ref('int_engagement_current_state') }}
where engagement_type = 'repost'
{% if is_incremental() %}
  and ingested_at >= {{ incremental_cutoff(this) }}
{% endif %}
