{{ incremental_config('at_uri', 'event_date', ['actor_did', 'subject_did', 'is_deleted']) }}

select
    event_id, at_uri, actor_did, subject_did, event_time, ingested_at,
    record_created_at, operation, is_deleted, event_date
from {{ ref('int_follow_current_state') }}
{% if is_incremental() %}
where ingested_at >= {{ incremental_cutoff(this) }}
{% endif %}
