{{ incremental_config('at_uri', 'event_date', ['author_did', 'at_uri', 'is_deleted']) }}

select
    event_id, at_uri, author_did, cid,
    coalesce(record_created_at, event_time) as created_at,
    event_time as last_mutation_at,
    ingested_at,
    text, languages,
    reply_root_uri, reply_parent_uri,
    reply_parent_uri is not null as is_reply,
    quoted_post_uri, has_quote, has_media, external_uri,
    {{ domain_from_url('external_uri') }} as external_domain,
    operation, is_deleted, event_date
from {{ ref('int_post_current_state') }}
{% if is_incremental() %}
where ingested_at >= {{ incremental_cutoff(this) }}
{% endif %}
