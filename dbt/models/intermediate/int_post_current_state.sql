{{ incremental_config('at_uri', 'event_date', ['author_did', 'at_uri']) }}

with ranked as (
    select
        *,
        row_number() over (
            partition by at_uri order by event_time desc, ingested_at desc, event_id desc
        ) as mutation_rank
    from {{ ref('stg_posts') }}
    {% if is_incremental() %}
    where ingested_at >= {{ incremental_cutoff(this) }}
    {% endif %}
)
select
    event_id, at_uri, author_did, operation, cid, event_time, ingested_at,
    record_created_at, text, languages, reply_root_uri, reply_parent_uri,
    external_uri, quoted_post_uri, record_type, embed_type, has_embed,
    has_external_link, has_quote, has_media, event_date,
    operation = 'delete' as is_deleted
from ranked
where mutation_rank = 1
