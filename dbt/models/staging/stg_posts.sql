with ranked as (
    select
        event_id, at_uri, author_did, operation, cid,
        cast(event_time as timestamp) as event_time,
        cast(ingested_at as timestamp) as ingested_at,
        cast(record_created_at as timestamp) as record_created_at,
        text, languages, reply_root_uri, reply_parent_uri, external_uri,
        quoted_post_uri, record_type, embed_type,
        cast(has_embed as boolean) as has_embed,
        cast(has_external_link as boolean) as has_external_link,
        cast(has_quote as boolean) as has_quote,
        cast(has_media as boolean) as has_media,
        cast(event_date as date) as event_date,
        row_number() over (partition by event_id order by ingested_at desc) as event_rank
    from {{ source('silver', 'silver_posts') }}
)
select
    event_id, at_uri, author_did, operation, cid, event_time, ingested_at,
    record_created_at, text, languages, reply_root_uri, reply_parent_uri,
    external_uri, quoted_post_uri, record_type, embed_type, has_embed,
    has_external_link, has_quote, has_media, event_date
from ranked where event_rank = 1
