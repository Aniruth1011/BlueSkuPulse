{{ incremental_config('post_uri', 'event_date', ['author_did', 'post_uri']) }}

select
    post_uri, author_did, event_date,
    like_count, repost_count, reply_count, quote_count, total_engagement
from {{ ref('fct_post_engagement') }}
