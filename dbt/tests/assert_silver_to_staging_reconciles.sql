with modeled as (
    select event_id from {{ ref('stg_posts') }}
    union all select event_id from {{ ref('stg_likes') }}
    union all select event_id from {{ ref('stg_reposts') }}
    union all select event_id from {{ ref('stg_follows') }}
    union all select event_id from {{ ref('stg_identity') }}
), expected as (
    select event_id
    from {{ ref('stg_events') }}
    where event_kind <> 'commit'
       or collection in (
           'app.bsky.feed.post', 'app.bsky.feed.like',
           'app.bsky.feed.repost', 'app.bsky.graph.follow'
       )
), differences as (
    (select event_id from expected except distinct select event_id from modeled)
    union all
    (select event_id from modeled except distinct select event_id from expected)
)
select * from differences
