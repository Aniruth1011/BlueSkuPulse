{{ incremental_config(['activity_hour'], 'activity_date', ['activity_hour']) }}

with event_counts as (
    select
        {{ hour_bucket('event_time') }} as activity_hour,
        count(*) as event_count,
        count(distinct did) as active_actors,
        sum(case when collection = 'app.bsky.feed.post' then 1 else 0 end) as post_mutations,
        sum(case when collection = 'app.bsky.feed.like' then 1 else 0 end) as like_mutations,
        sum(case when collection = 'app.bsky.feed.repost' then 1 else 0 end) as repost_mutations,
        sum(case when collection = 'app.bsky.graph.follow' then 1 else 0 end) as follow_mutations,
        max(ingested_at) as warehouse_watermark
    from {{ ref('stg_events') }}
    group by activity_hour
)
select
    activity_hour,
    cast(activity_hour as date) as activity_date,
    event_count,
    active_actors,
    post_mutations,
    like_mutations,
    repost_mutations,
    follow_mutations,
    warehouse_watermark
from event_counts
