{{ incremental_config(['activity_date', 'actor_did'], 'activity_date', ['actor_did']) }}

with posts as (
    select event_date as activity_date, author_did as actor_did, count(*) as posts
    from {{ ref('fct_posts') }} where not is_deleted group by event_date, author_did
), engagement as (
    select p.event_date as activity_date, p.author_did as actor_did,
           sum(e.total_engagement) as engagement_received
    from {{ ref('fct_posts') }} p
    left join {{ ref('fct_post_engagement') }} e on p.at_uri = e.post_uri
    where not p.is_deleted
    group by p.event_date, p.author_did
), follows as (
    select event_date as activity_date, subject_did as actor_did, count(*) as observed_follow_edges
    from {{ ref('fct_follows') }} where not is_deleted group by event_date, subject_did
)
select
    p.activity_date,
    p.actor_did,
    p.posts,
    coalesce(e.engagement_received, 0) as engagement_received,
    coalesce(e.engagement_received, 0) * 1.0 / nullif(p.posts, 0) as engagement_per_post,
    coalesce(f.observed_follow_edges, 0) as observed_follow_edges,
    sum(p.posts) over (
      partition by p.actor_did order by p.activity_date rows between 6 preceding and current row
    ) as rolling_7d_posts,
    sum(coalesce(e.engagement_received, 0)) over (
      partition by p.actor_did order by p.activity_date rows between 6 preceding and current row
    ) as rolling_7d_engagement
from posts p
left join engagement e using (activity_date, actor_did)
left join follows f using (activity_date, actor_did)
