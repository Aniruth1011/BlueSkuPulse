{{ incremental_config('post_uri', 'event_date', ['post_uri', 'author_did']) }}

with engagement as (
    select subject_uri as post_uri, count(*) as like_count, 0 as repost_count
    from {{ ref('fct_likes') }} where not is_deleted group by subject_uri
    union all
    select subject_uri, 0, count(*)
    from {{ ref('fct_reposts') }} where not is_deleted group by subject_uri
), replies as (
    select reply_parent_uri as post_uri, count(*) as reply_count
    from {{ ref('fct_posts') }}
    where not is_deleted and reply_parent_uri is not null
    group by reply_parent_uri
), quotes as (
    select quoted_post_uri as post_uri, count(*) as quote_count
    from {{ ref('fct_posts') }}
    where not is_deleted and quoted_post_uri is not null
    group by quoted_post_uri
)
select
    p.at_uri as post_uri,
    p.author_did,
    p.event_date,
    coalesce(sum(e.like_count), 0) as like_count,
    coalesce(sum(e.repost_count), 0) as repost_count,
    coalesce(max(r.reply_count), 0) as reply_count,
    coalesce(max(q.quote_count), 0) as quote_count,
    coalesce(sum(e.like_count + e.repost_count), 0)
      + coalesce(max(r.reply_count), 0)
      + coalesce(max(q.quote_count), 0) as total_engagement
from {{ ref('fct_posts') }} p
left join engagement e on p.at_uri = e.post_uri
left join replies r on p.at_uri = r.post_uri
left join quotes q on p.at_uri = q.post_uri
where not p.is_deleted
group by p.at_uri, p.author_did, p.event_date
