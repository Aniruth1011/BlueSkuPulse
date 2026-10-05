{{ incremental_config(['window_start', 'window_minutes', 'topic'], 'window_date', ['topic', 'window_minutes', 'window_start']) }}

with post_topics as (
    select
        p.created_at,
        p.author_did,
        topic,
        p.at_uri,
        coalesce(e.total_engagement, 0) as engagement
    from {{ ref('fct_posts') }} p
    left join {{ ref('fct_post_engagement') }} e on p.at_uri = e.post_uri
    cross join unnest({{ topic_tokens('p.text') }}) as topic
    where not p.is_deleted
), window_sizes as (
    select 5 as window_minutes union all select 15 union all select 60
), exploded as (
    select
        {{ minute_bucket('p.created_at', 'w.window_minutes') }} as window_start,
        w.window_minutes,
        p.author_did,
        p.topic,
        p.at_uri,
        p.engagement
    from post_topics p
    cross join window_sizes w
), author_counts as (
    select window_start, window_minutes, topic, author_did,
           count(*) as author_posts, sum(engagement) as author_engagement
    from exploded
    group by window_start, window_minutes, topic, author_did
), windows as (
    select
        window_start,
        window_minutes,
        topic,
        sum(author_posts) as post_count,
        count(distinct author_did) as unique_authors,
        max(author_posts) as largest_author_posts,
        sum(author_engagement) as engagement
    from author_counts
    group by window_start, window_minutes, topic
), compared as (
    select
        *,
        lag(post_count, 1, 0) over (
          partition by topic, window_minutes order by window_start
        ) as prior_count,
        lag(post_count, 2, 0) over (
          partition by topic, window_minutes order by window_start
        ) as two_windows_ago_count,
        avg(post_count) over (
          partition by topic, window_minutes order by window_start
          rows between 12 preceding and 1 preceding
        ) as baseline_mean,
        stddev_samp(post_count) over (
          partition by topic, window_minutes order by window_start
          rows between 12 preceding and 1 preceding
        ) as baseline_stddev
    from windows
)
select
    window_start,
    cast(window_start as date) as window_date,
    window_minutes,
    topic,
    post_count,
    unique_authors,
    engagement,
    largest_author_posts * 1.0 / nullif(post_count, 0) as author_concentration,
    post_count * 1.0 / greatest(prior_count, 1) as velocity,
    (post_count - prior_count) - (prior_count - two_windows_ago_count) as acceleration,
    (post_count - baseline_mean) / nullif(baseline_stddev, 0) as anomaly_z_score,
    case
        when unique_authors < 2 then 0
        when largest_author_posts * 1.0 / nullif(post_count, 0) > 0.5 then 0
        else ln(1 + post_count)
          * (post_count * 1.0 / greatest(prior_count, 1))
          * (1 + ln(1 + engagement))
    end as ranking_score,
    unique_authors >= 2
      and largest_author_posts * 1.0 / nullif(post_count, 0) <= 0.5 as passes_spam_guardrail
from compared
