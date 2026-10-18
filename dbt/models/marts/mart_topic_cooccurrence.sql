{{ incremental_config(['activity_hour', 'topic_a', 'topic_b'], 'activity_date', ['topic_a', 'topic_b', 'activity_hour']) }}

with post_topics as (
    select distinct
        {{ hour_bucket('p.created_at') }} as activity_hour,
        p.at_uri,
        p.author_did,
        topic
    from {{ ref('fct_posts') }} p
    cross join {{ unnest_as(topic_tokens('p.text'), 'topic') }}
    where not p.is_deleted
), hour_totals as (
    select activity_hour, count(distinct at_uri) as tagged_posts
    from post_topics
    group by activity_hour
), topic_totals as (
    select activity_hour, topic, count(distinct at_uri) as topic_posts
    from post_topics
    group by activity_hour, topic
), pairs as (
    select
        a.activity_hour,
        a.topic as topic_a,
        b.topic as topic_b,
        a.at_uri,
        a.author_did
    from post_topics a
    join post_topics b
      on a.at_uri = b.at_uri
     and a.activity_hour = b.activity_hour
     and a.topic < b.topic
), author_pairs as (
    select activity_hour, topic_a, topic_b, author_did, count(*) as author_posts
    from pairs
    group by activity_hour, topic_a, topic_b, author_did
), pair_counts as (
    select
        activity_hour,
        topic_a,
        topic_b,
        sum(author_posts) as pair_posts,
        count(distinct author_did) as unique_authors,
        max(author_posts) as largest_author_posts
    from author_pairs
    group by activity_hour, topic_a, topic_b
)
select
    c.activity_hour,
    cast(c.activity_hour as date) as activity_date,
    c.topic_a,
    c.topic_b,
    c.pair_posts,
    c.unique_authors,
    ta.topic_posts as topic_a_posts,
    tb.topic_posts as topic_b_posts,
    h.tagged_posts,
    c.pair_posts * 1.0 / h.tagged_posts as support,
    c.pair_posts * 1.0 / ta.topic_posts as confidence_a_to_b,
    c.pair_posts * 1.0 / tb.topic_posts as confidence_b_to_a,
    c.pair_posts * 1.0 * h.tagged_posts / (ta.topic_posts * tb.topic_posts) as lift,
    c.largest_author_posts * 1.0 / c.pair_posts as author_concentration,
    c.unique_authors >= 2
      and c.largest_author_posts * 1.0 / c.pair_posts <= 0.5 as passes_spam_guardrail
from pair_counts c
join hour_totals h on c.activity_hour = h.activity_hour
join topic_totals ta on c.activity_hour = ta.activity_hour and c.topic_a = ta.topic
join topic_totals tb on c.activity_hour = tb.activity_hour and c.topic_b = tb.topic
