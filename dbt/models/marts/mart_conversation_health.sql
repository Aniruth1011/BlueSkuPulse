{{ incremental_config('root_uri', 'event_date', ['root_uri']) }}

with recursive reply_edges as (
    select
        coalesce(reply_root_uri, reply_parent_uri) as root_uri,
        reply_parent_uri as parent_uri,
        at_uri as child_uri,
        author_did,
        created_at
    from {{ ref('fct_posts') }}
    where not is_deleted and reply_parent_uri is not null
), thread_walk as (
    select root_uri, parent_uri, child_uri, author_did, created_at, 1 as depth
    from reply_edges
    where parent_uri = root_uri
    union all
    select e.root_uri, e.parent_uri, e.child_uri, e.author_did, e.created_at, w.depth + 1
    from reply_edges e
    join thread_walk w
      on e.root_uri = w.root_uri and e.parent_uri = w.child_uri
    where w.depth < 100
), resolved as (
    select root_uri, parent_uri, child_uri, author_did, created_at, depth
    from thread_walk
    union all
    select e.root_uri, e.parent_uri, e.child_uri, e.author_did, e.created_at, null as depth
    from reply_edges e
    where not exists (select 1 from thread_walk w where w.child_uri = e.child_uri)
), root_times as (
    select at_uri as root_uri, created_at as root_created_at
    from {{ ref('fct_posts') }} where not is_deleted
)
select
    r.root_uri,
    cast(min(r.created_at) as date) as event_date,
    count(*) as reply_count,
    count(distinct r.author_did) as unique_participants,
    max(r.depth) as max_reply_depth,
    count(*) * 1.0 / nullif(count(distinct r.parent_uri), 0) as average_branching_factor,
    sum(case when r.depth is null then 1 else 0 end) as orphan_reply_count,
    min(r.created_at) as first_observed_reply_at,
    max(r.created_at) as last_observed_reply_at,
    {{ duration_seconds('min(r.created_at)', 'max(r.created_at)') }} as observed_duration_seconds,
    {{ duration_seconds('min(rt.root_created_at)', 'min(r.created_at)') }} as time_to_first_reply_seconds
from resolved r
left join root_times rt on r.root_uri = rt.root_uri
group by r.root_uri
