{{ incremental_config('at_uri', 'event_date', ['subject_uri', 'actor_did', 'engagement_type']) }}

with mutations as (
    select *, 'like' as engagement_type from {{ ref('stg_likes') }}
    union all
    select *, 'repost' as engagement_type from {{ ref('stg_reposts') }}
), ranked as (
    select
        *,
        row_number() over (
            partition by at_uri order by event_time desc, ingested_at desc, event_id desc
        ) as mutation_rank
    from mutations
    {% if is_incremental() %}
    where ingested_at >= {{ incremental_cutoff(this) }}
    {% endif %}
)
select
    event_id, at_uri, actor_did, engagement_type, operation,
    subject_uri, subject_cid, event_time, ingested_at, record_created_at,
    event_date, operation = 'delete' as is_deleted
from ranked
where mutation_rank = 1
