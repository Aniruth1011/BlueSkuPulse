{{ incremental_config('at_uri', 'event_date', ['actor_did', 'subject_did']) }}

with ranked as (
    select
        *,
        row_number() over (
            partition by at_uri order by event_time desc, ingested_at desc, event_id desc
        ) as mutation_rank
    from {{ ref('stg_follows') }}
    {% if is_incremental() %}
    where ingested_at >= {{ incremental_cutoff(this) }}
    {% endif %}
)
select
    event_id, at_uri, actor_did, subject_did, operation,
    event_time, ingested_at, record_created_at, event_date,
    operation = 'delete' as is_deleted
from ranked
where mutation_rank = 1
