{{ incremental_config('event_date', 'event_date', ['event_date']) }}

with silver as (
    select
        event_date,
        count(*) as silver_events,
        count(distinct event_id) as distinct_silver_events,
        sum(case when operation = 'delete' then 1 else 0 end) as delete_events,
        max(ingested_at) as latest_ingested_at
    from {{ ref('stg_events') }}
    group by event_date
), modeled as (
    select event_date, count(*) as modeled_mutations
    from (
        select event_date, event_id from {{ ref('stg_posts') }}
        union all select event_date, event_id from {{ ref('stg_likes') }}
        union all select event_date, event_id from {{ ref('stg_reposts') }}
        union all select event_date, event_id from {{ ref('stg_follows') }}
        union all select event_date, event_id from {{ ref('stg_identity') }}
    )
    group by event_date
), quarantined as (
    select event_date, count(*) as quarantine_events
    from {{ ref('stg_quarantine') }}
    group by event_date
)
select
    s.event_date,
    s.silver_events,
    s.distinct_silver_events,
    coalesce(m.modeled_mutations, 0) as modeled_mutations,
    coalesce(q.quarantine_events, 0) as quarantine_events,
    s.delete_events,
    s.silver_events - s.distinct_silver_events as duplicate_events,
    s.distinct_silver_events - coalesce(m.modeled_mutations, 0) as unexplained_delta,
    abs(s.distinct_silver_events - coalesce(m.modeled_mutations, 0)) * 1.0
      / nullif(s.distinct_silver_events, 0) as reconciliation_delta_ratio,
    s.latest_ingested_at
from silver s
left join modeled m using (event_date)
left join quarantined q using (event_date)
