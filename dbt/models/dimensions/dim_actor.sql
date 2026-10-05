with observations as (
    select author_did as did, event_time from {{ ref('stg_posts') }}
    union all select actor_did, event_time from {{ ref('stg_likes') }}
    union all select actor_did, event_time from {{ ref('stg_reposts') }}
    union all select actor_did, event_time from {{ ref('stg_follows') }}
    union all select subject_did, event_time from {{ ref('stg_follows') }}
    union all select did, event_time from {{ ref('stg_identity') }}
)
select
    did,
    min(event_time) as first_observed_at,
    max(event_time) as last_observed_at
from observations
where did is not null
group by did
