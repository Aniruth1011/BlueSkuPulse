{{ incremental_config(['activity_hour', 'language_code'], 'activity_date', ['language_code', 'activity_hour']) }}

with exploded as (
    select
        {{ hour_bucket('created_at') }} as activity_hour,
        author_did,
        language_code
    from {{ ref('fct_posts') }}
    cross join {{ unnest_as('languages', 'language_code') }}
    where not is_deleted
)
select
    activity_hour,
    cast(activity_hour as date) as activity_date,
    language_code,
    count(*) as post_count,
    count(distinct author_did) as unique_authors,
    count(*) * 1.0 / nullif(sum(count(*)) over (partition by activity_hour), 0) as post_share
from exploded
group by activity_hour, language_code
