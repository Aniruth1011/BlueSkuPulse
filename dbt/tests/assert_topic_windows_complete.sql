with expected as (
    select 5 as window_minutes union all select 15 union all select 60
), actual as (
    select distinct window_minutes from {{ ref('mart_topic_velocity') }}
)
select e.window_minutes
from expected e
left join actual a using (window_minutes)
where a.window_minutes is null
