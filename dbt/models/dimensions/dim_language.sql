select
    language_code,
    count(*) as observed_post_mutations
from {{ ref('stg_posts') }} as p
cross join unnest(p.languages) as language_code
where language_code is not null
group by language_code
