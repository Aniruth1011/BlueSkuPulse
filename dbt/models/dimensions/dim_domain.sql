with domains as (
    select {{ domain_from_url('external_uri') }} as domain
    from {{ ref('stg_posts') }}
    where external_uri is not null
)
select domain, count(*) as observed_post_mutations
from domains
where domain is not null and domain <> ''
group by domain
