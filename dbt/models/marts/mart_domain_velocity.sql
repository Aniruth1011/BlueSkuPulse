{{ incremental_config(['activity_hour', 'domain'], 'activity_date', ['domain', 'activity_hour']) }}

with post_domains as (
    select
        {{ hour_bucket('p.created_at') }} as activity_hour,
        p.external_domain as domain,
        p.author_did,
        p.at_uri,
        coalesce(e.total_engagement, 0) as engagement
    from {{ ref('fct_posts') }} p
    left join {{ ref('fct_post_engagement') }} e on p.at_uri = e.post_uri
    where not p.is_deleted and p.external_domain is not null and p.external_domain <> ''
), sharers as (
    select activity_hour, domain, author_did,
           count(*) as author_shares, sum(engagement) as author_engagement
    from post_domains
    group by activity_hour, domain, author_did
), hourly as (
    select
        activity_hour,
        domain,
        sum(author_shares) as shares,
        count(distinct author_did) as unique_sharers,
        sum(author_engagement) as engagement,
        max(author_shares) as largest_sharer_posts
    from sharers
    group by activity_hour, domain
), compared as (
    select
        *,
        lag(shares, 1, 0) over (partition by domain order by activity_hour) as prior_shares,
        sum(shares) over (partition by activity_hour) as all_domain_shares
    from hourly
)
select
    activity_hour,
    cast(activity_hour as date) as activity_date,
    domain,
    shares,
    unique_sharers,
    engagement,
    engagement * 1.0 / nullif(shares, 0) as engagement_per_share,
    largest_sharer_posts * 1.0 / nullif(shares, 0) as sharer_concentration,
    shares * 1.0 / nullif(all_domain_shares, 0) as outbound_link_share,
    shares * 1.0 / greatest(prior_shares, 1) as velocity,
    shares - prior_shares as acceleration
from compared
