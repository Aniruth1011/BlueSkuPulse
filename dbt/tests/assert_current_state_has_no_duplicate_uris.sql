select at_uri, count(*) as duplicate_count
from {{ ref('int_post_current_state') }}
group by at_uri
having count(*) > 1
