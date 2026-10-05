select distinct
    event_date as date_day,
    extract(year from event_date) as year_number,
    extract(month from event_date) as month_number,
    extract(day from event_date) as day_number,
    extract(dayofweek from event_date) as day_of_week
from {{ ref('stg_events') }}
