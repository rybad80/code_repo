select distinct
    date_trunc('month', full_date) as event_year_month
from
    {{ ref('dim_date') }}
where
    full_date >= '2017-01-01'
    and full_date < date_trunc('month', current_date)
