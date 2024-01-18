select
    harm_event_dt_month,
    harm_type,
    num_of_harm_events,
    num_of_population_days,
    num_of_harm_events * 100 as numerator,
    replace(date(harm_event_dt_month), '-', '') as primary_key
from
    {{ source('cdw', 'fact_ip_harm_monthly_dept_grp') }}
where
    lower(division) = 'neurosurgery'
    and lower(harm_type) = 'ssi'
    and date_trunc('month', harm_event_dt_month) < date_trunc('month', current_date)
