select 
    harm_event_dt_month,
    case when lower(harm_type) = 'harm index'
        then 'overall'
        else harm_type
        --changing Harm Index row to be labeled "overall" to be consistent with formatting in other tables
        end as harm_type,
    {{
    dbt_utils.surrogate_key([
        'harm_event_dt_month',
        'harm_type'
        ])
    }} as primary_key,
    sum(num_of_harm_events) as num_of_harm_events,
    sum(num_of_population_days) as num_of_population_days,
    'cardiac_harm' as metric_id
from 
    {{ source('cdw', 'fact_ip_harm_monthly_dept_grp') }} 
where 
    lower(dept_grp_abbr) in ('cicu', 'ccu')
    and num_of_population_days != 0
    and date_trunc('month', harm_event_dt_month) < date_trunc('month', current_date) 
group by
    harm_event_dt_month,
    harm_type
