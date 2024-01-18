select
	harm_event_dt_month,
    case
        when lower(harm_type) = 'harm index'
            then 'overall' else harm_type
        end as harm_type, --changing Harm Index row to be labeled "overall" to be consistent with other tables
	{{
    dbt_utils.surrogate_key([
        'harm_event_dt_month',
        'harm_type'
        ])
    }} as primary_key,
     case
        when lower(harm_type) = 'ssi' --numerator of ssi should be multiplied 100 instead of 1000 (like harm index)
            then (num_of_harm_events/10) else num_of_harm_events
        end as num_of_harm_events,
    num_of_population_days,
    'kpi_peds_harm_index' as metric_id
from
	{{ source('cdw', 'fact_ip_harm_monthly') }}
where
	lower(harm_type) in ('harm index', 'ssi', 'clabsi', 'pivie', 'havi')
	and num_of_population_days != 0
    and date(harm_event_dt_month) >= '07/01/2019'
    and date_trunc('month', harm_event_dt_month) < date_trunc('month', current_date)
