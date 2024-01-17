select
    'operational' as domain,
    'access center' as subdomain,
    'Incoming Call Volume' as metric_name,
    {{
    dbt_utils.surrogate_key([
        'access_intake_cont_call_type_key',
        'period_start_dt'
    ])
    }} as primary_key,
    period_start_dt as metric_date,
    call_center_grouper as drill_down_one, 
    call_center_group_alt_nm as drill_down_two,
    calls_offered_cnt as num, 
    'sum' as num_calculation,
    'count' as metric_type,
    'up' as desired_direction, 
    'spec_call_volume' as metric_id
from
    {{ref('access_services_calls')}}
where
    lower(call_center_grouper) in ('access center','decentralized', 'radiology')
    and scheduling_ind = 1
