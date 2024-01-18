select
    'operational' as domain,
    'access center' as subdomain,
    'Call Answer Service Level' as metric_name,
    {{
    dbt_utils.surrogate_key([
        'access_intake_cont_call_type_key',
        'period_start_dt'
    ])
    }} as primary_key,
    period_start_dt as metric_date,
    call_center_grouper as drill_down_one,
    call_center_group_alt_nm as drill_down_two,
    service_level_calls_answered_cnt as num,
    service_level_calls_could_answer as denom,
    'sum' as num_calculation,
    'sum' as denom_calculation,
    'percentage' as metric_type,
    'up' as desired_direction,
    'spec_call_answer_service_level' as metric_id
from
    {{ref('access_services_calls')}}
where
    lower(call_center_grouper) in ('access center','decentralized', 'radiology')
    and scheduling_ind = 1
