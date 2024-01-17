select
    {{
        dbt_utils.surrogate_key([
            'access_intake_cont_call_type_key',
            'period_start_dt'
        ])
    }} as primary_key,   
    'Call Answer Service Level - After Hours' as metric_name,
    'pc_pfex_after_hour_service_level' as metric_id,
    access_intake_cont_call_type_key,
    period_start_dt,
    period_start_hr,
    calendar_month,
    call_type_id,
    call_type,
    call_type_desc,
    call_center_group_id,
    call_center_group,
    call_center_grouper,
    calls_offered_cnt,
    calls_handled_cnt,
    calls_agent_answered_cnt,
    total_calls_abandoned_cnt,
    max_calls_queued_cnt,
    service_level_calls_answered_cnt,
    service_level_calls_offered_cnt,
    service_level_abandoned_cnt,
    service_level_calls_could_answer,
    service_level_calls_could_answer as denominator,
    call_handled_seconds,
    answer_wait_seconds,
    max_call_wait_seconds,
    abandoned_call_delay_seconds,
    standard_hours_ind,
    weekday_ind,
    standard_call_center_day_ind,
    scheduling_ind
    
from
    {{ref('access_services_calls')}}

where
    call_center_group_id = '5002' --After Hours Program
