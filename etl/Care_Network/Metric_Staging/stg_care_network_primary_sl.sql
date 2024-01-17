select
    {{
        dbt_utils.surrogate_key([
            'access_intake_cont_call_type_key',
            'period_start_dt'
        ])
    }} as primary_key,   
    'Call Answer Service Level' as metric_name,
    'pc_pfex_cisco_service_level' as metric_id,
    case when call_center_group_id = '5009' then 'Market Street'
            when call_center_group_id = '5042' then 'Cobbs Creek'
            when call_center_group_id = '5068' then 'Doylestown'
            when call_center_group_id = '5063' then 'Chadds Ford'
            when call_center_group_id = '5064' then 'Chestnut Hill'
            when call_center_group_id = '5071' then 'Coatesville'
            when call_center_group_id = '5073' then 'Harborview'
            when call_center_group_id = '5061' then 'Karabots'
            when call_center_group_id = '5070' then 'Kennett Square'
            when call_center_group_id = '5078' then 'Moorestown'
            when call_center_group_id = '5082' then 'Norristown'
            when call_center_group_id = '5072' then 'Paoli'
            when call_center_group_id = '5083' then 'Salem Road'
            when call_center_group_id = '5062' then 'South Philadelphia'
            when call_center_group_id = '5094' then 'Collegeville'
            when call_center_group_id = '5092' then 'Delaware County'
            when call_center_group_id = '5098' then 'Flourtown'
            when call_center_group_id = '5087' then 'Gibbsboro'
            when call_center_group_id = '5089' then 'Haverford'
            when call_center_group_id = '5097' then 'Highpoint'
            when call_center_group_id = '5096' then 'Newtown'
            when call_center_group_id = '5095' then 'Pottstown'
            when call_center_group_id = '5091' then 'Roxborough'
            when call_center_group_id = '5081' then 'Souderton'
            when call_center_group_id = '5090' then 'Springfield'
            when call_center_group_id = '5093' then 'West Chester'
            when call_center_group_id = '5088' then 'West Grove'
           else null
           end as drill_down,
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
    call_center_grouper = 'Primary Care'
