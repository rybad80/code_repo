-- g:growth
select
    'ACT-HF: Telemedicine Visits' as metric_name,
    visit_key as primary_key,
    provider_name as drill_down_one,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_act_hf_telemed' as metric_id,
    visit_key as num
from
    {{ ref('frontier_act_hf_encounter_cohort')}}
where
    visit_type_id in ('2124', --'video visit follow up'
                    '2088', --'video visit new'
                    '2152', -- 'telephone visit'
                    '2318' -- 'video visit diabetes'
                    )
    or encounter_type_id = '76' --'telemedicine'
