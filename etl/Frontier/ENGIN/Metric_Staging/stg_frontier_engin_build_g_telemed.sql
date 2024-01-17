-- g:growth
select
    'ENGIN: Telemedicine Visits' as metric_name,
    visit_key as primary_key,
    provider_name as drill_down_one,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_engin_telemed' as metric_id,
    visit_key as num
from
    {{ ref('frontier_engin_encounter_cohort')}}
where
    visit_type_id in ('2124', --'video visit follow up'
                    '2088', --'video visit new'
                    '2152', -- 'telephone visit'
                    '2546', -- 'engin video visit new'
                    '2548', --'engin video visit fol up'
                    '2555', -- 'engin telephone visit'
                    '3277', -- 'engin video visit gc only'
                    '3278' -- 'engin video visit fu intrnl'
                    )
    or encounter_type_id = '76' --'telemedicine'
