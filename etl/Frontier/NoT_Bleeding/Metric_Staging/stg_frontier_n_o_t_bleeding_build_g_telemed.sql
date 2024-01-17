-- g:growth
select
    'NoT Bleeding: Telemedicine Visits' as metric_name,
    visit_key as primary_key,
    provider_name as drill_down_one,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_n_o_t_bleeding_telemed' as metric_id,
    visit_key as num
from
    {{ ref('frontier_n_o_t_bleeding_encounter_cohort')}}
where
    visit_type_id in ('2124', --'video visit follow up'
                    '2088', --'video visit new'
                    '2152' -- 'telephone visit'
                    )
    or encounter_type_id = '76' --'telemedicine'
