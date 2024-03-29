-- g:growth
select
    'Program-Specific: New Patient (Unique)' as metric_name,
    visit_key as primary_key,
    visit_type as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_engin_new_unique' as metric_id,
    pat_key as num
from
    {{ ref('frontier_engin_encounter_cohort')}}
where
    new_visit_ind = 1
