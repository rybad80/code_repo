-- g:growth
select
    'Program-Specific: All Births' as metric_name,
    visit_key as primary_key,
    sub_cohort as drill_down_one,
    department_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_drof_all_births' as metric_id,
    pat_key as num
from
    {{ ref('frontier_drof_encounter_cohort')}}
