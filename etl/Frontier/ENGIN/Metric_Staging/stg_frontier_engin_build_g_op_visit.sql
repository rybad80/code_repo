-- g:growth
select
    'ENGIN: Outpatient Visits' as metric_name,
    visit_key as primary_key,
    visit_type as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_engin_op_visits' as metric_id,
    visit_key as num
from
    {{ ref('frontier_engin_encounter_cohort')}}
where
    engin_inpatient_ind = 0
