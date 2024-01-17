select
    'Airway: Outpatient Visits' as metric_name,
    visit_key as primary_key,
    provider_name as drill_down_one,
    visit_type as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_airway_op_visits' as metric_id,
    visit_key as num
from
    {{ ref('frontier_airway_encounter_cohort')}}
where
    inpatient_ind = 0
