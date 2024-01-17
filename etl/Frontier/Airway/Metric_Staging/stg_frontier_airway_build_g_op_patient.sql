select
    'Airway: Outpatients (Unique)' as metric_name,
    visit_key as primary_key,
    provider_name as drill_down_one,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_airway_op_unique' as metric_id,
    pat_key as num
from
    {{ ref('frontier_airway_encounter_cohort')}}
where
    inpatient_ind = 0
