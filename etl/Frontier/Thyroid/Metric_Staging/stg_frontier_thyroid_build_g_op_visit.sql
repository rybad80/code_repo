select
    'Thyroid Center: Outpatient Visits' as metric_name,
    visit_key as primary_key,
    provider_name as drill_down_one,
    visit_type as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_thyroid_op_visits' as metric_id,
    visit_key as num
from
    {{ ref('frontier_thyroid_encounter_cohort')}}
where
    thyroid_inpatient_ind = 0
    and (center_visit_ind + dx_visit_ind
        + elect_providers_ind + developmental_therapeutics_ind) >= 1
