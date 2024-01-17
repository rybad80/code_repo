select
    'Program-Specific: ELECT Visits' as metric_name,
    frontier_thyroid_encounter_cohort.visit_key as primary_key,
    frontier_thyroid_encounter_cohort.provider_name as drill_down_one,
    frontier_thyroid_encounter_cohort.visit_type as drill_down_two,
    frontier_thyroid_encounter_cohort.encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_thyroid_elect_visits' as metric_id,
    frontier_thyroid_encounter_cohort.visit_key as num
from
    {{ ref('frontier_thyroid_encounter_cohort')}} as frontier_thyroid_encounter_cohort
where
    frontier_thyroid_encounter_cohort.elect_thyroid_ind = 1
