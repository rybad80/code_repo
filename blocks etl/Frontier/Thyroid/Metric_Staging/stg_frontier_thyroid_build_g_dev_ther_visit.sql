select
    'Program-Specific: Developmental Therapeutics Visits' as metric_name,
    visit_key as primary_key,
    provider_name as drill_down_one,
    visit_type as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_thyroid_dev_ther_visits' as metric_id,
    visit_key as num
from
    {{ ref('frontier_thyroid_encounter_cohort')}}
where
    developmental_therapeutics_ind = 1
