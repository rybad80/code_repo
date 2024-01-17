select
    'Program Specific: Visits - Primary Care' as metric_name,
    visit_key as primary_key,
    visit_type as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_minds_matter_visits_pc' as metric_id,
    visit_key as num
from
    {{ ref('frontier_minds_matter_encounter_cohort') }}
where
    encounter_sub_group = 'Primary Care'
    --and visit_type_id != '0'
