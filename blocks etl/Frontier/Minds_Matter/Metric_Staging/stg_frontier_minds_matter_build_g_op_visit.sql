select
    'Minds Matter: Outpatients - Visits' as metric_name,
    visit_key as primary_key,
    visit_type as drill_down_one,
    encounter_type as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_minds_matter_op_visits' as metric_id,
    visit_key as num
from
    {{ ref('frontier_minds_matter_encounter_cohort') }}
where
    minds_matter_patient_ind = '1'
