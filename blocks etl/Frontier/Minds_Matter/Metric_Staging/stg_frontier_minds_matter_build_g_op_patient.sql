select
    'Minds Matter: Outpatients - Unique' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_minds_matter_op_unique' as metric_id,
    pat_key as num
from
    {{ ref('frontier_minds_matter_encounter_cohort') }}
where
    minds_matter_patient_ind = '1'
