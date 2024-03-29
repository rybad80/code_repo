select
    'Program Specific: Other Concussion Patients - Unique' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_minds_matter_other_patient' as metric_id,
    pat_key as num
from
    {{ ref('frontier_minds_matter_encounter_cohort') }}
where
    encounter_sub_group = 'Other'
