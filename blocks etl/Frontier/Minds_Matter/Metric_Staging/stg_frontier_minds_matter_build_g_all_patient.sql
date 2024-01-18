select
    'Minds Matter: All Patients - Unique' as metric_name,
    visit_key as primary_key,
    patient_sub_group as drill_down_one,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_minds_matter_all_unique' as metric_id,
    pat_key as num
from
    {{ ref('frontier_minds_matter_encounter_cohort') }}
where
    lower(appointment_status) not in ('canceled', 'no show', 'left without seen', 'no')
