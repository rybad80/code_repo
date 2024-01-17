select
    'Program-Specific: Patients' as metric_name,
    visit_key as primary_key,
    patient_description as drill_down,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_heart_valve_unique_pat' as metric_id,
    mrn as num
from
    {{ ref('frontier_heart_valve_encounter_cohort')}}
