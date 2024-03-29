select
    'CCPM: All Patients (Unique)' as metric_name,
    visit_key as primary_key,
    patient_sub_cohort as drill_down_one,
    admission_department as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_ccpm_pat_all_unique' as metric_id,
    pat_key as num
from
    {{ ref('frontier_ccpm_encounter_cohort') }}
where
    patient_sub_cohort != 'potential ccpm group'
