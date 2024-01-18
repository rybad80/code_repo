select
    'Program-Specific: EEG' as metric_name,
    frontier_engin_encounter_cohort.visit_key as primary_key,
    frontier_engin_encounter_cohort.department_name as drill_down_one,
    frontier_engin_encounter_cohort.encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_engin_eeg' as metric_id,
    frontier_engin_encounter_cohort.visit_key as num
from
    {{ ref('frontier_engin_encounter_cohort')}} as frontier_engin_encounter_cohort
where frontier_engin_encounter_cohort.eeg_ind = 1
