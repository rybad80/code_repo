select
    'Program Specific: Concussion Step Test' as metric_name,
    frontier_minds_matter_encounter_cohort.visit_key as primary_key,
    frontier_minds_matter_encounter_cohort.department_name as drill_down_one,
    frontier_minds_matter_encounter_cohort.provider_name as drill_down_two,
    frontier_minds_matter_encounter_cohort.encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_minds_matter_ps_step' as metric_id,
    frontier_minds_matter_encounter_cohort.pat_key as num
from
    {{ ref('frontier_minds_matter_encounter_cohort') }} as frontier_minds_matter_encounter_cohort
    left join {{ ref('smart_data_element_all') }} as smart_data_element_all
        on frontier_minds_matter_encounter_cohort.visit_key = smart_data_element_all.visit_key
where
    minds_matter_patient_ind = '1'
    and lower(concept_id) = 'choportho#331'
