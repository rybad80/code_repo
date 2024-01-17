select
    'Program-Specific: MRI' as metric_name,
    frontier_heart_valve_encounter_cohort.visit_key as primary_key,
    stg_heart_valve_mri.procedure_order_description as drill_down_one,
    stg_heart_valve_mri.patient_class as drill_down_two,
    stg_heart_valve_mri.encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_heart_valve_mri' as metric_id,
    frontier_heart_valve_encounter_cohort.visit_key as num
from
    {{ ref('frontier_heart_valve_encounter_cohort')}} as frontier_heart_valve_encounter_cohort
    inner join {{ ref('stg_heart_valve_mri')}} as stg_heart_valve_mri
        on frontier_heart_valve_encounter_cohort.visit_key = stg_heart_valve_mri.visit_key
            and frontier_heart_valve_encounter_cohort.heart_valve_mri_ind = '1'
