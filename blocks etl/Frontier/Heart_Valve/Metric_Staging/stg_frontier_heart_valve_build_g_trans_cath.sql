select
    'Program-Specific: Transcatheter Valve Placement' as metric_name,
    frontier_heart_valve_encounter_cohort.visit_key as primary_key,
    initcap(stg_heart_valve_trans_cath.patient_class) as drill_down_one,
    stg_heart_valve_trans_cath.surgery_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_heart_valve_trans_cath' as metric_id,
    frontier_heart_valve_encounter_cohort.visit_key as num
from
    {{ ref('frontier_heart_valve_encounter_cohort')}} as frontier_heart_valve_encounter_cohort
    inner join {{ ref('stg_heart_valve_trans_cath')}} as stg_heart_valve_trans_cath
        on frontier_heart_valve_encounter_cohort.visit_key = stg_heart_valve_trans_cath.visit_key
            and frontier_heart_valve_encounter_cohort.heart_valve_trans_cath_ind = '1'
