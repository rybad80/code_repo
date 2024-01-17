select
    'Program-Specific: Surgeries' as metric_name,
    frontier_heart_valve_encounter_cohort.visit_key as primary_key,
    stg_heart_valve_surgery.surgeon as drill_down_one,
    stg_heart_valve_surgery.surgery_type as drill_down_two,
    stg_heart_valve_surgery.surg_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_heart_valve_surgery' as metric_id,
    frontier_heart_valve_encounter_cohort.visit_key as num
from
    {{ ref('frontier_heart_valve_encounter_cohort')}} as frontier_heart_valve_encounter_cohort
    inner join {{ ref('stg_heart_valve_surgery')}} as stg_heart_valve_surgery
        on frontier_heart_valve_encounter_cohort.visit_key = stg_heart_valve_surgery.visit_key
            and frontier_heart_valve_encounter_cohort.heart_valve_surgery_ind = '1'
