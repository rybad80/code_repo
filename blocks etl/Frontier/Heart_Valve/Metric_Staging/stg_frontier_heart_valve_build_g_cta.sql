select
    'Program-Specific: CTA' as metric_name,
    frontier_heart_valve_encounter_cohort.visit_key as primary_key,
    stg_heart_valve_cta.procedure_name as drill_down_one,
    --stg_heart_valve_cta.echo_type as drill_down_two,
    stg_heart_valve_cta.encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_heart_valve_cta' as metric_id,
    frontier_heart_valve_encounter_cohort.visit_key as num
from
    {{ ref('frontier_heart_valve_encounter_cohort')}} as frontier_heart_valve_encounter_cohort
    inner join {{ ref('stg_heart_valve_cta')}} as stg_heart_valve_cta
        on frontier_heart_valve_encounter_cohort.visit_key = stg_heart_valve_cta.visit_key
            and frontier_heart_valve_encounter_cohort.heart_valve_cta_ind = '1'