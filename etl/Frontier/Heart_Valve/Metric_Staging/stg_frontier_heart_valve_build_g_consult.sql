select
    'Program-Specific: Heart Valve Conference Notes' as metric_name,
    stg_heart_valve_notes.visit_key as primary_key,
    stg_heart_valve_notes.provider_name as drill_down_one,
    stg_heart_valve_notes.note_encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_heart_valve_consult' as metric_id,
    stg_heart_valve_notes.visit_key as num
from
    {{ ref('frontier_heart_valve_encounter_cohort')}} as frontier_heart_valve_encounter_cohort
    inner join {{ ref('stg_heart_valve_notes')}} as stg_heart_valve_notes
        on frontier_heart_valve_encounter_cohort.visit_key = stg_heart_valve_notes.visit_key
            and frontier_heart_valve_encounter_cohort.heart_valve_notes_ind = '1'
