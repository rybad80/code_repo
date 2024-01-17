select
    'Heart Valve: Average Inpatient Length of Stay' as metric_name,
    frontier_heart_valve_encounter_cohort.visit_key as primary_key,
    frontier_heart_valve_encounter_cohort.admission_department as drill_down,
    frontier_heart_valve_encounter_cohort.encounter_date as metric_date,
    encounter_inpatient.inpatient_los_days as num,
    frontier_heart_valve_encounter_cohort.visit_key as denom,
    'sum' as num_calculation,
    'count' as denom_calculation,
    'rate' as metric_type,
    'down' as direction,
    'fp_heart_valve_los' as metric_id
from
    {{ ref('frontier_heart_valve_encounter_cohort') }} as frontier_heart_valve_encounter_cohort
    inner join {{ ref('encounter_inpatient') }} as encounter_inpatient
        on frontier_heart_valve_encounter_cohort.visit_key = encounter_inpatient.visit_key
    left join {{ ref('cardiac_unit_encounter') }} as cardiac_unit_encounter
        on frontier_heart_valve_encounter_cohort.visit_key = cardiac_unit_encounter.visit_key
