select
    'IDFP: Average Inpatient Length of Stay' as metric_name,
    frontier_id_encounter_cohort.visit_key as primary_key,
    frontier_id_encounter_cohort.department_name as drill_down_one,
    frontier_id_encounter_cohort.encounter_date as metric_date,
    encounter_inpatient.inpatient_los_days as num,
    frontier_id_encounter_cohort.visit_key as denom,
    'sum' as num_calculation,
    'count' as denom_calculation,
    'rate' as metric_type,
    'down' as direction,
    'fp_id_los' as metric_id
from
    {{ ref('frontier_id_encounter_cohort') }} as frontier_id_encounter_cohort
    inner join {{ ref('encounter_inpatient') }} as encounter_inpatient
        on frontier_id_encounter_cohort.visit_key = encounter_inpatient.visit_key
where
    frontier_id_encounter_cohort.id_ip_consult_ind = '1'
    and frontier_id_encounter_cohort.id_ip_progress_sum > 0