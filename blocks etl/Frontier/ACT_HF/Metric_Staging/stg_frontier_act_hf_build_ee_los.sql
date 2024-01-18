-- ee:effective efficient
select
    'ACT-HF: Average Inpatient Length of Stay' as metric_name,
    encounter_cohort.visit_key as primary_key,
    encounter_cohort.department_name as drill_down_one,
    encounter_cohort.encounter_date as metric_date,
    encounter_inpatient.inpatient_los_days as num,
    encounter_cohort.visit_key as denom,
    'sum' as num_calculation,
    'count' as denom_calculation,
    'rate' as metric_type,
    'down' as direction,
    'fp_act_hf_los' as metric_id
from
    {{ ref('frontier_act_hf_encounter_cohort')}} as encounter_cohort
    inner join {{ ref('encounter_inpatient')}} as encounter_inpatient
        on encounter_cohort.visit_key = encounter_inpatient.visit_key
