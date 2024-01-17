select
    'CTIS: Average Inpatient Length of Stay (CTIS surgery only)' as metric_name,
    frontier_ctis_encounter_cohort.visit_key as primary_key,
    frontier_ctis_encounter_cohort.encounter_date as metric_date,
    encounter_inpatient.inpatient_los_days as num,
    frontier_ctis_encounter_cohort.visit_key as denom,
    'sum' as num_calculation,
    'count' as denom_calculation,
    'rate' as metric_type,
    'down' as direction,
    'frontier_ctis_los' as metric_id
from
    {{ ref('frontier_ctis_encounter_cohort')}} as frontier_ctis_encounter_cohort
    inner join {{ ref('encounter_inpatient')}} as encounter_inpatient
        on frontier_ctis_encounter_cohort.visit_key = encounter_inpatient.visit_key
where
    surgery_encounter_ind = 1
