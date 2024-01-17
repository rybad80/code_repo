-- g:growth
select
    'ENGIN: Outpatients (Unique)' as metric_name,
    visit_key as primary_key,
    stg_patient.race as drill_down_one,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_engin_op_unique' as metric_id,
    frontier_engin_encounter_cohort.pat_key as num
from
    {{ ref('frontier_engin_encounter_cohort')}} as frontier_engin_encounter_cohort
inner join {{ ref('stg_patient')}} as stg_patient
        on frontier_engin_encounter_cohort.pat_key = stg_patient.pat_key
where
    engin_inpatient_ind = 0
