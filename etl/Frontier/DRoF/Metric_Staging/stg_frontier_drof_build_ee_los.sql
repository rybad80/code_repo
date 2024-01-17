-- ee:effective efficient
select
    'DRoF: Average Inpatient Length of Stay' as metric_name,
    visit_key as primary_key,
    sub_cohort as drill_down_one,
    department_name as drill_down_two,
    hospital_discharge_date as metric_date,
    inpatient_los_days as num,
    visit_key as denom,
    'sum' as num_calculation,
    'count' as denom_calculation,
    'rate' as metric_type,
    'down' as direction,
    'fp_drof_los' as metric_id
from
    {{ ref('frontier_drof_encounter_cohort')}}
where
    inpatient_ind = 1 and drof_sub_cohort_ind = 1
