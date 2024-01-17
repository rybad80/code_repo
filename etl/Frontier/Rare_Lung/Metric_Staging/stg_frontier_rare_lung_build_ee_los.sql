select
    'Rare Lung: Average Inpatient Length of Stay' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    encounter_date as metric_date,
    inpatient_los_days as num,
    visit_key as denom,
    'sum' as num_calculation,
    'count' as denom_calculation,
    'rate' as metric_type,
    'down' as direction,
    'fp_rare_lung_los' as metric_id
from
    {{ ref('frontier_rare_lung_encounter_cohort')}}
where
    rare_lung_ip_ind = 1
    and ip_by_note_only_ind = '0'
    and inpatient_los_days is not null
