select
    'Program-Specific: RBD Patients (Unique)' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_n_o_t_bleeding_rbd_pat' as metric_id,
    pat_key as num
from
    {{ ref('frontier_n_o_t_bleeding_encounter_cohort') }}
where
    sub_cohort = 'RBD patient'
