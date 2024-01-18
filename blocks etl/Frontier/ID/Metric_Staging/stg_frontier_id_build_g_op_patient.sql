select
    'IDFP: Outpatients - (Unique)' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_id_op_unique' as metric_id,
    pat_key as num
from
    {{ ref('frontier_id_encounter_cohort') }}
where
    id_multi_d_ind = '1'
