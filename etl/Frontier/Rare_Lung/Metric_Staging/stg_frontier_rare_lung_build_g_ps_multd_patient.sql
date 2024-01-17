select
    'RL-Program Specific: Multi-D Patients' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_rare_lung_ps_multid' as metric_id,
    pat_key as num
from
    {{ ref('encounter_all')}}
where
    lower(visit_type_id) in (
        '3006',   --idfp new
        '3007'    --idfp fol
    )
    and provider_id = '28429'   --young, lisa
