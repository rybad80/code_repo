select
    'RL-Program Specific: Surgical Biopsy' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_rare_lung_ps_surg_bx' as metric_id,
    pat_key as num
from
    {{ ref('frontier_rare_lung_encounter_cohort')}}
where
    surgical_bx_ind = 1
