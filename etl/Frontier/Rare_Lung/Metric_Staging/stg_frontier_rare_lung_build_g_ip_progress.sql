select
    'Rare Lung: Inpatients - Progress Notes' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_rare_lung_ip_progress_notes' as metric_id,
    pat_key as num
from
    {{ ref('frontier_rare_lung_encounter_cohort') }}
where
    rl_ip_progress_ind = 1
    --rl_ip_progress_sum > 0
    --and admit_start_date is not null
    --and (surgical_bx_ind != '1' or surgical_bx_ind is null)
