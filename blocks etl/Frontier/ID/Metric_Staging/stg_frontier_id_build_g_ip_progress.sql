select
    'IDFP: Inpatients - Progress Notes' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'sum' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_id_ip_progress_notes' as metric_id,
    id_ip_progress_sum as num
from
    {{ ref('frontier_id_encounter_cohort') }}
where
    id_ip_consult_ind = 1
    and id_ip_progress_sum > 0
