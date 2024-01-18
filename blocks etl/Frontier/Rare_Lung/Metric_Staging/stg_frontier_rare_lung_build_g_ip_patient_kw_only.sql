select
    'Rare Lung: Potential Inpatients (Unique)' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_rare_lung_ip_unique_kw_only' as metric_id,
    pat_key as num
from
    {{ ref('frontier_rare_lung_encounter_cohort')}}
where
    rare_lung_ip_ind = 1
    and ip_by_note_only_ind = '1'
