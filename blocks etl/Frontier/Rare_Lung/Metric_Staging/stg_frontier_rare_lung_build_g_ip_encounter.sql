select
    'Rare Lung: Inpatient Encounters' as metric_name,
    visit_key as primary_key,
    visit_type as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_rare_lung_ip_enc' as metric_id,
    visit_key as num
from
    {{ ref('frontier_rare_lung_encounter_cohort')}}
where
    rare_lung_ip_ind = 1
    and encounter_type_id = 3
    and ip_by_note_only_ind = 0
