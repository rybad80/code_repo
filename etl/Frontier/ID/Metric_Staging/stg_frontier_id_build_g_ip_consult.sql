select
    'IDFP: Inpatients - Consults' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_id_ip_consults' as metric_id,
    visit_key as num
from
    {{ ref('frontier_id_encounter_cohort') }}
where
    id_ip_consult_ind = '1'
