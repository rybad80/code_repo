select
    'CTIS: Inpatients (Unique)' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'frontier_ctis_ip_unique' as metric_id,
    pat_key as num
from
    {{ ref('frontier_ctis_encounter_cohort')}}
where
    inpatient_ind = 1
    and ctis_event_ind = 1
    --surgery_encounter_ind = 1 (or this instead? check with ctis team)
