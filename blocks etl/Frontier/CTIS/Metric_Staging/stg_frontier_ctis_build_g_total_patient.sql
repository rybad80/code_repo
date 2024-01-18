select
    'CTIS: Total Patients (Unique)' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'frontier_ctis_totpat_unique' as metric_id,
    pat_key as num
from
    {{ ref('frontier_ctis_encounter_cohort')}}
where
    ctis_event_ind = 1
