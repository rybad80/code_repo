select
    'HI: Inpatient Visits' as metric_name,
    visit_key as primary_key,
    visit_type as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'frontier_hi_ip_visits' as metric_id,
    visit_key as num
from
    {{ ref('frontier_hi_encounter_cohort')}}
where
    inpatient_ind = 1
