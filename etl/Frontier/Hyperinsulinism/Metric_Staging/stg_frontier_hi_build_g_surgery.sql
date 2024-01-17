select
    'Program-Specific: Surgeries' as metric_name,
    visit_key as primary_key,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'frontier_hi_surgery' as metric_id,
    visit_key as num
from
    {{ ref('frontier_hi_encounter_cohort')}}
where
    surgery_ind = 1
    and provider_id in ('3203')     --'adzick, n scott'
    and lower(department_name) = 'wood ped gen thor surg'
