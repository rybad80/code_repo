select
    'Rare Lung: Telemedicine Visits' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_rare_lung_telemed' as metric_id,
    visit_key as num
from
    {{ ref('frontier_rare_lung_encounter_cohort')}}
where
    visit_type_id in ('2124', '2088')   --'video visit follow up', 'video visit new'
    or encounter_type_id = '76'         --'telemedicine'
