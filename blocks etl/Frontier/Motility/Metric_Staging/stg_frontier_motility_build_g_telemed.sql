select
    'Motility: Telemedicine Visits' as metric_name,
    visit_key as primary_key,
    provider_name as drill_down_one,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_motility_telemed' as metric_id,
    visit_key as num
from
    {{ ref('frontier_motility_encounter_cohort')}}
where
    visit_type_id in ('2124', '2088')   --'video visit follow up', 'video visit new'
    or encounter_type_id = '76'         --'telemedicine'
