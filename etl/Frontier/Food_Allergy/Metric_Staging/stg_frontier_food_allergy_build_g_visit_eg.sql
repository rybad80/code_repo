select
    'Food Allergy (Eg): Outpatient Visits' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'frontier_fa_eg_visit' as metric_id,
    visit_key as num
from
    {{ ref('frontier_food_allergy_encounter_cohort')}}
where
    eg_ind = 1
