select
    'Food Allergy (EoE): Outpatients' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'frontier_fa_eoe_pat' as metric_id,
    pat_key as num
from
    {{ ref('frontier_food_allergy_encounter_cohort')}}
where
    eoe_ind = 1
    and pat_per_fy_seq_num = 1
