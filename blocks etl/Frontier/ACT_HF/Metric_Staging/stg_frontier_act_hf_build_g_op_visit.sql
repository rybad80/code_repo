-- g:growth
select
    'ACT-HF: Outpatient Visits' as metric_name,
    visit_key as primary_key,
    specialty_name as drill_down_one,
    visit_type as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_act_hf_op_visits' as metric_id,
    visit_key as num
from
    {{ ref('frontier_act_hf_encounter_cohort')}}
where
    act_hf_inpatient_ind = 0
