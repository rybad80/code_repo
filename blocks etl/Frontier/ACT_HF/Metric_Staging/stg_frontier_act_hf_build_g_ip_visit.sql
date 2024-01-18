-- g:growth
select
    'ACT-HF: Inpatient Visits' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_act_hf_ip_visits' as metric_id,
    visit_key as num
from
    {{ ref('frontier_act_hf_encounter_cohort')}}
where
    act_hf_inpatient_ind = 1