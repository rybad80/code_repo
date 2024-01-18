select
    'CVA: Outpatients - Oncology Day Visits' as metric_name,
    visit_key as primary_key,
    visit_type as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_cva_op_onco_day_visit' as metric_id,
    visit_key as num
from
    {{ ref('frontier_cva_encounter_cohort') }}
where
    cva_onco_day_ind = '1'
    and visit_type_id != '0'