select distinct
    'CTIS: Outpatient Visits' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'frontier_ctis_op_visits' as metric_id,
    visit_key as num
from
    {{ ref('ctis_outpatient_visits')}}
