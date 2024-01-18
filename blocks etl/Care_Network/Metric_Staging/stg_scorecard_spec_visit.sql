select
    'operational' as domain, 
    'Specialty Care Physician Visits' as metric_name, 
    {{
        dbt_utils.surrogate_key([
            'post_date',
            'cost_center_id',
            'cost_center_site_id',
            'specialty_care_visit_type'
        ])
    }} as primary_key,
    post_date as metric_date,
    cost_center_description as drill_down_one, 
    cost_center_site_name as drill_down_two,
    specialty_care_visit_actual as num,
    cost_center_id, 
    cost_center_site_id,
    'sum' as num_calculation,
    'count' as metric_type, 
    'up' as desired_direction,
    'spec_visit' as metric_id
from
    {{ref('finance_sc_visit_daily_cost_center')}}
where
    post_date < current_date
