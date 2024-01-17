select
    post_date_month,
    cost_center_description as drill_down_one,
    cost_center_site_name as drill_down_two,
    'count' as metric_type,
    sum(specialty_care_visit_budget) as specialty_care_visit_budget,
    'spec_visit' as metric_id
from
    {{ ref('finance_sc_visit_month_cost_center')}}
group by
    post_date_month,
    cost_center_description,
    cost_center_site_name
