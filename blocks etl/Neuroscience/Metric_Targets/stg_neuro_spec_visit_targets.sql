select
    post_date_month,
    'count' as metric_type,
    sum(specialty_care_visit_budget) as specialty_care_visit_budget,
    'neuro_new_scc_phys_visits' as metric_id
from
    {{ ref('finance_sc_visit_month_cost_center')}}
where
    lower(cost_center_description) in ('neurology', 'neurosurgery')
    and lower(specialty_care_visit_type) = 'new outpatient physician visits'
group by
    post_date_month
