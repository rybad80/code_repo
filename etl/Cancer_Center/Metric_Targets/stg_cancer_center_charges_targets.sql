select
    cost_center_name || '-' || cost_center_site_name as cost_center_name_site_group,
    budget_date,
    sum(budget) as budget
from
    {{ ref('lookup_service_line_budgets') }}
where
    lower(service_line) = 'oncology'
group by
    cost_center_name_site_group,
    budget_date
