select
    cost_center_name,
    cost_center_site_name,
    budget_date,
    budget
from
    {{ ref('lookup_service_line_budgets') }}
where
    lower(service_line) = 'neonatology'
