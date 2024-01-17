select
    case
        when cost_center_site_name = 'Philadelphia Campus' then 'Philadelphia Campus'
        else 'Satellite Campus'
    end as cost_center_site_group,
    cost_center_name || '-' || cost_center_site_group as cost_center_name_site_group,
    budget_date,
    sum(budget) as budget
from
    {{ ref('lookup_service_line_budgets') }}
where
    lower(service_line) = 'cardiac'
group by
    cost_center_site_name,
    cost_center_name_site_group,
    budget_date
