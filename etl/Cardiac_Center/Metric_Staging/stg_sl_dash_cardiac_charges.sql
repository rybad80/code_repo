select
    date_trunc('day', finance_charges_actual.post_date) as post_date,
    finance_charges_actual.cost_center_id,
    finance_charges_actual.cost_center_site_id,
    finance_charges_actual.cost_center_name,
    finance_charges_actual.cost_center_site_name,
    case
        when finance_charges_actual.cost_center_site_name = 'Philadelphia Campus' then 'Philadelphia Campus' 
        else 'Satellite Campus'
    end as cost_center_site_group,
    finance_charges_actual.cost_center_name || '-'||cost_center_site_group as cost_center_name_site_group,
    sum(finance_charges_actual.charges_actual) as charges_actual,
    {{
        dbt_utils.surrogate_key([
            'finance_charges_actual.post_date',
            'finance_charges_actual.cost_center_id',
            'finance_charges_actual.cost_center_site_id'
        ])
    }} as primary_key,
    'cardiac_dept_charges' as metric_id
from
    {{ ref('finance_charges_actual') }} as finance_charges_actual
    inner join {{ ref('lookup_cost_center_service_line') }} as lookup_cost_center_service_line
        on finance_charges_actual.cost_center_id = lookup_cost_center_service_line.cost_center_gl_id
where
    lower(lookup_cost_center_service_line.service_line) = 'cardiac center'
group by
    post_date,
    finance_charges_actual.cost_center_id,
    finance_charges_actual.cost_center_site_id,
    finance_charges_actual.cost_center_name,
    finance_charges_actual.cost_center_site_name
