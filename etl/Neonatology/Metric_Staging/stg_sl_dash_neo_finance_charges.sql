select
    {{
        dbt_utils.surrogate_key([
            'finance_charges_actual.cost_center_id',
            'finance_charges_actual.cost_center_site_id',
            'finance_charges_actual.payor_id',
            'finance_charges_actual.post_date',
            'finance_charges_actual.revenue_category_id'
        ])
    }} as primary_key,
    date_trunc('day', finance_charges_actual.post_date) as metric_date,
    finance_charges_actual.cost_center_name,
    finance_charges_actual.cost_center_site_name,
    finance_charges_actual.charges_actual
from
    {{ ref('finance_charges_actual') }} as finance_charges_actual
    inner join {{ ref('lookup_cost_center_service_line') }} as lookup_cost_center_service_line
        on finance_charges_actual.cost_center_id = lookup_cost_center_service_line.cost_center_gl_id
where
    lower(lookup_cost_center_service_line.service_line) = 'neonatology'
