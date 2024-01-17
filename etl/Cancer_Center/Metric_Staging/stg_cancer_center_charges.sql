{{ config(meta = {
    'critical': false
}) }}

select
    date_trunc('day', finance_charges_actual.post_date) as post_date,
    finance_charges_actual.cost_center_id,
    finance_charges_actual.cost_center_site_id,
    finance_charges_actual.cost_center_name,
    finance_charges_actual.cost_center_site_name,
    finance_charges_actual.cost_center_name || '-'|| cost_center_site_name as cost_center_name_site_group,
    sum(finance_charges_actual.charges_actual) as charges_actual,
    {{
        dbt_utils.surrogate_key([
            'post_date',
            'cost_center_name_site_group'
        ])
    }} as primary_key
from
    {{ ref('finance_charges_actual') }} as finance_charges_actual
    inner join {{ ref('lookup_cost_center_service_line') }} as lookup_cost_center_service_line
        on finance_charges_actual.cost_center_id = lookup_cost_center_service_line.cost_center_gl_id
where
    lower(lookup_cost_center_service_line.service_line) = 'oncology'
group by
    post_date,
    finance_charges_actual.cost_center_id,
    finance_charges_actual.cost_center_site_id,
    finance_charges_actual.cost_center_name,
    finance_charges_actual.cost_center_site_name
