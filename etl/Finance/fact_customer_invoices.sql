{{
    config(
        materialized='incremental',
        unique_key = 'cust_inv_line_wid',
        meta = {
            'critical': true
        }
    )
}}
with cust_inv_stage
as (
select
    workday_customer_invoices.cust_inv_reference_id,
    workday_customer_invoices.cust_inv_wid,
    workday_customer_invoices.cust_inv_number,
    workday_customer_invoices.cust_inv_line_reference_id,
    workday_customer_invoices.cust_inv_line_wid,
    workday_customer_invoices.cust_inv_line_order,
    workday_customer_invoices.cust_inv_is_adjustment,
    workday_customer_invoices.cust_inv_adj_reason,
    workday_customer_invoices.sponsor_reference_id,
    workday_customer_invoices.sponsor_wid,
    workday_customer_invoices.customer_reference_id,
    workday_customer_invoices.customer_wid,
    workday_customer_invoices.cust_inv_status,
    workday_customer_invoices.cust_inv_type,
    workday_customer_invoices.cust_inv_type_is_revenue,
    workday_customer_invoices.cust_inv_date,
    workday_customer_invoices.cust_inv_document_date,
    workday_customer_invoices.cust_inv_period,
    workday_customer_invoices.cust_inv_line_amount,
    workday_customer_invoices.cust_inv_line_amount_paid,
    workday_customer_invoices.cust_inv_paid_date,
    workday_customer_invoices.cust_inv_line_payment_status,
    workday_customer_invoices.cust_inv_payment_type,
    workday_customer_invoices.cust_inv_payment_reference,
    workday_customer_invoices.company_reference_id,
    workday_customer_invoices.cost_center_reference_id,
    workday_customer_invoices.cost_center_site_reference_id,
    workday_customer_invoices.award_reference_id,
    workday_customer_invoices.grant_reference_id,
    workday_customer_invoices.grant_cost_sharing_reference_id,
    workday_customer_invoices.project_reference_id,
    workday_customer_invoices.fund_reference_id,
    workday_customer_invoices.program_reference_id,
    workday_customer_invoices.provider_reference_id,
    workday_customer_invoices.object_class_reference_id,
    workday_customer_invoices.object_class_wid,
    workday_customer_invoices.last_updated_date,
    workday_customer_invoices.location_reference_id,
    workday_customer_invoices.payor_reference_id,
    workday_customer_invoices.supplier_reference_id,
    workday_customer_invoices.employee_reference_id,
    workday_customer_invoices.upd_dt

from
    {{source('workday_ods', 'workday_customer_invoices')}} as workday_customer_invoices
where 1 = 1 
    {%- if is_incremental() %}
    and workday_customer_invoices.upd_dt > (select max(update_date) from {{ this }})
    {%- endif %}
)

select
    workday_customer_invoices.cust_inv_reference_id,
    workday_customer_invoices.cust_inv_wid,
    workday_customer_invoices.cust_inv_number,
    workday_customer_invoices.cust_inv_line_reference_id,
    workday_customer_invoices.cust_inv_line_wid,
    workday_customer_invoices.cust_inv_line_order,
    workday_customer_invoices.cust_inv_is_adjustment,
    workday_customer_invoices.cust_inv_adj_reason,
    workday_customer_invoices.sponsor_reference_id,
    workday_customer_invoices.customer_reference_id,
    workday_customer_invoices.cust_inv_status,
    workday_customer_invoices.cust_inv_type,
    workday_customer_invoices.cust_inv_type_is_revenue,
    workday_customer_invoices.cust_inv_date,
    workday_customer_invoices.cust_inv_document_date,
    workday_customer_invoices.cust_inv_period,
    workday_customer_invoices.cust_inv_line_amount,
    workday_customer_invoices.cust_inv_line_amount_paid,
    workday_customer_invoices.cust_inv_paid_date,
    workday_customer_invoices.cust_inv_line_payment_status,
    workday_customer_invoices.cust_inv_payment_type,
    workday_customer_invoices.cust_inv_payment_reference,
    workday_customer_invoices.grant_cost_sharing_reference_id,
    workday_customer_invoices.object_class_reference_id,
    workday_customer_invoices.location_reference_id,
    workday_customer_invoices.payor_reference_id,
    coalesce(dim_payor.payor_key, 0) as payor_key,
    workday_customer_invoices.supplier_reference_id,
    workday_customer_invoices.company_reference_id,
    coalesce(dim_company.company_key, 0) as company_key,
    workday_customer_invoices.cost_center_reference_id,
    coalesce(dim_cost_center.cost_center_key, 0) as cost_center_key,
    workday_customer_invoices.cost_center_site_reference_id,
    coalesce(dim_cost_center_site.cost_center_site_key, 0) as cost_center_site_key,
    workday_customer_invoices.award_reference_id,
    workday_customer_invoices.grant_reference_id,
    coalesce(dim_grant.grant_key, 0) as grant_key,
    workday_customer_invoices.project_reference_id,
    coalesce(dim_project.project_key, 0) as project_key,
    workday_customer_invoices.program_reference_id,
    coalesce(dim_program.program_key, 0) as program_key,
    workday_customer_invoices.fund_reference_id,
    coalesce(dim_fund.fund_key, 0) as fund_key,
    workday_customer_invoices.provider_reference_id,
    workday_customer_invoices.upd_dt as update_date
from
    cust_inv_stage as workday_customer_invoices
    left join {{ref('dim_company')}} as dim_company
        on dim_company.integration_id = 'WORKDAY~' || workday_customer_invoices.company_reference_id
    left join {{ref('dim_cost_center')}} as dim_cost_center
        on dim_cost_center.integration_id = 'WORKDAY~' || workday_customer_invoices.cost_center_reference_id
    left join {{ref('dim_cost_center_site')}} as dim_cost_center_site
        on dim_cost_center_site.integration_id
            = 'WORKDAY~' || workday_customer_invoices.cost_center_site_reference_id
    left join {{ref('dim_grant')}} as dim_grant
        on dim_grant.integration_id = 'WORKDAY~' || workday_customer_invoices.grant_reference_id
    left join {{ref('dim_payor')}} as dim_payor
        on dim_payor.integration_id = 'WORKDAY~' || workday_customer_invoices.payor_reference_id
    left join {{ref('dim_fund')}} as dim_fund
        on dim_fund.integration_id = 'WORKDAY~' || workday_customer_invoices.fund_reference_id
    left join {{ref('dim_program')}} as dim_program
        on dim_program.integration_id = 'WORKDAY~' || workday_customer_invoices.program_reference_id
    left join {{ref('dim_project')}} as dim_project
        on dim_project.integration_id = 'WORKDAY~' || workday_customer_invoices.project_reference_id
