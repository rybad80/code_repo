{{
    config(
        materialized='incremental',
        unique_key = 'plan_entry_line_wid',
        meta = {
            'critical': true
        }
    )
}}
with plan_line_stage
as (
select
    workday_plan_lines.plan_structure_reference_id,
    workday_plan_lines.plan_type_reference_id,
    workday_plan_lines.plan_name_wid,
    workday_plan_lines.plan_entry_line_wid,
    workday_plan_lines.line_order,
    workday_plan_lines.transaction_debit_amount,
    workday_plan_lines.transaction_credit_amount,
    workday_plan_lines.ledger_budget_debit_amount,
    workday_plan_lines.ledger_budget_credit_amount,
    workday_plan_lines.quantity,
    workday_plan_lines.line_memo,
    workday_plan_lines.year,
    workday_plan_lines.period,
    workday_plan_lines.fiscal_year_reference_id,
    workday_plan_lines.fiscal_period_reference_id,
    workday_plan_lines.fiscal_schedule,
    workday_plan_lines.fiscal_summary_schedule,
    workday_plan_lines.ledger_account_reference_id,
    workday_plan_lines.account_set_reference_id,
    workday_plan_lines.revenue_category_reference_id,
    workday_plan_lines.spend_category_reference_id,
    workday_plan_lines.company_reference_id,
    workday_plan_lines.intercompany_affiliate_reference_id,
    workday_plan_lines.gl_company_reference_id,
    workday_plan_lines.cost_center_reference_id,
    workday_plan_lines.cost_center_site_reference_id,
    workday_plan_lines.location_reference_id,
    workday_plan_lines.award_reference_id,
    workday_plan_lines.grant_reference_id,
    workday_plan_lines.project_reference_id,
    workday_plan_lines.program_reference_id,
    workday_plan_lines.fund_reference_id,
    workday_plan_lines.gift_reference_id,
    workday_plan_lines.provider_reference_id,
    workday_plan_lines.payor_reference_id,
    workday_plan_lines.worker_employee_id,
    workday_plan_lines.job_profile_reference_id,
    workday_plan_lines.position_reference_id,
    workday_plan_lines.last_functionally_updated,
    workday_plan_lines.plan_status,
    workday_plan_lines.plan_period_start_date,
    workday_plan_lines.plan_period_end_date,
    workday_plan_lines.schedule_year,
    workday_plan_lines.schedule_period,
    workday_plan_lines.upd_dt
from
    {{source('workday_ods', 'workday_plan_lines')}} as workday_plan_lines
where 1 = 1 
    {%- if is_incremental() %}
    and workday_plan_lines.upd_dt > (select max(update_date) from {{ this }})
    {%- endif %}
--
union all
--
select
    workday_plan_line_awards.plan_structure_reference_id,
    workday_plan_line_awards.plan_type_reference_id,
    workday_plan_line_awards.plan_name_wid,
    workday_plan_line_awards.plan_entry_line_wid,
    workday_plan_line_awards.line_order,
    workday_plan_line_awards.transaction_debit_amount,
    workday_plan_line_awards.transaction_credit_amount,
    workday_plan_line_awards.ledger_budget_debit_amount,
    workday_plan_line_awards.ledger_budget_credit_amount,
    workday_plan_line_awards.quantity,
    workday_plan_line_awards.line_memo,
    workday_plan_line_awards.year,
    workday_plan_line_awards.period,
    workday_plan_line_awards.fiscal_year_reference_id,
    workday_plan_line_awards.fiscal_period_reference_id,
    workday_plan_line_awards.fiscal_schedule,
    workday_plan_line_awards.fiscal_summary_schedule,
    workday_plan_line_awards.ledger_account_reference_id,
    workday_plan_line_awards.account_set_reference_id,
    workday_plan_line_awards.revenue_category_reference_id,
    workday_plan_line_awards.spend_category_reference_id,
    workday_plan_line_awards.company_reference_id,
    workday_plan_line_awards.intercompany_affiliate_reference_id,
    workday_plan_line_awards.gl_company_reference_id,
    workday_plan_line_awards.cost_center_reference_id,
    workday_plan_line_awards.cost_center_site_reference_id,
    workday_plan_line_awards.location_reference_id,
    workday_plan_line_awards.award_reference_id,
    workday_plan_line_awards.grant_reference_id,
    workday_plan_line_awards.project_reference_id,
    workday_plan_line_awards.program_reference_id,
    workday_plan_line_awards.fund_reference_id,
    workday_plan_line_awards.gift_reference_id,
    workday_plan_line_awards.provider_reference_id,
    workday_plan_line_awards.payor_reference_id,
    workday_plan_line_awards.worker_employee_id,
    workday_plan_line_awards.job_profile_reference_id,
    workday_plan_line_awards.position_reference_id,
    workday_plan_line_awards.last_functionally_updated,
    workday_plan_line_awards.plan_status,
    workday_plan_line_awards.plan_period_start_date,
    workday_plan_line_awards.plan_period_end_date,
    workday_plan_line_awards.schedule_year,
    workday_plan_line_awards.schedule_period,
    workday_plan_line_awards.upd_dt
from
    {{source('workday_ods', 'workday_plan_line_awards')}} as workday_plan_line_awards
where 1 = 1 
    {%- if is_incremental() %}
    and workday_plan_line_awards.upd_dt > (select max(update_date) from {{ this }})
    {%- endif %}
)
select
    workday_plan_lines.plan_structure_reference_id,
    workday_plan_lines.plan_type_reference_id,
    workday_plan_lines.plan_name_wid,
    workday_plan_lines.plan_entry_line_wid,
    workday_plan_lines.line_order,
    workday_plan_lines.transaction_debit_amount,
    workday_plan_lines.transaction_credit_amount,
    workday_plan_lines.ledger_budget_debit_amount,
    workday_plan_lines.ledger_budget_credit_amount,
    workday_plan_lines.quantity,
    workday_plan_lines.line_memo,
    workday_plan_lines.year,
    workday_plan_lines.period,
    workday_plan_lines.fiscal_year_reference_id,
    workday_plan_lines.fiscal_period_reference_id,
    workday_plan_lines.fiscal_schedule,
    workday_plan_lines.fiscal_summary_schedule,
    workday_plan_lines.ledger_account_reference_id,
    coalesce(dim_ledger_account.ledger_account_key, 0) as ledger_account_key,
    workday_plan_lines.account_set_reference_id,
    workday_plan_lines.revenue_category_reference_id,
    coalesce(dim_revenue_category.revenue_category_key, 0) as revenue_category_key,
    workday_plan_lines.spend_category_reference_id,
    coalesce(dim_spend_category.spend_category_key, 0) as spend_category_key,
    workday_plan_lines.company_reference_id,
    coalesce(dim_company.company_key, 0) as company_key,
    workday_plan_lines.intercompany_affiliate_reference_id,
    workday_plan_lines.gl_company_reference_id,
    coalesce(dim_company_gl.company_key, 0) as gl_company_key,
    workday_plan_lines.cost_center_reference_id,
    coalesce(dim_cost_center.cost_center_key, 0) as cost_center_key,
    workday_plan_lines.cost_center_site_reference_id,
    coalesce(dim_cost_center_site.cost_center_site_key, 0) as cost_center_site_key,
    workday_plan_lines.location_reference_id,
    workday_plan_lines.award_reference_id,
    workday_plan_lines.grant_reference_id,
    coalesce(dim_grant.grant_key, 0) as grant_key,
    workday_plan_lines.project_reference_id,
    workday_plan_lines.program_reference_id,
    coalesce(dim_program.program_key, 0) as program_key,
    workday_plan_lines.fund_reference_id,
    coalesce(dim_fund.fund_key, 0) as fund_key,
    workday_plan_lines.gift_reference_id,
    workday_plan_lines.provider_reference_id,
    workday_plan_lines.payor_reference_id,
    coalesce(dim_payor.payor_key, 0) as payor_key,
    workday_plan_lines.worker_employee_id,
    workday_plan_lines.job_profile_reference_id,
    workday_plan_lines.position_reference_id,
    workday_plan_lines.last_functionally_updated,
    workday_plan_lines.plan_status,
    workday_plan_lines.plan_period_start_date,
    workday_plan_lines.plan_period_end_date,
    workday_plan_lines.schedule_year,
    workday_plan_lines.schedule_period,
    workday_plan_lines.upd_dt as update_date
from
    plan_line_stage as workday_plan_lines
    left join {{ref('dim_company')}} as dim_company
        on dim_company.integration_id = 'WORKDAY~' || workday_plan_lines.company_reference_id
    left join {{ref('dim_company')}} as dim_company_gl
        on dim_company_gl.integration_id = 'WORKDAY~' || workday_plan_lines.gl_company_reference_id
    left join {{ref('dim_ledger_account')}} as dim_ledger_account
    on dim_ledger_account.integration_id = 'WORKDAY~' || workday_plan_lines.ledger_account_reference_id
    left join {{ref('dim_cost_center')}} as dim_cost_center
        on dim_cost_center.integration_id = 'WORKDAY~' || workday_plan_lines.cost_center_reference_id
    left join {{ref('dim_cost_center_site')}} as dim_cost_center_site
        on dim_cost_center_site.integration_id = 'WORKDAY~' || workday_plan_lines.cost_center_site_reference_id
    left join {{ref('dim_grant')}} as dim_grant
        on dim_grant.integration_id = 'WORKDAY~' || workday_plan_lines.grant_reference_id
    left join {{ref('dim_fund')}} as dim_fund
        on dim_fund.integration_id = 'WORKDAY~' || workday_plan_lines.fund_reference_id
    left join {{ref('dim_payor')}} as dim_payor
        on dim_payor.integration_id = 'WORKDAY~' || workday_plan_lines.payor_reference_id
    left join {{ref('dim_program')}} as dim_program
        on dim_program.integration_id = 'WORKDAY~' || workday_plan_lines.program_reference_id
    left join {{ref('dim_revenue_category')}} as dim_revenue_category
        on dim_revenue_category.integration_id = 'WORKDAY~' || workday_plan_lines.revenue_category_reference_id
    left join {{ref('dim_spend_category')}} as dim_spend_category
        on dim_spend_category.integration_id = 'WORKDAY~' || workday_plan_lines.spend_category_reference_id
