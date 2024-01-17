{{
    config(
        materialized = 'incremental',
        unique_key = 'journal_entry_line_wid',
        meta = {
            'critical': true
        }
    )
}}

{% set column_names = 'journal_reference_id,
    journal_number,
    journal_wid,
    ledger_type,
    journal_line_wid as journal_entry_line_wid,
    line_number,
    line_order,
    payroll_result_wid,
    payroll_pay_period_reference_id,
    payroll_pay_period_wid,
    pay_period_start_date,
    pay_period_end_date,
    payroll_result_line_wid,
    transaction_debit_amount,
    transaction_credit_amount,
    journal_line_exchange_rate,
    ledger_debit_amount,
    ledger_credit_amount,
    estimated_facilities_and_administration_amount,
    gl_hours,
    allocated_hours,
    quantity,
    journal_line_memo,
    transaction_line_memo,
    external_reference_id,
    year,
    journal_line_year_reference_id,
    period,
    journal_line_period_reference_id,
    accounting_date,
    budget_date,
    company_reference_id,
    line_company_reference_id,
    intercompany_initiating_company_reference_id,
    intercompany_affiliate_reference_id,
    ledger_account_id,
    account_set_reference_id,
    revenue_category_reference_id,
    spend_category_reference_id,
    journal_source_reference_id,
    cost_center_reference_id,
    cost_center_site_ref_id as cost_center_site_reference_id,
    location_reference_id,
    employee_id,
    job_profile_reference_id,
    position_reference_id,
    pay_rate_type_reference_id,
    employee_type_reference_id,
    pay_component_reference_id,
    earning,
    deduction,
    calculation_type,
    deduction_code_reference_id,
    earning_code_reference_id,
    award_reference_id,
    grant_ref_id as grant_reference_id,
    project_ref_id as project_reference_id,
    program_ref_id as program_reference_id,
    fund_ref_id as fund_reference_id,
    gift_ref_id as gift_reference_id,
    provider_ref_id as provider_reference_id,
    object_class_reference_id,
    run_category,
    tax_applicability,
    salary_over_the_cap_type,
    tax_code_reference_id,
    gl_company_reference_id,
    pay_group_reference_id,
    last_updated_moment,
    journal_status,
    grant_costshare_reference_id,
    earnings_in_effort_report,
    upd_dt as update_date'
%}

with
all_accounting_lines as (
    select
        {{ column_names }}
    from
        {{ source('workday_ods', 'workday_payroll_accounting_lines') }}
    where
        1 = 1
        {%- if is_incremental() %}
            and upd_dt > (select max(update_date) from {{ this }})
        {%- endif %}

    union all

    select
        {{ column_names }}
    from
        {{ source('workday_ods', 'workday_payroll_accounting_lines_obligations') }}
    where
        1 = 1
        {%- if is_incremental() %}
            and upd_dt > (select max(update_date) from {{ this }})
        {%- endif %}
)

select
    all_accounting_lines.journal_reference_id,
    all_accounting_lines.journal_number,
    all_accounting_lines.journal_wid,
    all_accounting_lines.ledger_type,
    all_accounting_lines.journal_entry_line_wid,
    all_accounting_lines.line_number,
    all_accounting_lines.line_order,
    all_accounting_lines.payroll_result_wid,
    all_accounting_lines.payroll_pay_period_reference_id,
    all_accounting_lines.payroll_pay_period_wid,
    all_accounting_lines.pay_period_start_date,
    all_accounting_lines.pay_period_end_date,
    all_accounting_lines.payroll_result_line_wid,
    all_accounting_lines.transaction_debit_amount,
    all_accounting_lines.transaction_credit_amount,
    all_accounting_lines.journal_line_exchange_rate,
    all_accounting_lines.ledger_debit_amount,
    all_accounting_lines.ledger_credit_amount,
    all_accounting_lines.estimated_facilities_and_administration_amount,
    all_accounting_lines.gl_hours,
    all_accounting_lines.allocated_hours,
    all_accounting_lines.quantity,
    all_accounting_lines.journal_line_memo,
    all_accounting_lines.transaction_line_memo,
    all_accounting_lines.external_reference_id,
    all_accounting_lines.year,
    all_accounting_lines.journal_line_year_reference_id,
    all_accounting_lines.period,
    all_accounting_lines.journal_line_period_reference_id,
    all_accounting_lines.accounting_date,
    all_accounting_lines.budget_date,
    coalesce(dim_company.company_key, 0) as company_key,
    all_accounting_lines.company_reference_id,
    coalesce(dim_company_line.company_key, 0) as line_company_key,
    all_accounting_lines.line_company_reference_id,
    all_accounting_lines.intercompany_initiating_company_reference_id,
    all_accounting_lines.intercompany_affiliate_reference_id,
    coalesce(dim_ledger_account.ledger_account_key, 0) as ledger_account_key,
    all_accounting_lines.ledger_account_id,
    all_accounting_lines.account_set_reference_id,
    coalesce(dim_revenue_category.revenue_category_key, 0) as revenue_category_key,
    all_accounting_lines.revenue_category_reference_id,
    coalesce(dim_spend_category.spend_category_key, 0) as spend_category_key,
    all_accounting_lines.spend_category_reference_id,
    coalesce(dim_journal_source.journal_source_key, 0) as journal_source_key,
    all_accounting_lines.journal_source_reference_id,
    coalesce(dim_cost_center.cost_center_key, 0) as cost_center_key,
    all_accounting_lines.cost_center_reference_id,
    coalesce(dim_cost_center_site.cost_center_site_key, 0) as cost_center_site_key,
    all_accounting_lines.cost_center_site_reference_id,
    all_accounting_lines.location_reference_id,
    all_accounting_lines.employee_id,
    all_accounting_lines.job_profile_reference_id,
    all_accounting_lines.position_reference_id,
    all_accounting_lines.pay_rate_type_reference_id,
    all_accounting_lines.employee_type_reference_id,
    all_accounting_lines.pay_component_reference_id,
    all_accounting_lines.earning,
    all_accounting_lines.deduction,
    all_accounting_lines.calculation_type,
    all_accounting_lines.deduction_code_reference_id,
    all_accounting_lines.earning_code_reference_id,
    all_accounting_lines.award_reference_id,
    coalesce(dim_grant.grant_key, 0) as grant_key,
    all_accounting_lines.grant_reference_id,
    coalesce(dim_project.project_key, 0) as project_key,
    all_accounting_lines.project_reference_id,
    coalesce(dim_program.program_key, 0) as program_key,
    all_accounting_lines.program_reference_id,
    coalesce(dim_fund.fund_key, 0) as fund_key,
    all_accounting_lines.fund_reference_id,
    all_accounting_lines.gift_reference_id,
    all_accounting_lines.provider_reference_id,
    all_accounting_lines.object_class_reference_id,
    all_accounting_lines.run_category,
    all_accounting_lines.tax_applicability,
    all_accounting_lines.salary_over_the_cap_type,
    all_accounting_lines.tax_code_reference_id,
    all_accounting_lines.gl_company_reference_id,
    coalesce(dim_pay_group.pay_group_key, 0) as pay_group_key,
    all_accounting_lines.pay_group_reference_id,
    all_accounting_lines.last_updated_moment,
    all_accounting_lines.journal_status,
    all_accounting_lines.grant_costshare_reference_id,
    coalesce(dim_grant_costshare.grant_costshare_key, 0) as grant_costshare_key,
    all_accounting_lines.earnings_in_effort_report,
    all_accounting_lines.update_date
from
    all_accounting_lines
    left join {{ref('dim_company')}} as dim_company
        on dim_company.integration_id
            = 'WORKDAY~' || all_accounting_lines.company_reference_id
    left join {{ref('dim_company')}} as  dim_company_line
        on dim_company_line.integration_id
            = 'WORKDAY~' || all_accounting_lines.line_company_reference_id
    left join {{ref('dim_cost_center')}} as dim_cost_center
        on dim_cost_center.integration_id
            = 'WORKDAY~' || all_accounting_lines.cost_center_reference_id
    left join {{ref('dim_cost_center_site')}} as dim_cost_center_site
        on dim_cost_center_site.integration_id
            = 'WORKDAY~' || all_accounting_lines.cost_center_site_reference_id
    left join {{ref('dim_ledger_account')}} as dim_ledger_account
        on dim_ledger_account.integration_id
            = 'WORKDAY~' || all_accounting_lines.ledger_account_id
    left join {{ref('dim_journal_source')}} as dim_journal_source
        on dim_journal_source.integration_id
            = 'WORKDAY~' || all_accounting_lines.journal_source_reference_id
    left join {{ref('dim_grant')}} as dim_grant
        on dim_grant.integration_id
            = 'WORKDAY~' || all_accounting_lines.grant_reference_id
    left join {{ref('dim_fund')}} as dim_fund
        on dim_fund.integration_id
            = 'WORKDAY~' || all_accounting_lines.fund_reference_id
    left join {{ref('dim_pay_group')}} as dim_pay_group
        on dim_pay_group.integration_id
            = 'WORKDAY~' || all_accounting_lines.pay_group_reference_id
    left join {{ref('dim_program')}} as dim_program
        on dim_program.integration_id
            = 'WORKDAY~' || all_accounting_lines.program_reference_id
    left join {{ref('dim_revenue_category')}} as dim_revenue_category
        on dim_revenue_category.integration_id
            = 'WORKDAY~' || all_accounting_lines.revenue_category_reference_id
    left join {{ref('dim_spend_category')}} as dim_spend_category
        on dim_spend_category.integration_id
            = 'WORKDAY~' || all_accounting_lines.spend_category_reference_id
    left join {{ref('dim_project')}} as dim_project
        on dim_project.integration_id
            = 'WORKDAY~' || all_accounting_lines.project_reference_id
    left join {{ref('dim_grant_costshare')}} as dim_grant_costshare
        on dim_grant_costshare.integration_id
            = 'WORKDAY~' || all_accounting_lines.grant_costshare_reference_id
