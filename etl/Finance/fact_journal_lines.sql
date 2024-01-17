{{
    config(
        materialized = 'incremental',
        unique_key = 'integration_id',
        pre_hook=before_begin(
            "update
                {{source('workday_ods', 'workday_journal_lines')}}
             set
                journal_status = 'Canceled',
                upd_dt = current_timestamp
             from
                 {{ref('stg_workday_journal_lines_deleted')}} as stg_workday_journal_lines_deleted
             where
                workday_journal_lines.journal_entry_line_wid = stg_workday_journal_lines_deleted.journal_entry_line_wid"
                if target.name in ('uat', 'prod') else ""
            ),
        meta = {
            'critical': true
        }
    )
}}
-- depends_on: {{ ref('stg_workday_journal_lines_deleted') }}
{% set column_names = '
    journal_reference_id,
    journal_number,
    journal_wid,
    ledger_type,
    journal_entry_line_wid,
    line_number,
    line_order,
    transaction_debit_amount,
    transaction_credit_amount,
    journal_line_exchange_rate,
    ledger_debit_amount,
    ledger_credit_amount,
    quantity,
    journal_memo,
    line_memo,
    external_reference_id,
    year,
    period,
    journal_line_year_reference_id,
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
    deduction_code_reference_id,
    earning_code_reference_id,
    grant_ref_id as grant_reference_id,
    project_ref_id as project_reference_id,
    program_ref_id as program_reference_id,
    fund_ref_id as fund_reference_id,
    gift_ref_id as gift_reference_id,
    provider_ref_id as provider_reference_id,
    supplier_id,
    supplier_reference_id,
    procurement_item_reference_id,
    procurement_item_wid,
    object_class_reference_id,
    sponsor_id,
    sponsor_reference_id,
    bank_account_ref_id as bank_account_reference_id,
    petty_cash_account_ref_id as petty_cash_account_reference_id,
    customer_id,
    customer_reference_id,
    expense_item_ref_id as expense_item_reference_id,
    corporate_credit_card_account_reference_id,
    corporate_credit_card_account_wid,
    customer_contract_reference_id,
    contingent_worker_ref_id as contingent_worker_reference_id,
    run_category,
    tax_applicability,
    salary_over_the_cap_type,
    external_committee_member,
    receivable_writeoff_reason,
    cash_flow_code,
    cash_flow_ref_id as cash_flow_reference_id,
    withholding_tax_code,
    tax_code,
    tax_code_reference_id,
    ad_hoc_payee,
    ad_hoc_payee_reference_id,
    payor_reference_id,
    gl_company_reference_id,
    pay_group_reference_id,
    supplier_invoice_wid,
    supplier_invoice_line_wid,
    purchase_order_line_wid,
    asset_impairment_reason,
    exclude_from_spend_report,
    operational_transaction,
    last_updated_moment,
    estimated_facilities_and_administration_amount,
    award_reference_id,
    award_wid,
    journal_status,
    upd_dt'
%}
--
with wjl_full as (
    select
        {{ column_names }},
        'JEL' as line_type,
        'JEL' || '~' || journal_entry_line_wid as integration_id,  -- noqa: L028
        grant_costshare_reference_id  -- noqa: L028
    from
        {{source('workday_ods', 'workday_journal_lines')}}
    where 1 = 1
        {%- if is_incremental() %}
        and upd_dt > (select max(update_date) from {{ this }} where line_type = 'JEL')   -- noqa: L028
        {%- endif %}
--
    union all
--
    select
        {{ column_names }},
        'BEG_BAL' as line_type,
        'BEG_BAL' || '~' || journal_entry_line_wid as integration_id,  -- noqa: L028
        null as grant_costshare_reference_id
    from
        {{source('workday_ods', 'workday_journal_lines_beginning_balance')}}
    where 1 = 1
        {%- if is_incremental() %}
        and upd_dt > (select max(update_date) from {{this}} where line_type = 'BEG_BAL')   -- noqa: L028
        {%- endif %}
),
--
extra_fields as (
    select
        wjl_full.*,
        coalesce(dim_company.company_key, 0) as company_key,
        coalesce(dim_company_line.company_key, 0) as line_company_key,
        coalesce(
            dim_ledger_account.ledger_account_key, 0
        ) as ledger_account_key,
        coalesce(
            dim_revenue_category.revenue_category_key, 0
        ) as revenue_category_key,
        coalesce(
            dim_spend_category.spend_category_key, 0
        ) as spend_category_key,
        coalesce(
            dim_journal_source.journal_source_key, 0
        ) as journal_source_key,
        coalesce(dim_cost_center.cost_center_key, 0) as cost_center_key,
        coalesce(
            dim_cost_center_site.cost_center_site_key, 0
        ) as cost_center_site_key,
        coalesce(dim_grant.grant_key, 0) as grant_key,
        coalesce(dim_project.project_key, 0) as project_key,
        coalesce(dim_program.program_key, 0) as program_key,
        coalesce(dim_fund.fund_key, 0) as fund_key,
        coalesce(dim_supplier.supplier_key, 0) as supplier_key,
        coalesce(dim_payor.payor_key, 0) as payor_key,
        coalesce(dim_pay_group.pay_group_key, 0) as pay_group_key,
        coalesce(
            dim_grant_costshare.grant_costshare_key, 0
        ) as grant_costshare_key
    from wjl_full
    left join {{ref('dim_company')}} as dim_company
        on
            dim_company.integration_id = 'WORKDAY~' || wjl_full.company_reference_id
    left join {{ref('dim_company')}} as dim_company_line
        on
            dim_company_line.integration_id = 'WORKDAY~' || wjl_full.line_company_reference_id
    left join {{ref('dim_cost_center')}} as dim_cost_center
        on
            dim_cost_center.integration_id = 'WORKDAY~' || wjl_full.cost_center_reference_id
    left join {{ref('dim_cost_center_site')}} as dim_cost_center_site
        on
            dim_cost_center_site.integration_id = 'WORKDAY~' || wjl_full.cost_center_site_reference_id
    left join {{ref('dim_ledger_account')}} as dim_ledger_account
        on
            dim_ledger_account.integration_id = 'WORKDAY~' || wjl_full.ledger_account_id
    left join {{ref('dim_journal_source')}} as dim_journal_source
        on
            dim_journal_source.integration_id = 'WORKDAY~' || wjl_full.journal_source_reference_id
    left join {{ref('dim_grant')}} as dim_grant
        on dim_grant.integration_id = 'WORKDAY~' || wjl_full.grant_reference_id
    left join {{ref('dim_fund')}} as dim_fund
        on dim_fund.integration_id = 'WORKDAY~' || wjl_full.fund_reference_id
    left join {{ref('dim_pay_group')}} as dim_pay_group
        on
            dim_pay_group.integration_id = 'WORKDAY~' || wjl_full.pay_group_reference_id
    left join {{ref('dim_payor')}} as dim_payor
        on dim_payor.integration_id = 'WORKDAY~' || wjl_full.payor_reference_id
    left join {{ref('dim_program')}} as dim_program
        on
            dim_program.integration_id = 'WORKDAY~' || wjl_full.program_reference_id
    left join {{ref('dim_revenue_category')}} as dim_revenue_category
        on
            dim_revenue_category.integration_id = 'WORKDAY~' || wjl_full.revenue_category_reference_id
    left join {{ref('dim_spend_category')}} as dim_spend_category
        on
            dim_spend_category.integration_id = 'WORKDAY~' || wjl_full.spend_category_reference_id
    left join {{ref('dim_project')}} as dim_project
        on
            dim_project.integration_id = 'WORKDAY~' || wjl_full.project_reference_id
    left join {{ref('dim_supplier')}} as dim_supplier
        on
            dim_supplier.integration_id = 'WORKDAY~' || wjl_full.supplier_reference_id
    left join {{ref('dim_grant_costshare')}} as dim_grant_costshare
        on dim_grant_costshare.integration_id
            = 'WORKDAY~' || wjl_full.grant_costshare_reference_id
)
--
-- Reorder the fields:
select
    extra_fields.journal_reference_id,
    extra_fields.journal_number,
    extra_fields.journal_wid,
    extra_fields.ledger_type,
    extra_fields.journal_entry_line_wid,
    extra_fields.line_type,
    extra_fields.integration_id,
    extra_fields.line_number,
    extra_fields.line_order,
    extra_fields.transaction_debit_amount,
    extra_fields.transaction_credit_amount,
    extra_fields.journal_line_exchange_rate,
    extra_fields.ledger_debit_amount,
    extra_fields.ledger_credit_amount,
    extra_fields.quantity,
    extra_fields.journal_memo,
    extra_fields.line_memo,
    extra_fields.external_reference_id,
    extra_fields.year,
    extra_fields.period,
    extra_fields.journal_line_year_reference_id,
    extra_fields.journal_line_period_reference_id,
    extra_fields.accounting_date,
    extra_fields.budget_date,
    extra_fields.company_reference_id,
    extra_fields.company_key,
    extra_fields.line_company_reference_id,
    extra_fields.line_company_key,
    extra_fields.intercompany_initiating_company_reference_id,
    extra_fields.intercompany_affiliate_reference_id,
    extra_fields.ledger_account_id,
    extra_fields.ledger_account_key,
    extra_fields.account_set_reference_id,
    extra_fields.revenue_category_reference_id,
    extra_fields.revenue_category_key,
    extra_fields.spend_category_reference_id,
    extra_fields.spend_category_key,
    extra_fields.journal_source_reference_id,
    extra_fields.journal_source_key,
    extra_fields.cost_center_reference_id,
    extra_fields.cost_center_key,
    extra_fields.cost_center_site_reference_id,
    extra_fields.cost_center_site_key,
    extra_fields.location_reference_id,
    extra_fields.employee_id,
    extra_fields.job_profile_reference_id,
    extra_fields.position_reference_id,
    extra_fields.pay_rate_type_reference_id,
    extra_fields.employee_type_reference_id,
    extra_fields.deduction_code_reference_id,
    extra_fields.earning_code_reference_id,
    extra_fields.grant_reference_id,
    extra_fields.grant_key,
    extra_fields.project_reference_id,
    extra_fields.project_key,
    extra_fields.program_reference_id,
    extra_fields.program_key,
    extra_fields.fund_reference_id,
    extra_fields.fund_key,
    extra_fields.gift_reference_id,
    extra_fields.provider_reference_id,
    extra_fields.supplier_id,
    extra_fields.supplier_reference_id,
    extra_fields.supplier_key,
    extra_fields.procurement_item_reference_id,
    extra_fields.procurement_item_wid,
    extra_fields.object_class_reference_id,
    extra_fields.sponsor_id,
    extra_fields.sponsor_reference_id,
    extra_fields.bank_account_reference_id,
    extra_fields.petty_cash_account_reference_id,
    extra_fields.customer_id,
    extra_fields.customer_reference_id,
    extra_fields.expense_item_reference_id,
    extra_fields.corporate_credit_card_account_reference_id,
    extra_fields.corporate_credit_card_account_wid,
    extra_fields.customer_contract_reference_id,
    extra_fields.contingent_worker_reference_id,
    extra_fields.run_category,
    extra_fields.tax_applicability,
    extra_fields.salary_over_the_cap_type,
    extra_fields.external_committee_member,
    extra_fields.receivable_writeoff_reason,
    extra_fields.cash_flow_code,
    extra_fields.cash_flow_reference_id,
    extra_fields.withholding_tax_code,
    extra_fields.tax_code,
    extra_fields.tax_code_reference_id,
    extra_fields.ad_hoc_payee,
    extra_fields.ad_hoc_payee_reference_id,
    extra_fields.payor_reference_id,
    extra_fields.payor_key,
    extra_fields.gl_company_reference_id,
    extra_fields.pay_group_reference_id,
    extra_fields.pay_group_key,
    extra_fields.supplier_invoice_wid,
    extra_fields.supplier_invoice_line_wid,
    extra_fields.purchase_order_line_wid,
    extra_fields.asset_impairment_reason,
    extra_fields.exclude_from_spend_report,
    extra_fields.operational_transaction,
    extra_fields.last_updated_moment,
    extra_fields.estimated_facilities_and_administration_amount,
    extra_fields.award_reference_id,
    extra_fields.award_wid,
    extra_fields.journal_status,
    extra_fields.grant_costshare_reference_id,
    extra_fields.grant_costshare_key,
    extra_fields.upd_dt as update_date
from
    extra_fields
