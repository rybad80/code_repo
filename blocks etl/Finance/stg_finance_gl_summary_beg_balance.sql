{{ config(meta = {
    'critical': true
}) }}

select
    'Beginning Balance' as amount_type,
    fact_journal_lines.company_key,
    fact_journal_lines.company_reference_id,
    dim_company.company_name,
    fact_journal_lines.cost_center_key,
    fact_journal_lines.cost_center_reference_id,
    dim_cost_center.cost_center_name as cost_center_name,
    fact_journal_lines.ledger_account_key,
    fact_journal_lines.ledger_account_id,
    dim_ledger_account.ledger_account_name,
    '01-JUL' as period,
    fact_journal_lines.year,
    fact_journal_lines.ledger_type,
    sum(fact_journal_lines.ledger_debit_amount
        - fact_journal_lines.ledger_credit_amount) as month_activity_amount,
    fact_journal_lines.cost_center_site_key,
    fact_journal_lines.cost_center_site_reference_id,
    dim_cost_center_site.cost_center_site_name,
    fact_journal_lines.revenue_category_key,
    fact_journal_lines.revenue_category_reference_id,
    dim_revenue_category.revenue_category_name,
    fact_journal_lines.spend_category_key,
    fact_journal_lines.spend_category_reference_id,
    dim_spend_category.spend_category_name,
    fact_journal_lines.intercompany_affiliate_reference_id
from
    {{ref('fact_journal_lines_posted')}} as fact_journal_lines
inner join {{ref('dim_ledger_account')}} as dim_ledger_account
        on
    fact_journal_lines.ledger_account_key
           = dim_ledger_account.ledger_account_key
inner join {{ref('dim_company')}} as dim_company
        on
    fact_journal_lines.company_key
           = dim_company.company_key
inner join {{ref('dim_cost_center')}} as dim_cost_center
        on
    fact_journal_lines.cost_center_key
           = dim_cost_center.cost_center_key
inner join {{ref('dim_revenue_category')}} as dim_revenue_category
        on
    fact_journal_lines.revenue_category_key
           = dim_revenue_category.revenue_category_key
inner join {{ref('dim_spend_category')}} as dim_spend_category
        on
    fact_journal_lines.spend_category_key
           = dim_spend_category.spend_category_key
inner join {{ref('dim_cost_center_site')}} as dim_cost_center_site
        on
    fact_journal_lines.cost_center_site_key
           = dim_cost_center_site.cost_center_site_key
where
    1 = 1
    and fact_journal_lines.line_type = 'BEG_BAL'
group by
    fact_journal_lines.company_key,
    fact_journal_lines.company_reference_id,
    dim_company.company_name,
    fact_journal_lines.cost_center_key,
    fact_journal_lines.cost_center_reference_id,
    dim_cost_center.cost_center_name,
    fact_journal_lines.ledger_account_key,
    fact_journal_lines.ledger_account_id,
    dim_ledger_account.ledger_account_name,
    fact_journal_lines.year,
    fact_journal_lines.ledger_type,
    fact_journal_lines.cost_center_site_key,
    fact_journal_lines.cost_center_site_reference_id,
    dim_cost_center_site.cost_center_site_name,
    fact_journal_lines.revenue_category_key,
    fact_journal_lines.revenue_category_reference_id,
    dim_revenue_category.revenue_category_name,
    fact_journal_lines.spend_category_key,
    fact_journal_lines.spend_category_reference_id,
    dim_spend_category.spend_category_name,
    fact_journal_lines.intercompany_affiliate_reference_id
