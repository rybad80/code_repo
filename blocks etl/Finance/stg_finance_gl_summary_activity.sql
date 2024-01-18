{{ config(meta = {
    'critical': true
}) }}

select
    'Activity' as amount_type,
    fact_journal_lines.company_key,
    fact_journal_lines.company_reference_id,
    dim_company.company_name,
    fact_journal_lines.cost_center_key,
    fact_journal_lines.cost_center_reference_id,
    dim_cost_center.cost_center_name as cost_center_name,
    fact_journal_lines.ledger_account_key,
    fact_journal_lines.ledger_account_id,
    dim_ledger_account.ledger_account_name,
    fact_journal_lines.project_key,
    fact_journal_lines.project_reference_id,
    dim_project.project_name,
    fact_journal_lines.provider_reference_id,
    fact_journal_lines.period,
    fact_journal_lines.year,
    fact_journal_lines.ledger_type,
    sum(fact_journal_lines.ledger_debit_amount
        - fact_journal_lines.ledger_credit_amount) as month_activity_amount,
    fact_journal_lines.payor_key,
    fact_journal_lines.payor_reference_id,
    dim_payor.payor_name,
    fact_journal_lines.cost_center_site_key,
    fact_journal_lines.cost_center_site_reference_id,
    dim_cost_center_site.cost_center_site_name,
    fact_journal_lines.revenue_category_key,
    fact_journal_lines.revenue_category_reference_id,
    dim_revenue_category.revenue_category_name,
    fact_journal_lines.spend_category_key,
    fact_journal_lines.spend_category_reference_id,
    dim_spend_category.spend_category_name,
    fact_journal_lines.grant_key,
    fact_journal_lines.grant_reference_id,
    dim_grant.grant_name,
    fact_journal_lines.program_key,
    fact_journal_lines.program_reference_id,
    dim_program.program_name,
    fact_journal_lines.fund_key,
    fact_journal_lines.fund_reference_id,
    dim_fund.fund_name,
    fact_journal_lines.gift_reference_id,
    fact_journal_lines.location_reference_id,
    location.loc_nm as location_name,
    fact_journal_lines.grant_costshare_key,
    fact_journal_lines.grant_costshare_reference_id,
    dim_grant_costshare.grant_costshare_code,
    fact_journal_lines.journal_source_key,
    fact_journal_lines.journal_source_reference_id,
    fact_journal_lines.intercompany_affiliate_reference_id
from
    {{ref('fact_journal_lines_posted')}} as fact_journal_lines
inner join {{ref('dim_ledger_account')}} as dim_ledger_account
        on
    fact_journal_lines.ledger_account_key
           = dim_ledger_account.ledger_account_key
inner join {{ref('dim_payor')}} as dim_payor
        on
    fact_journal_lines.payor_key
           = dim_payor.payor_key
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
inner join {{ref('dim_grant')}} as dim_grant
        on
    fact_journal_lines.grant_key
           = dim_grant.grant_key
inner join {{ref('dim_project')}} as dim_project
        on
    fact_journal_lines.project_key
           = dim_project.project_key
inner join {{ref('dim_program')}} as dim_program
        on
    fact_journal_lines.program_key
           = dim_program.program_key
inner join {{ref('dim_grant_costshare')}} as dim_grant_costshare
        on
    fact_journal_lines.grant_costshare_key
           = dim_grant_costshare.grant_costshare_key
inner join {{ref('dim_fund')}} as dim_fund
        on
    fact_journal_lines.fund_key
           = dim_fund.fund_key
inner join {{ref('dim_cost_center_site')}} as dim_cost_center_site
        on
    fact_journal_lines.cost_center_site_key
           = dim_cost_center_site.cost_center_site_key
inner join {{ref('dim_ledger_account_hierarchy_long')}} as dim_ledger_account_hierarchy_long
        on
    fact_journal_lines.ledger_account_id
           = dim_ledger_account_hierarchy_long.ledger_account_id
    and dim_ledger_account_hierarchy_long.hier_level_num = 2
    and top_level = 'All Ledger Accounts'
left join {{source('cdw', 'location')}} as location
        on
    fact_journal_lines.location_reference_id
           = location.loc_id::varchar(200)
where
    fact_journal_lines.ledger_type = 'Actuals'
    and fact_journal_lines.line_type = 'JEL'
    and dim_ledger_account_hierarchy_long.ledger_account_hierarchy_level = 'Income Statement'
group by
    fact_journal_lines.company_key,
    fact_journal_lines.company_reference_id,
    dim_company.company_name,
    fact_journal_lines.cost_center_key,
    fact_journal_lines.cost_center_reference_id,
    cost_center_name,
    fact_journal_lines.ledger_account_key,
    fact_journal_lines.ledger_account_id,
    dim_ledger_account.ledger_account_name,
    fact_journal_lines.project_key,
    fact_journal_lines.project_reference_id,
    dim_project.project_name,
    fact_journal_lines.provider_reference_id,
    fact_journal_lines.period,
    fact_journal_lines.year,
    fact_journal_lines.ledger_type,
    fact_journal_lines.payor_key,
    fact_journal_lines.payor_reference_id,
    dim_payor.payor_name,
    fact_journal_lines.cost_center_site_key,
    fact_journal_lines.cost_center_site_reference_id,
    dim_cost_center_site.cost_center_site_name,
    fact_journal_lines.revenue_category_key,
    fact_journal_lines.revenue_category_reference_id,
    dim_revenue_category.revenue_category_name,
    fact_journal_lines.spend_category_key,
    fact_journal_lines.spend_category_reference_id,
    dim_spend_category.spend_category_name,
    fact_journal_lines.grant_key,
    fact_journal_lines.grant_reference_id,
    dim_grant.grant_name,
    fact_journal_lines.program_key,
    fact_journal_lines.program_reference_id,
    dim_program.program_name,
    fact_journal_lines.fund_key,
    fact_journal_lines.fund_reference_id,
    dim_fund.fund_name,
    fact_journal_lines.gift_reference_id,
    fact_journal_lines.location_reference_id,
    location_name,
    fact_journal_lines.journal_source_key,
    fact_journal_lines.journal_source_reference_id,
    fact_journal_lines.grant_costshare_key,
    fact_journal_lines.grant_costshare_reference_id,
    dim_grant_costshare.grant_costshare_code,
    fact_journal_lines.intercompany_affiliate_reference_id
