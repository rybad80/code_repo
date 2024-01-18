{{ config(meta = {
    'critical': true
}) }}

select
    sum(stg_gl_summary_ending_balance.month_activity_amount) as month_activity_amount,
    stg_gl_summary_ending_balance.company_key,
    stg_gl_summary_ending_balance.company_reference_id,
    stg_gl_summary_ending_balance.company_name,
    stg_gl_summary_ending_balance.cost_center_key,
    stg_gl_summary_ending_balance.cost_center_reference_id,
    stg_gl_summary_ending_balance.cost_center_name,
    stg_gl_summary_ending_balance.ledger_account_key,
    stg_gl_summary_ending_balance.ledger_account_id,
    stg_gl_summary_ending_balance.ledger_account_name,
    stg_gl_summary_ending_balance.period,
    stg_gl_summary_ending_balance.year,
    stg_gl_summary_ending_balance.ledger_type,
    stg_gl_summary_ending_balance.cost_center_site_key,
    stg_gl_summary_ending_balance.cost_center_site_reference_id,
    stg_gl_summary_ending_balance.cost_center_site_name,
    stg_gl_summary_ending_balance.revenue_category_key,
    stg_gl_summary_ending_balance.revenue_category_reference_id,
    stg_gl_summary_ending_balance.revenue_category_name,
    stg_gl_summary_ending_balance.spend_category_key,
    stg_gl_summary_ending_balance.spend_category_reference_id,
    stg_gl_summary_ending_balance.spend_category_name,
    stg_gl_summary_ending_balance.intercompany_affiliate_reference_id
from
    (
    select
        company_key,
        company_reference_id,
        company_name,
        cost_center_key,
        cost_center_reference_id,
        cost_center_name,
        ledger_account_key,
        ledger_account_id,
        ledger_account_name,
        period,
        year,
        ledger_type,
        cost_center_site_key,
        cost_center_site_reference_id,
        cost_center_site_name,
        revenue_category_key,
        revenue_category_reference_id,
        revenue_category_name,
        spend_category_key,
        spend_category_reference_id,
        spend_category_name,
        intercompany_affiliate_reference_id,
        month_activity_amount
    from
        {{ref('stg_finance_gl_summary_ending_balance')}}
union all
    select
        company_key,
        company_reference_id,
        company_name,
        cost_center_key,
        cost_center_reference_id,
        cost_center_name,
        ledger_account_key,
        ledger_account_id,
        ledger_account_name,
        period,
        year,
        ledger_type,
        cost_center_site_key,
        cost_center_site_reference_id,
        cost_center_site_name,
        revenue_category_key,
        revenue_category_reference_id,
        revenue_category_name,
        spend_category_key,
        spend_category_reference_id,
        spend_category_name,
        intercompany_affiliate_reference_id,
        month_activity_amount
    from
        {{ref('stg_finance_gl_summary_beg_balance')}}
     ) as stg_gl_summary_ending_balance
where
    1 = 1
group by
    stg_gl_summary_ending_balance.company_key,
    stg_gl_summary_ending_balance.company_reference_id,
    stg_gl_summary_ending_balance.company_name,
    stg_gl_summary_ending_balance.cost_center_key,
    stg_gl_summary_ending_balance.cost_center_reference_id,
    stg_gl_summary_ending_balance.cost_center_name,
    stg_gl_summary_ending_balance.ledger_account_key,
    stg_gl_summary_ending_balance.ledger_account_id,
    stg_gl_summary_ending_balance.ledger_account_name,
    stg_gl_summary_ending_balance.period,
    stg_gl_summary_ending_balance.year,
    stg_gl_summary_ending_balance.ledger_type,
    stg_gl_summary_ending_balance.cost_center_site_key,
    stg_gl_summary_ending_balance.cost_center_site_reference_id,
    stg_gl_summary_ending_balance.cost_center_site_name,
    stg_gl_summary_ending_balance.revenue_category_key,
    stg_gl_summary_ending_balance.revenue_category_reference_id,
    stg_gl_summary_ending_balance.revenue_category_name,
    stg_gl_summary_ending_balance.spend_category_key,
    stg_gl_summary_ending_balance.spend_category_reference_id,
    stg_gl_summary_ending_balance.spend_category_name,
    stg_gl_summary_ending_balance.intercompany_affiliate_reference_id