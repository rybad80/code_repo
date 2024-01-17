{{ config(meta = {
    'critical': true
}) }}

select
    journal_entry_line.accounting_date as post_date,
    coalesce(journal_entry_line.cost_center_reference_id, '0') as cost_center_id,
    coalesce(journal_entry_line.cost_center_site_ref_id, '0') as cost_center_site_id,
    coalesce(workday_revenue_category.revenue_cat_id, '0') as revenue_category_id,
    coalesce(journal_entry_line.payor_reference_id, '0') as payor_id,
    date_trunc('month', journal_entry_line.accounting_date) as post_month,
    cost_center.cost_cntr_nm as cost_center_name,
    workday_cost_center_site.cost_cntr_site_nm as cost_center_site_name,
    workday_revenue_category.revenue_cat_nm as revenue_category_name,
    workday_payor.payor_nm as payor_name,
    sum(
        journal_entry_line.ledger_debit_amount - journal_entry_line.ledger_credit_amount
    ) * -1 as charges_actual
from
    {{source('workday_ods', 'workday_journal_lines')}} as journal_entry_line
    inner join {{source('workday_ods', 'ledger_account')}} as ledger_account
        on ledger_account.ledger_account_id = journal_entry_line.ledger_account_id
    inner join {{source('workday', 'workday_payor')}} as workday_payor
        on workday_payor.payor_id = coalesce(journal_entry_line.payor_reference_id, '0')
    inner join {{source('workday', 'workday_revenue_category')}} as workday_revenue_category
        on workday_revenue_category.revenue_cat_id
        = coalesce(journal_entry_line.revenue_category_reference_id, '0')
    inner join {{source('cdw', 'cost_center')}} as cost_center
        on journal_entry_line.cost_center_reference_id = coalesce(cost_center.cost_cntr_id, '0')
    inner join {{source('workday', 'workday_cost_center_site')}} as workday_cost_center_site
        on journal_entry_line.cost_center_site_ref_id = coalesce(workday_cost_center_site.cost_cntr_site_id, '0')
where
    journal_entry_line.ledger_type = 'Actuals'
    and journal_entry_line.journal_status in ('Posted') --, 'PRO_FORMA')
    and journal_entry_line.ledger_account_id = '40000'
    and journal_entry_line.accounting_date  >= to_date('2019-07-01', 'yyyy-mm-dd') --noqa: L006
group by
    journal_entry_line.accounting_date,
    journal_entry_line.cost_center_reference_id,
    journal_entry_line.cost_center_site_ref_id,
    cost_center.cost_cntr_nm,
    workday_cost_center_site.cost_cntr_site_nm,
    workday_revenue_category.revenue_cat_id,
    workday_revenue_category.revenue_cat_nm,
    journal_entry_line.payor_reference_id,
    workday_payor.payor_nm
