{{ config(
    materialized = 'incremental',
    unique_key = ['journal_wid','journal_line_wid'],
    incremental_strategy = 'merge',
    merge_update_columns = ['journal_wid','journal_line_wid','company_reference_id','cost_center_reference_id','cost_center_site_reference_id','ledger_account_reference_id','revenue_category_reference_id','spend_category_reference_id','ledger_account_type_reference_id','intercompany_affiliate_reference_id','intercompany_initiating_company_reference_id','journal_source_reference_id','ledger_budget_credit_amount','ledger_budget_debit_amount','is_intercompany_journal','accounting_date','last_updated_dt', 'md5', 'upd_dt', 'upd_by']
) }}
select
    journal_wid,
    journal_line_wid,
    company_reference_id,
    cost_center_reference_id,
    cost_center_site_reference_id,
    ledger_account_reference_id,
    revenue_category_reference_id,
    spend_category_reference_id,
    ledger_account_type_reference_id,
    intercompany_affiliate_reference_id,
    intercompany_initiating_company_reference_id,
    journal_source_reference_id,
    ledger_budget_credit_amount,
    ledger_budget_debit_amount,
    is_intercompany_journal,
    to_date(substr(accounting_date, 1,19),'yyyy-mm-dd') as accounting_date,
    to_timestamp(substr(last_updated_moment,1,10) || ' ' || substr(last_updated_moment,12,8), 'yyyy-mm-dd hh24:mi:ss') as last_updated_dt,
    cast({{
        dbt_utils.surrogate_key([
            'journal_wid',
            'journal_line_wid',
            'company_reference_id',
            'cost_center_reference_id',
            'cost_center_site_reference_id',
            'ledger_account_reference_id',
            'revenue_category_reference_id',
            'spend_category_reference_id',
            'ledger_account_type_reference_id',
            'intercompany_affiliate_reference_id',
            'intercompany_initiating_company_reference_id',
            'journal_source_reference_id',
            'ledger_budget_credit_amount',
            'ledger_budget_debit_amount',
            'is_intercompany_journal',
            'accounting_date',
            'last_updated_dt'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'workday_journal_line_undistributed_earnings')}} as workday_journal_line_undistributed_earnings
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                journal_wid = workday_journal_line_undistributed_earnings.journal_wid
                and journal_line_wid = workday_journal_line_undistributed_earnings.journal_line_wid
        )
    {%- endif %}