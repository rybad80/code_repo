select distinct
    ledger_account_summary_ledger_account_summary_reference_wid as ledger_account_summary_wid,
    ledger_account_summary_ledger_account_summary_data_ledger_account_summary_id as ledger_account_summary_id,
    get_account_sets.account_set_data_account_set_reference_account_set_id as account_set_id,
    coalesce(all_ledger_account_data_ledger_account_included_reference_chop_financials, 'N/A') as ledger_account_id,
    cast({{
        dbt_utils.surrogate_key([
            'ledger_account_summary_wid',
            'ledger_account_summary_id',
            'account_set_id',
            'ledger_account_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_ledger_account_summaries')}} as get_ledger_account_summaries
inner join
    {{source('workday_ods', 'get_account_sets')}} as get_account_sets
    on ledger_account_summary_data_account_set_reference_wid = get_account_sets.account_set_account_set_reference_wid
where
    1 = 1
