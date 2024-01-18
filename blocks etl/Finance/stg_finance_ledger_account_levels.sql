{{ config(meta = {
    'critical': true
}) }}

select
    workday_ledger_account.ledger_acct_key as ledger_account_key,
    workday_ledger_account.ledger_acct_id as ledger_account_id,
    workday_ledger_account.ledger_acct_nm as ledger_account_name,
    workday_ledger_account.ledger_acct_type as ledger_account_type,
    lower(ledger_account_ledger_account_summary.ledger_account_summary_id) as ledger_account_summary_id,
    ledger_account_ledger_account_summary.ledger_account_summary_wid,
    ledger_account_summary_levels.level_2_ledger_account_summary_name as level2_name,
    ledger_account_summary_levels.level_3_ledger_account_summary_name as level3_name,
    ledger_account_summary_levels.level_4_ledger_account_summary_name as level4_name,
    ledger_account_summary_levels.level_5_ledger_account_summary_name as level5_name,
    ledger_account_summary_levels.level_6_ledger_account_summary_name as level6_name
from
    {{source('workday_ods', 'ledger_account_ledger_account_summary')}} as ledger_account_ledger_account_summary
    inner join {{source('workday_ods', 'ledger_account_summary_levels')}} as ledger_account_summary_levels
        on ledger_account_ledger_account_summary.ledger_account_summary_id
            = ledger_account_summary_levels.ledger_account_summary_id
    inner join {{source('workday', 'workday_ledger_account')}} as workday_ledger_account
        on workday_ledger_account.ledger_acct_id = ledger_account_ledger_account_summary.ledger_account_id
