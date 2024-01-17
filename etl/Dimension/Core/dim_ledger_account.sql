{{
  config(
    materialized = 'incremental',
    unique_key = 'integration_id',
    incremental_strategy = 'merge',
    merge_update_columns = [ 'account_set_wid', 'account_set_id', 'ledger_account_id', 'ledger_account_name', 'retired_ind', 'ledger_account_type_id', 'update_date', 'hash_value', 'integration_id' ],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'ledger_account'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with ledger_account
as (
select
    {{
        dbt_utils.surrogate_key([
            'ledger_account.ledger_account_id',
            'ledger_account.create_by'
        ])
    }} as ledger_account_key,
    ledger_account.account_set_wid,
    ledger_account.account_set_id,
    ledger_account.ledger_account_id,
    ledger_account.ledger_account_name,
    ledger_account.retired_ind,
    ledger_account.ledger_account_type_id,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    ledger_account.create_by || '~' || ledger_account.ledger_account_id as integration_id,
    current_timestamp as create_date,
    ledger_account.create_by,
    current_timestamp as update_date,
    ledger_account.upd_by as update_by
from
    {{source('workday_ods', 'ledger_account')}} as ledger_account
--
union all
--
select
    0,
    'NA',
    'NA',
    'NA',
    'NA',
    0,
    'NA',
    0,
    'NA',
    CURRENT_TIMESTAMP,
    'DEFAULT',
    CURRENT_TIMESTAMP,
    'DEFAULT'
)
select
    ledger_account.ledger_account_key,
    ledger_account.account_set_wid,
    ledger_account.account_set_id,
    ledger_account.ledger_account_id,
    ledger_account.ledger_account_name,
    ledger_account.retired_ind,
    ledger_account.ledger_account_type_id,
    ledger_account.hash_value,
    ledger_account.integration_id,
    ledger_account.create_date,
    ledger_account.create_by,
    ledger_account.update_date,
    ledger_account.update_by
from
    ledger_account
where 1 = 1     
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where integration_id = ledger_account.integration_id)
{%- endif %}
