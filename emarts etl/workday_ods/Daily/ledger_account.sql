{{ config(
    materialized = 'incremental',
    unique_key = ['account_set_wid','ledger_account_id'],
    incremental_strategy = 'merge',
    merge_update_columns = ['account_set_wid','account_set_id','ledger_account_id','ledger_account_name','retired_ind','ledger_account_type_id', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
    account_set_account_set_reference_wid as account_set_wid,
    account_set_account_set_reference_account_set_id as account_set_id,
    account_set_data_ledger_account_data_ledger_account_identifier as ledger_account_id,
    account_set_data_ledger_account_data_ledger_account_name as ledger_account_name,
    coalesce(cast(account_set_data_ledger_account_data_retired as int), -2) as retired_ind,
    ledger_account_data_ledger_account_type_reference_ledger_account_type_id as ledger_account_type_id,
    cast({{
        dbt_utils.surrogate_key([
            'account_set_wid',
            'account_set_id',
            'ledger_account_id',
            'ledger_account_name',
            'retired_ind',
            'ledger_account_type_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_account_sets')}} as get_account_sets
where
    1 = 1
    and account_set_data_ledger_account_data_ledger_account_identifier is not null
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                account_set_wid = get_account_sets.account_set_account_set_reference_wid
                and ledger_account_id = get_account_sets.account_set_data_ledger_account_data_ledger_account_identifier
        )
    {%- endif %}