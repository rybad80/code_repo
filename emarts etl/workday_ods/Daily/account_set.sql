{{ config(
    materialized = 'incremental',
    unique_key = 'account_set_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['account_set_wid','account_set_id','account_set_name', 'chart_of_accounts_ind', 'reference_account_set_wid', 'reference_account_set_id','md5', 'upd_dt', 'upd_by']
) }}
select distinct
    account_set_account_set_reference_wid as account_set_wid,
    account_set_account_set_reference_account_set_id as account_set_id,
    account_set_account_set_data_account_set_name as account_set_name,
    account_set_account_set_data_chart_of_accounts as chart_of_accounts_ind,
    account_set_data_account_set_reference_wid as reference_account_set_wid,
    account_set_data_account_set_reference_account_set_id as reference_account_set_id,
    cast({{
        dbt_utils.surrogate_key([
            'account_set_wid',
            'account_set_id',
            'account_set_name',
            'chart_of_accounts_ind',
            'reference_account_set_wid',
            'reference_account_set_id'
            ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_account_sets')}} as get_account_set
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                account_set_wid = get_account_set.account_set_account_set_reference_wid
        )
    {%- endif %}
