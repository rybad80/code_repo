{{ config(
    materialized = 'incremental',
    unique_key = ['bank_account_wid', 'payment_type_id'],
    incremental_strategy = 'merge',
    merge_update_columns = [
        'bank_account_wid', 'bank_account_id', 'payment_type_id',
        'md5', 'upd_dt', 'upd_by'
    ]
) }}

select distinct  -- there is duplication in the source table
    bank_account_bank_account_reference_wid as bank_account_wid,
    bank_account_bank_account_data_bank_account_id as bank_account_id,
    bank_account_data_payment_type_reference_payment_type_id as payment_type_id,
    cast({{
        dbt_utils.surrogate_key([
            'bank_account_wid',
            'bank_account_id',
            'payment_type_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_bank_accounts')}}
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                bank_account_wid = get_bank_accounts.bank_account_bank_account_reference_wid
                and payment_type_id = get_bank_accounts.bank_account_data_payment_type_reference_payment_type_id
        )
    {%- endif %}
