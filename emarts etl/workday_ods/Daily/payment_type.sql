{{ 
    config(
        materialized = 'incremental',
        unique_key = 'payment_type_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['payment_type_wid','payment_type_name','payment_type_id','md5', 'upd_dt', 'upd_by']
    )
 }}
select distinct
    payment_type_reference_wid  as payment_type_wid,
    payment_type_data_payment_type_name as payment_type_name,
    payment_type_data_payment_type_id as payment_type_id,
    cast({{
        dbt_utils.surrogate_key([
            'payment_type_wid',
            'payment_type_name',
            'payment_type_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from 
    {{source('workday_ods', 'get_payment_types')}} as get_payment_type
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                payment_type_wid = get_payment_type.payment_type_reference_wid
        )
    {%- endif %}    

