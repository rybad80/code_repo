{{ config(
    materialized = 'incremental',
    unique_key = 'customer_group_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['customer_group_wid', 'customer_group_id', 'customer_group_name',
        'md5', 'upd_dt', 'upd_by']
) }}

select distinct
    customer_group_reference_wid as customer_group_wid,
    customer_group_reference_customer_group_id as customer_group_id,
    customer_group_data_customer_group_name as customer_group_name,
    cast({{
        dbt_utils.surrogate_key([
            'customer_group_wid',
            'customer_group_id',
            'customer_group_name'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{ source('workday_ods', 'get_customer_groups') }} as get_customer_groups
where 1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select
                md5
            from
                {{ this }}
            where
                customer_group_wid = get_customer_groups.customer_group_reference_wid
        )
    {%- endif %}
