{{ config(
    materialized = 'incremental',
    unique_key = ['customer_wid', 'customer_group_wid'],
    incremental_strategy = 'merge',
    merge_update_columns = [
        'customer_wid',
        'customer_id',
        'customer_group_wid',
        'customer_group_id',
        'md5', 'upd_dt', 'upd_by'
    ]
) }}

select distinct  -- As a bridge table, we need to find all distinct combinations of these fields
    customer_customer_reference_wid as customer_wid,
    customer_customer_data_customer_id as customer_id,
    customer_customer_data_customer_group_reference_wid as customer_group_wid,
    customer_customer_data_customer_group_reference_customer_group_id as customer_group_id,
    cast({{
        dbt_utils.surrogate_key([
            'customer_wid',
            'customer_id',
            'customer_group_wid',
            'customer_group_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{ source('workday_ods', 'get_customers') }} as get_customers
where
    customer_group_wid is not null  -- matches the inner join in the informatica code
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                customer_wid = get_customers.customer_customer_reference_wid
                and customer_group_wid = get_customers.customer_customer_data_customer_group_reference_wid
        )
    {%- endif %}
