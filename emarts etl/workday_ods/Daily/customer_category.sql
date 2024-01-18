{{ config(
    materialized = 'incremental',
    unique_key = 'customer_category_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['customer_category_wid','customer_category_id','customer_category_name',
        'md5', 'upd_dt', 'upd_by']
) }}

select distinct
    customer_category_reference_wid as customer_category_wid,
    customer_category_reference_customer_category_id as customer_category_id,
    customer_category_data_customer_category_name as customer_category_name,
    cast({{
        dbt_utils.surrogate_key([
            'customer_category_wid',
            'customer_category_id',
            'customer_category_name'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{ source('workday_ods', 'get_customer_categories') }}
where 1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                customer_category_wid = get_customer_categories.customer_category_reference_wid
        )
    {%- endif %}
