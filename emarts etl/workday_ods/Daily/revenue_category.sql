{{ config(
    materialized = 'incremental',
    unique_key = 'revenue_category_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['revenue_category_wid','revenue_category_id','revenue_category_name','revenue_category_inactive_ind', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
    revenue_category_reference_wid as revenue_category_wid,
    revenue_category_reference_revenue_category_id as revenue_category_id,
    revenue_category_data_revenue_category_name as revenue_category_name,
    coalesce(cast(revenue_category_data_revenue_category_inactive as int), -2) as revenue_category_inactive_ind,
    cast({{
        dbt_utils.surrogate_key([
            'revenue_category_wid',
            'revenue_category_id',
            'revenue_category_name',
            'revenue_category_inactive_ind'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_revenue_categories')}} as get_revenue_categories
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                revenue_category_wid = get_revenue_categories.revenue_category_reference_wid
        )
    {%- endif %}
