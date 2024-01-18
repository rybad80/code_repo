{{ config(
    materialized = 'incremental',
    unique_key = 'revenue_category_hierarchy_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['revenue_category_hierarchy_wid','revenue_category_hierarchy_id','revenue_category_hierarchy_name','revenue_category_hierarchy_level_id', 'parent_revenue_category_hierarchy_wid', 'parent_revenue_category_hierarchy_id', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
revenue_category_hierarchy_reference_wid as revenue_category_hierarchy_wid,
revenue_category_hierarchy_reference_revenue_category_hierarchy_id as revenue_category_hierarchy_id,
revenue_category_hierarchy_data_revenue_category_hierarchy_name as revenue_category_hierarchy_name,
null as revenue_category_hierarchy_level_id,
parent_revenue_category_hierarchy_reference_wid as parent_revenue_category_hierarchy_wid,
parent_revenue_category_hierarchy_reference_revenue_category_hierarchy_id as parent_revenue_category_hierarchy_id,
    cast({{
        dbt_utils.surrogate_key([
            'revenue_category_hierarchy_wid',
            'revenue_category_hierarchy_id',
            'revenue_category_hierarchy_name',
            'revenue_category_hierarchy_level_id',
            'parent_revenue_category_hierarchy_wid',
            'parent_revenue_category_hierarchy_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_revenue_category_hierarchies')}} as get_revenue_category_hierarchies
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                revenue_category_hierarchy_wid = get_revenue_category_hierarchies.revenue_category_hierarchy_reference_wid
        )
    {%- endif %}
