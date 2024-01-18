{{
  config(
    materialized = 'incremental',
    unique_key = ['revenue_cat_wid', 'top_level', 'hier_level_num'],
    incremental_strategy = 'merge',
    merge_update_columns = [ 'revenue_cat_id', 'revenue_cat_nm', 'revenue_cat_wid' ,'top_level', 'revenue_category_hierarchy_level', 'revenue_category_hierarchy_level_wid', 'hier_level_num', 'update_date', 'hash_value', 'integration_id'],
    meta={
        'critical': true
    }
  )
}}
with rch as (
    select
        workday_revenue_category.revenue_cat_key,
        workday_revenue_category.revenue_cat_id,
        workday_revenue_category.revenue_cat_nm,
        workday_revenue_category.revenue_cat_wid,
        revenue_category_hierarchy_levels.*
    from
        {{source('workday_ods', 'revenue_category_revenue_category_hierarchy')}}
            as revenue_category_revenue_category_hierarchy,
        {{source('workday_ods', 'revenue_category_hierarchy_levels')}}
            as revenue_category_hierarchy_levels,
        {{source('workday', 'workday_revenue_category')}}
            as workday_revenue_category
    where
        revenue_category_revenue_category_hierarchy.revenue_category_hierarchy_id
          = revenue_category_hierarchy_levels.revenue_category_hierarchy_id
        and workday_revenue_category.revenue_cat_id
          = revenue_category_revenue_category_hierarchy.revenue_category_id
),

levels as (
    select
        revenue_cat_id,
        revenue_cat_nm,
        revenue_cat_wid,
        revenue_category_hierarchy_level1_name as top_level,
        revenue_category_hierarchy_level1_name as revenue_category_hierarchy_level,
        revenue_category_hierarchy_level1_wid as revenue_category_hierarchy_level_wid,
        1 as hier_level,
        create_by,
        upd_by
    from
        rch
    
    
    union all
    
    
    select
        revenue_cat_id,
        revenue_cat_nm,
        revenue_cat_wid,
        revenue_category_hierarchy_level1_name as top_level,
        revenue_category_hierarchy_level2_name as revenue_category_hierarchy_level,
        revenue_category_hierarchy_level2_wid as revenue_category_hierarchy_level_wid,
        2 as hier_level,
        create_by,
        upd_by
    from
        rch
    where
        revenue_category_hierarchy_level2_name is not null
    
    
    union all
    
    
    select
        revenue_cat_id,
        revenue_cat_nm,
        revenue_cat_wid,
        revenue_category_hierarchy_level1_name as top_level,
        revenue_category_hierarchy_level3_name as revenue_category_hierarchy_level,
        revenue_category_hierarchy_level3_wid as revenue_category_hierarchy_level_wid,
        3 as hier_level,
        create_by,
        upd_by
    from
        rch
    where
        revenue_category_hierarchy_level3_name is not null
),

rev_hier as (
select
    {{
        dbt_utils.surrogate_key([
            'revenue_cat_wid',
            'top_level',
            'hier_level'
        ])
    }} as revenue_category_hier_key, 
    revenue_cat_id,
    revenue_cat_nm,
    revenue_cat_wid,
    top_level,
    revenue_category_hierarchy_level,
    revenue_category_hierarchy_level_wid,
    hier_level as hier_level_num,
    {{
        dbt_utils.surrogate_key([
            'revenue_cat_id',
            'revenue_cat_nm',
            'revenue_cat_wid',
            'top_level',
            'revenue_category_hierarchy_level',
            'revenue_category_hierarchy_level_wid',
            'hier_level'
        ])
    }} as hash_value,
    create_by || '~' || revenue_cat_id  || '~' || top_level || '~' || hier_level as integration_id,
    current_timestamp as create_date,
    create_by,
    current_timestamp as update_date,
    upd_by as update_by
from
    levels
where 1 = 1

union all

select
    0, --key
    'NA', --id
    'NA', --nm
    'NA', --wid
    'NA', --top_lv
    'NA', --hier_lvl
    'NA', --lvl_wid
    0, --lvl
    0, --hash
    'NA', --int_id
    CURRENT_TIMESTAMP, --ct_dt
    'DEFAULT', --by
    CURRENT_TIMESTAMP, --upd_dt
    'DEFAULT' --by
)

select
    revenue_category_hier_key,
    revenue_cat_id,
    revenue_cat_nm,
    revenue_cat_wid,
    top_level,
    revenue_category_hierarchy_level,
    revenue_category_hierarchy_level_wid,
    hier_level_num,
    hash_value,
    integration_id,
    create_date,
    create_by,
    update_date,
    update_by
from
    rev_hier
where 1=1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where revenue_cat_wid = rev_hier.revenue_cat_wid
      and top_level = rev_hier.top_level
      and hier_level_num = rev_hier.hier_level_num)
{%- endif %}
