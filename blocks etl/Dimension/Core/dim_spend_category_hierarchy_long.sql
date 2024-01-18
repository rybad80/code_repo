{{
  config(
    materialized = 'incremental',
    unique_key = ['spend_cat_wid', 'top_level', 'hier_level_num'],
    incremental_strategy = 'merge',
    merge_update_columns = [ 'spend_cat_id', 'spend_cat_nm', 'spend_cat_wid' ,'top_level', 'spend_category_hierarchy_level', 'spend_category_hierarchy_level_wid', 'hier_level_num', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
with rch as (
    select
        workday_spend_category.spend_cat_key,
        workday_spend_category.spend_cat_id,
        workday_spend_category.spend_cat_nm,
        workday_spend_category.spend_cat_wid,
        spend_category_hierarchy_levels.*
    from
        {{source('workday_ods', 'spend_category_spend_category_hierarchy')}}
            as spend_category_spend_category_hierarchy,
        {{source('workday_ods', 'spend_category_hierarchy_levels')}}
            as spend_category_hierarchy_levels,
        {{source('workday', 'workday_spend_category')}}
            as workday_spend_category
    where
        spend_category_spend_category_hierarchy.spend_category_hierarchy_id
          = spend_category_hierarchy_levels.spend_category_hierarchy_id
        and workday_spend_category.spend_cat_id
          = spend_category_spend_category_hierarchy.spend_category_id
),

levels as (
    select
        spend_cat_id,
        spend_cat_nm,
        spend_cat_wid,
        spend_category_hierarchy_level1_name as top_level,
        spend_category_hierarchy_level1_name as spend_category_hierarchy_level,
        spend_category_hierarchy_level1_wid as spend_category_hierarchy_level_wid,
        1 as hier_level,
        create_by,
        upd_by
    from
        rch
    
    
    union all
    
    
    select
        spend_cat_id,
        spend_cat_nm,
        spend_cat_wid,
        spend_category_hierarchy_level1_name as top_level,
        spend_category_hierarchy_level2_name as spend_category_hierarchy_level,
        spend_category_hierarchy_level2_wid as spend_category_hierarchy_level_wid,
        2 as hier_level,
        create_by,
        upd_by
    from
        rch
    where
        spend_category_hierarchy_level2_name is not null
    
    
    union all
    
    
    select
        spend_cat_id,
        spend_cat_nm,
        spend_cat_wid,
        spend_category_hierarchy_level1_name as top_level,
        spend_category_hierarchy_level3_name as spend_category_hierarchy_level,
        spend_category_hierarchy_level3_wid as spend_category_hierarchy_level_wid,
        3 as hier_level,
        create_by,
        upd_by
    from
        rch
    where
        spend_category_hierarchy_level3_name is not null
    
    union all
    
    select
        spend_cat_id,
        spend_cat_nm,
        spend_cat_wid,
        spend_category_hierarchy_level1_name as top_level,
        spend_category_hierarchy_level4_name as spend_category_hierarchy_level,
        spend_category_hierarchy_level4_wid as spend_category_hierarchy_level_wid,
        4 as hier_level,
        create_by,
        upd_by
    from
        rch
    where
        spend_category_hierarchy_level4_name is not null
),

spend_hier as (
select
    {{
        dbt_utils.surrogate_key([
            'spend_cat_wid',
            'top_level',
            'hier_level'
        ])
    }} as spend_category_hier_key, 
    spend_cat_id,
    spend_cat_nm,
    spend_cat_wid,
    top_level,
    spend_category_hierarchy_level,
    spend_category_hierarchy_level_wid,
    hier_level as hier_level_num,
    {{
        dbt_utils.surrogate_key([
            'spend_cat_id',
            'spend_cat_nm',
            'spend_cat_wid',
            'top_level',
            'spend_category_hierarchy_level',
            'spend_category_hierarchy_level_wid',
            'hier_level'
        ])
    }} as hash_value,
    create_by || '~' || spend_cat_id  || '~' || top_level || '~' || hier_level as integration_id,
    current_timestamp as create_date,
    create_by,
    current_timestamp as update_date,
    upd_by as update_by
from
    levels
where 1 = 1

union all
--
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
    spend_category_hier_key,
    spend_cat_id,
    spend_cat_nm,
    spend_cat_wid,
    top_level,
    spend_category_hierarchy_level,
    spend_category_hierarchy_level_wid,
    hier_level_num,
    hash_value,
    integration_id,
    create_date,
    create_by,
    update_date,
    update_by
from
    spend_hier
where 1=1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where spend_cat_wid = spend_hier.spend_cat_wid
      and top_level = spend_hier.top_level
      and hier_level_num = spend_hier.hier_level_num)
{%- endif %}
