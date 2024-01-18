{{
  config(
    materialized = 'incremental',
    unique_key = ['payor_wid', 'top_level', 'hier_level_num'],
    incremental_strategy = 'merge',
    merge_update_columns = [ 'payor_id', 'payor_name', 'top_level', 'payor_hierarchy_level', 'hier_level_num', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
with payor_hier_details
as (
select
    payor.payor_id,
    payor.payor_wid,
    payor.payor_name,
    payor_level3_hier_name.payor_hierarchy_name as payor_hierarchy_level3_name,
    case when payor_level2_hier_name.payor_hierarchy_name = payor_level1_hier_name.payor_hierarchy_name
        then payor_level3_hier_name.payor_hierarchy_name
        else payor_level2_hier_name.payor_hierarchy_name
        end as payor_hierarchy_level2_name,
    payor_level1_hier_name.payor_hierarchy_name as payor_hierarchy_level1_name,
	payor.create_by,
	payor.upd_by
from
    {{source('workday_ods', 'payor')}} as payor
    inner join {{source('workday_ods', 'payor_payor_hierarchy')}} as payor_level3_hier_id
        on payor.payor_id = payor_level3_hier_id.payor_id
    inner join {{source('workday_ods', 'payor_hierarchy')}} as payor_level3_hier_name
        on payor_level3_hier_id.payor_hierarchy_id = payor_level3_hier_name.payor_hierarchy_id
    inner join {{source('workday_ods', 'payor_hierarchy')}} as payor_level2_hier_name
        on payor_level2_hier_name.payor_hierarchy_id = payor_level3_hier_name.payor_parent_hierarchy_id
    inner join {{source('workday_ods', 'payor_hierarchy')}} as payor_level1_hier_name
        on payor_level1_hier_name.payor_hierarchy_id = payor_level3_hier_name.payor_toplevel_hierarchy_id 
where
    1 = 1
),
payor_hier_long
as (
select
    payor_id,
    payor_wid,
    payor_name,
    payor_hierarchy_level1_name as top_level,
    payor_hierarchy_level1_name as payor_hierarchy_level,
    1 as hier_level,
    create_by,
    upd_by
from
    payor_hier_details
--    
union all
--
select
    payor_id,
    payor_wid,
    payor_name,
    payor_hierarchy_level1_name as top_level,
    payor_hierarchy_level2_name as payor_hierarchy_level,
    2 as hier_level,
    create_by,
    upd_by
from
    payor_hier_details
where
    payor_hierarchy_level2_name is not null
--    
union all
--
select
    payor_id,
    payor_wid,
    payor_name,
    payor_hierarchy_level1_name as top_level,
    payor_hierarchy_level3_name as payor_hierarchy_level,
    3 as hier_level,
    create_by,
    upd_by
from
    payor_hier_details
where
    payor_hierarchy_level3_name is not null
),
payor_hier
as (
select
    {{
        dbt_utils.surrogate_key([
            'payor_wid',
            'top_level',
            'hier_level'
        ])
    }} as payor_hier_key, 
    payor_id,
    payor_wid,
    payor_name,
    top_level,
    payor_hierarchy_level,
    hier_level as hier_level_num,
    {{
        dbt_utils.surrogate_key([
            'payor_id',
            'payor_wid',
            'payor_name',
            'top_level',
            'payor_hierarchy_level',
            'hier_level'
        ])
    }} as hash_value,
    create_by || '~' || payor_id  || '~' || top_level || '~' || hier_level as integration_id,
    current_timestamp as create_date,
    create_by,
    current_timestamp as update_date,
    upd_by as update_by
from
    payor_hier_long
where 1 = 1
--
union all
--
select
    0, 
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    0,
    0,
    'NA',
    CURRENT_TIMESTAMP,
    'DEFAULT',
    CURRENT_TIMESTAMP, 
    'DEFAULT'
)
select
    payor_hier_key, 
    payor_id,
    payor_wid,
    payor_name,
    top_level,
    payor_hierarchy_level,
    hier_level_num,
    hash_value,
    integration_id,
    create_date,
    create_by,
    update_date,
    update_by
from
    payor_hier
where 1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where payor_wid = payor_hier.payor_wid
      and top_level = payor_hier.top_level
      and hier_level_num = payor_hier.hier_level_num)
{%- endif %}
