{{
  config(
    materialized = 'incremental',
    unique_key = ['project_wid', 'top_level', 'hier_level_num'],
    incremental_strategy = 'merge',
    merge_update_columns = ['project_wid', 'project_id', 'project_name', 'top_level', 'project_hierarchy_level_id', 'project_hierarchy_level_name', 'hier_level_num', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
with project_hier_details
as (
select
    project_project_hierarchy.project_wid,
    project_project_hierarchy.project_id,
    project.project_name,
    project_hierarchy_levels.project_hierarchy_level1_id,
    project_hierarchy_levels.project_hierarchy_level1_name,
    project_hierarchy_levels.project_hierarchy_level2_id,
    project_hierarchy_levels.project_hierarchy_level2_name,
    project_hierarchy_levels.project_hierarchy_level3_id,
    project_hierarchy_levels.project_hierarchy_level3_name
from
    {{source('workday_ods', 'project_project_hierarchy')}} as project_project_hierarchy
inner join {{source('workday_ods', 'project_hierarchy_levels')}} as project_hierarchy_levels on
    project_project_hierarchy.project_hierarchy_id = project_hierarchy_levels.project_hierarchy_id
inner join
    {{source('workday_ods', 'project')}} as project
on project_project_hierarchy.project_id = project.project_id
),
project_hier_long
as (
select
    project_wid,
    project_id,
    project_name,
    project_hierarchy_level1_name as top_level,
    project_hierarchy_level1_id as project_hierarchy_level_id,
    project_hierarchy_level1_name as project_hierarchy_level_name,
    1 as hier_level
from
    project_hier_details
--    
union all
--
select
    project_wid,
    project_id,
    project_name,
    project_hierarchy_level1_name as top_level,
    project_hierarchy_level2_id as project_hierarchy_level_id,
    project_hierarchy_level2_name as project_hierarchy_level_name,
    2 as hier_level
from
    project_hier_details
where
    project_hierarchy_level2_id is not null
--
union all
--
select
    project_wid,
    project_id,
    project_name,
    project_hierarchy_level1_name as top_level,
    project_hierarchy_level3_id as project_hierarchy_level_id,
    project_hierarchy_level3_name as project_hierarchy_level_name,
    3 as hier_level
from
    project_hier_details
where
    project_hierarchy_level3_id is not null
),
project_hier
as (
select
    {{
        dbt_utils.surrogate_key([
            'project_wid',
            'top_level',
            'hier_level'
        ])
    }} as project_hier_key, 
    project_wid,
    project_id,
    project_name,
    top_level,
    project_hierarchy_level_id,
    project_hierarchy_level_name,
    hier_level as hier_level_num,
    {{
        dbt_utils.surrogate_key([
            'project_wid',
            'project_id',
            'project_name',
            'top_level',
            'project_hierarchy_level_id',
            'project_hierarchy_level_name',
            'hier_level'
        ])
    }} as hash_value,
    'WORKDAY' || '~' || project_id  || '~' || top_level || '~' || hier_level as integration_id,
    current_timestamp as create_date,
    'WORKDAY'as create_by,
    current_timestamp as update_date,
    'WORKDAY' as update_by
from
    project_hier_long
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
    project_hier_key, 
    project_wid,
    project_id,
    project_name,
    top_level,
    project_hierarchy_level_id,
    project_hierarchy_level_name,
    hier_level_num,
    hash_value,
    integration_id,
    create_date,
    create_by,
    update_date,
    update_by
from
    project_hier
where 1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where project_wid = project_hier.project_wid
      and top_level = project_hier.top_level
      and hier_level_num = project_hier.hier_level_num)
{%- endif %}
