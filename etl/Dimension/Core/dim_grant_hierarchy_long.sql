with
grant_hier
as (
select
    {{
        dbt_utils.surrogate_key([
            'grant_wid',
            'grant_hierarchy_top_level_name',
            'hier_level_num'
        ])
    }} as grant_hier_key, 
    grant_reference_id as grant_id,
    grant_name,
    grant_wid,
    grant_hierarchy_top_level_name,
    grant_hierarchy_id as grant_hierarchy_level_id,
    grant_hierarchy_name as grant_hierarchy_level_name,
    hier_level_num + 1 as hier_level_num,
    {{
        dbt_utils.surrogate_key([
            'grant_reference_id',
            'grant_name',
            'grant_wid',
            'grant_hierarchy_top_level_name',
            'grant_hierarchy_id',
            'grant_hierarchy_name',
            'hier_level_num'
        ])
    }} as hash_value,
    'WORKDAY'
    || '~'
    || grant_reference_id
    || '~'
    || grant_hierarchy_top_level_name
    || '~'
    || (hier_level_num + 1) as integration_id,
    current_timestamp as create_date,
    'WORKDAY' as create_by,
    current_timestamp as update_date,
    'WORKDAY' as update_by
from
    {{source('workday_ods', 'workday_grant_hierarchy')}} as workday_grant_hierarchy
where 1 = 1
  and grant_hierarchy_id not like 'GCCH_%'  -- Temporary fix until the issue is fixed in Workday.
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
    grant_hier_key, 
    grant_id,
    grant_name,
    grant_wid,
    grant_hierarchy_top_level_name,
    grant_hierarchy_level_id,
    grant_hierarchy_level_name,
    hier_level_num,
    hash_value,
    integration_id,
    create_date,
    create_by,
    update_date,
    update_by
from
    grant_hier
where 1 = 1
