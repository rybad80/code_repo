{{
  config(
    materialized = 'incremental',
    unique_key = ['location_wid', 'top_level', 'hier_level_num'],
    incremental_strategy = 'merge',
    merge_update_columns = ['location_id', 'location', 'location_wid', 'top_level', 'location_hierarchy_level', 'location_hierarchy_level_wid', 'hier_level_num', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
with location_hier_details
as (
select
    location_reference_id as location_id,
    location_wid,
    location,
    location_hierarchy_5,
    location_hierarchy_5_reference_id,
    location_hierarchy_5_wid,
    location_hierarchy_4,
    location_hierarchy_4_reference_id,
    location_hierarchy_4_wid,
    location_hierarchy_3,
    location_hierarchy_3_reference_id,
    location_hierarchy_3_wid,
    location_hierarchy_2,
    location_hierarchy_2_reference_id,
    location_hierarchy_2_wid,
    location_hierarchy_1,
    location_hierarchy_1_reference_id,
    location_hierarchy_1_wid,
    upd_dt
from
    {{source('workday_ods', 'workday_location_hierarchy')}}
),
location_hier_long
as (
select
    location_id,
    location,
    location_wid,
    location_hierarchy_1 as top_level,
    location_hierarchy_1 as location_hierarchy_level,
    location_hierarchy_1_wid as location_hierarchy_level_wid,
    1 as hier_level,
    'WORKDAY' as create_by,
    'WORKDAY' as upd_by
from
    location_hier_details
--    
union all
--
select
    location_id,
    location,
    location_wid,
    location_hierarchy_1 as top_level,
    location_hierarchy_2 as location_hierarchy_level,
    location_hierarchy_2_wid as location_hierarchy_level_wid,
    2 as hier_level,
    'WORKDAY' as create_by,
    'WORKDAY' as upd_by
from
    location_hier_details
where
    location_hierarchy_2 is not null
--    
union all
--
select
    location_id,
    location,
    location_wid,
    location_hierarchy_1 as top_level,
    location_hierarchy_3 as location_hierarchy_level,
    location_hierarchy_3_wid as location_hierarchy_level_wid,
    3 as hier_level,
    'WORKDAY' as create_by,
    'WORKDAY' as upd_by
from
    location_hier_details
where
    location_hierarchy_3_wid is not null
--    
union all
--
select
    location_id,
    location,
    location_wid,
    location_hierarchy_1 as top_level,
    location_hierarchy_4 as location_hierarchy_level,
    location_hierarchy_4_wid as location_hierarchy_level_wid,
    4 as hier_level,
    'WORKDAY' as create_by,
    'WORKDAY' as upd_by
from
    location_hier_details
where
    location_hierarchy_4_wid is not null
--    
union all
--
select
    location_id,
    location,
    location_wid,
    location_hierarchy_1 as top_level,
    location_hierarchy_5 as location_hierarchy_level,
    location_hierarchy_5_wid as location_hierarchy_level_wid,
    5 as hier_level,
    'WORKDAY' as create_by,
    'WORKDAY' as upd_by
from
    location_hier_details
where
    location_hierarchy_5_wid is not null
),
location_hier
as (
select
    {{
        dbt_utils.surrogate_key([
            'location_wid',
            'top_level',
            'hier_level'
        ])
    }} as location_hier_key, 
    location_id,
    location,
    location_wid,
    top_level,
    location_hierarchy_level,
    location_hierarchy_level_wid,
    hier_level as hier_level_num,
    {{
        dbt_utils.surrogate_key([
            'location_id',
            'location',
            'location_wid',
            'top_level',
            'location_hierarchy_level',
            'location_hierarchy_level_wid',
            'hier_level'
        ])
    }} as hash_value,
    create_by || '~' || location_id  || '~' || top_level || '~' || hier_level as integration_id,
    current_timestamp as create_date,
    create_by,
    current_timestamp as update_date,
    upd_by as update_by
from
    location_hier_long
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
    location_hier_key, 
    location_id,
    location,
    location_wid,
    top_level,
    location_hierarchy_level,
    location_hierarchy_level_wid,
    hier_level_num,
    hash_value,
    integration_id,
    create_date,
    create_by,
    update_date,
    update_by
from
    location_hier
where 1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where location_wid = location_hier.location_wid
      and top_level = location_hier.top_level
      and hier_level_num = location_hier.hier_level_num
    )
{%- endif %}
