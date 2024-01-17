{{
  config(
    meta = {
      'critical': true
    }
  )
}}

with main as (
select
    hier_main.cost_center_hierarchy_key,
    cost_center_hierarchy_levels.cost_center_hierarchy_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_name,
    hier_main.cost_center_parent_hierarchy_key,
    cost_center_hierarchy_levels.cost_center_parent_hierarchy_wid,
    cost_center_hierarchy_levels.cost_center_parent_hierarchy_id,
    hier_main.cost_center_toplevel_hierarchy_key,
    cost_center_hierarchy_levels.cost_center_toplevel_hierarchy_wid,
    cost_center_hierarchy_levels.cost_center_toplevel_hierarchy_id,
    hier_1.cost_center_hierarchy_key as cost_center_hierarchy_level1_key,
    cost_center_hierarchy_levels.cost_center_hierarchy_level1_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level1_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level1_name,
    hier_2.cost_center_hierarchy_key as cost_center_hierarchy_level2_key,
    cost_center_hierarchy_levels.cost_center_hierarchy_level2_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level2_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level2_name,
    hier_3.cost_center_hierarchy_key as cost_center_hierarchy_level3_key,
    cost_center_hierarchy_levels.cost_center_hierarchy_level3_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level3_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level3_name,
    hier_4.cost_center_hierarchy_key as cost_center_hierarchy_level4_key,
    cost_center_hierarchy_levels.cost_center_hierarchy_level4_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level4_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level4_name,
    hier_5.cost_center_hierarchy_key as cost_center_hierarchy_level5_key,
    cost_center_hierarchy_levels.cost_center_hierarchy_level5_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level5_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level5_name,
    hier_6.cost_center_hierarchy_key as cost_center_hierarchy_level6_key,
    cost_center_hierarchy_levels.cost_center_hierarchy_level6_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level6_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level6_name
from
    {{ source('workday_ods', 'cost_center_hierarchy_levels') }} as cost_center_hierarchy_levels
left join {{ ref('workday_cost_center_hierarchy') }} as hier_main
    on cost_center_hierarchy_levels.cost_center_hierarchy_id = hier_main.cost_center_hierarchy_id
left join {{ ref('workday_cost_center_hierarchy') }} as hier_1
    on cost_center_hierarchy_levels.cost_center_hierarchy_level1_wid = hier_1.cost_center_hierarchy_id
left join {{ ref('workday_cost_center_hierarchy') }} as hier_2
    on cost_center_hierarchy_levels.cost_center_hierarchy_level1_wid = hier_2.cost_center_hierarchy_id
left join {{ ref('workday_cost_center_hierarchy') }} as hier_3
    on cost_center_hierarchy_levels.cost_center_hierarchy_level1_wid = hier_3.cost_center_hierarchy_id
left join {{ ref('workday_cost_center_hierarchy') }} as hier_4
    on cost_center_hierarchy_levels.cost_center_hierarchy_level1_wid = hier_4.cost_center_hierarchy_id
left join {{ ref('workday_cost_center_hierarchy') }} as hier_5
    on cost_center_hierarchy_levels.cost_center_hierarchy_level1_wid = hier_5.cost_center_hierarchy_id
left join {{ ref('workday_cost_center_hierarchy') }} as hier_6
    on cost_center_hierarchy_levels.cost_center_hierarchy_level1_wid = hier_6.cost_center_hierarchy_id
),

unionset as (
select
    *,
    'CLARITY' || '~' || cost_center_hierarchy_key as integration_id,
    current_timestamp as update_date,
    'CLARITY' as update_source
from
    main

union all

select
    -2, '-2', 'NOT APPLICABLE', 'NOT APPLICABLE',  -- main
    -2, '-2', 'NOT APPLICABLE',  -- parent
    -2, '-2', 'NOT APPLICABLE',  -- top
    -2, '-2', 'NOT APPLICABLE', 'NOT APPLICABLE',  -- lvl 1
    -2, '-2', 'NOT APPLICABLE', 'NOT APPLICABLE',  -- lvl 2
    -2, '-2', 'NOT APPLICABLE', 'NOT APPLICABLE',  -- lvl 3
    -2, '-2', 'NOT APPLICABLE', 'NOT APPLICABLE',  -- lvl 4
    -2, '-2', 'NOT APPLICABLE', 'NOT APPLICABLE',  -- lvl 5
    -2, '-2', 'NOT APPLICABLE', 'NOT APPLICABLE',  -- lvl 6
    -- additional columns
    'NOT APPLICABLE' as integration_id,
    current_timestamp as update_date,
    'NOT APPLICABLE' as update_source

union all

select
    -1, '-1', 'UNSPECIFIED', 'UNSPECIFIED',  -- main
    -1, '-1', 'UNSPECIFIED',  -- parent
    -1, '-1', 'UNSPECIFIED',  -- top
    -1, '-1', 'UNSPECIFIED', 'UNSPECIFIED',  -- lvl 1
    -1, '-1', 'UNSPECIFIED', 'UNSPECIFIED',  -- lvl 2
    -1, '-1', 'UNSPECIFIED', 'UNSPECIFIED',  -- lvl 3
    -1, '-1', 'UNSPECIFIED', 'UNSPECIFIED',  -- lvl 4
    -1, '-1', 'UNSPECIFIED', 'UNSPECIFIED',  -- lvl 5
    -1, '-1', 'UNSPECIFIED', 'UNSPECIFIED',  -- lvl 6
    -- additional columns
    'UNSPECIFIED' as integration_id,
    current_timestamp as update_date,
    'UNSPECIFIED' as update_source
)

select
    cost_center_hierarchy_key,
    cost_center_hierarchy_wid,
    cost_center_hierarchy_id,
    cost_center_hierarchy_name,
    cost_center_parent_hierarchy_key,
    cost_center_parent_hierarchy_wid,
    cost_center_parent_hierarchy_id,
    cost_center_toplevel_hierarchy_key,
    cost_center_toplevel_hierarchy_wid,
    cost_center_toplevel_hierarchy_id,
    cost_center_hierarchy_level1_key,
    cost_center_hierarchy_level1_wid,
    cost_center_hierarchy_level1_id,
    cost_center_hierarchy_level1_name,
    cost_center_hierarchy_level2_key,
    cost_center_hierarchy_level2_wid,
    cost_center_hierarchy_level2_id,
    cost_center_hierarchy_level2_name,
    cost_center_hierarchy_level3_key,
    cost_center_hierarchy_level3_wid,
    cost_center_hierarchy_level3_id,
    cost_center_hierarchy_level3_name,
    cost_center_hierarchy_level4_key,
    cost_center_hierarchy_level4_wid,
    cost_center_hierarchy_level4_id,
    cost_center_hierarchy_level4_name,
    cost_center_hierarchy_level5_key,
    cost_center_hierarchy_level5_wid,
    cost_center_hierarchy_level5_id,
    cost_center_hierarchy_level5_name,
    cost_center_hierarchy_level6_key,
    cost_center_hierarchy_level6_wid,
    cost_center_hierarchy_level6_id,
    cost_center_hierarchy_level6_name,
    integration_id,
    update_date,
    update_source
from
    unionset
