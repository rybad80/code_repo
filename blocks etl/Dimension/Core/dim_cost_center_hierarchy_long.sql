with cost_center_hier_details
as (
select
    dim_cost_center.cost_center_key,
    dim_cost_center.cost_center_wid as cost_cntr_wid,
    dim_cost_center.cost_center_id as cost_cntr_id,
    dim_cost_center.cost_center_name as cost_cntr_nm,
    cost_center_hierarchy_levels.*
from
    {{source('workday_ods', 'cost_center_cost_center_hierarchy')}} as cost_center_cost_center_hierarchy,
    {{source('workday_ods', 'cost_center_hierarchy_levels')}} as cost_center_hierarchy_levels,
    {{ref('dim_cost_center')}} as dim_cost_center
where
    cost_center_cost_center_hierarchy.cost_center_hierarchy_id
    = cost_center_hierarchy_levels.cost_center_hierarchy_id
    and dim_cost_center.cost_center_id = cost_center_cost_center_hierarchy.cost_center_id
),
cost_center_hier_long
as (
select
    cost_center_key,
    cost_cntr_id,
    cost_cntr_nm,
    cost_cntr_wid,
    cost_center_hierarchy_level1_name as top_level,
    cost_center_hierarchy_level1_name as cost_center_hierarchy_level,
    cost_center_hierarchy_level1_wid as cost_center_hierarchy_level_wid,
    cost_center_hierarchy_level1_id as cost_center_hierarchy_level_id,
    1 as hier_level,
    create_by,
    upd_by
from
    cost_center_hier_details
--
union all
--
select
    cost_center_key,
    cost_cntr_id,
    cost_cntr_nm,
    cost_cntr_wid,
    cost_center_hierarchy_level1_name as top_level,
    cost_center_hierarchy_level2_name as cost_center_hierarchy_level,
    cost_center_hierarchy_level2_wid as cost_center_hierarchy_level_wid,
    cost_center_hierarchy_level2_id as cost_center_hierarchy_level_id,
    2 as hier_level,
    create_by,
    upd_by
from
    cost_center_hier_details
where
    cost_center_hierarchy_level2_name is not null
--
union all
--
select
    cost_center_key,
    cost_cntr_id,
    cost_cntr_nm,
    cost_cntr_wid,
    cost_center_hierarchy_level1_name as top_level,
    cost_center_hierarchy_level3_name as cost_center_hierarchy_level,
    cost_center_hierarchy_level3_wid as cost_center_hierarchy_level_wid,
    cost_center_hierarchy_level3_id as cost_center_hierarchy_level_id,
    3 as hier_level,
    create_by,
    upd_by
from
    cost_center_hier_details
where
    cost_center_hierarchy_level3_name is not null
--
union all
--
select
    cost_center_key,
    cost_cntr_id,
    cost_cntr_nm,
    cost_cntr_wid,
    cost_center_hierarchy_level1_name as top_level,
    cost_center_hierarchy_level4_name as cost_center_hierarchy_level,
    cost_center_hierarchy_level4_wid as cost_center_hierarchy_level_wid,
    cost_center_hierarchy_level4_id as cost_center_hierarchy_level_id,
    4 as hier_level,
    create_by,
    upd_by
from
    cost_center_hier_details
where
    cost_center_hierarchy_level4_name is not null
--
union all
--
select
    cost_center_key,
    cost_cntr_id,
    cost_cntr_nm,
    cost_cntr_wid,
    cost_center_hierarchy_level1_name as top_level,
    cost_center_hierarchy_level5_name as cost_center_hierarchy_level,
    cost_center_hierarchy_level5_wid as cost_center_hierarchy_level_wid,
    cost_center_hierarchy_level5_id as cost_center_hierarchy_level_id,
    5 as hier_level,
    create_by,
    upd_by
from
    cost_center_hier_details
where
    cost_center_hierarchy_level5_name is not null
--
union all
--
select
    cost_center_key,
    cost_cntr_id,
    cost_cntr_nm,
    cost_cntr_wid,
    cost_center_hierarchy_level1_name as top_level,
    cost_center_hierarchy_level6_name as cost_center_hierarchy_level,
    cost_center_hierarchy_level6_wid as cost_center_hierarchy_level_wid,
    cost_center_hierarchy_level6_id as cost_center_hierarchy_level_id,
    6 as hier_level,
    create_by,
    upd_by
from
    cost_center_hier_details
where
    cost_center_hierarchy_level6_name is not null
),
cost_center_hier
as (
select
    {{
        dbt_utils.surrogate_key([
            'cost_cntr_wid',
            'top_level',
            'hier_level'
        ])
    }} as cost_center_hier_key,
    cost_center_key,
    cost_cntr_id,
    cost_cntr_nm,
    cost_cntr_wid,
    top_level,
    cost_center_hierarchy_level,
    cost_center_hierarchy_level_wid,
    cost_center_hierarchy_level_id,
    hier_level as hier_level_num,
    {{
        dbt_utils.surrogate_key([
            'cost_cntr_id',
            'cost_cntr_nm',
            'cost_cntr_wid',
            'top_level',
            'cost_center_hierarchy_level',
            'cost_center_hierarchy_level_wid',
            'cost_center_hierarchy_level_id',
            'hier_level'
        ])
    }} as hash_value,
    create_by || '~' || cost_cntr_id  || '~' || top_level || '~' || hier_level as integration_id,
    current_timestamp as create_date,
    create_by,
    current_timestamp as update_date,
    upd_by as update_by
from
    cost_center_hier_long
where 1 = 1
--
union all
--
select
    0,
    0,
    'NA',
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
    cost_center_hier_key,
    cost_center_key,
    cost_cntr_id,
    cost_cntr_nm,
    cost_cntr_wid,
    top_level,
    cost_center_hierarchy_level,
    cost_center_hierarchy_level_wid,
    cost_center_hierarchy_level_id,
    hier_level_num,
    hash_value,
    integration_id,
    create_date,
    create_by,
    update_date,
    update_by
from
    cost_center_hier
where 1 = 1
