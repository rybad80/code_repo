{{
  config(
    meta = {
        'critical': true
    }
  )
}}
--
with cost_center_site_hier_details
as (
select
	cost_center_site.cost_center_site_id,
	cost_center_site.cost_center_site_name,
	cost_center_site.cost_center_site_wid,
	cost_center_site_hierarchy_levels.*
from
	{{source('workday_ods', 'cost_center_site_cost_center_site_hierarchy')}} as cost_center_site_cost_center_site_hierarchy,
	{{source('workday_ods', 'cost_center_site_hierarchy_levels')}} as cost_center_site_hierarchy_levels,
	{{source('workday_ods', 'cost_center_site')}} as cost_center_site
where
	cost_center_site_cost_center_site_hierarchy.cost_center_site_hierarchy_id
	= cost_center_site_hierarchy_levels.cost_center_site_hierarchy_id
	and cost_center_site.cost_center_site_id = cost_center_site_cost_center_site_hierarchy.cost_center_site_id
),
cost_center_site_hier_long
as (
select
    cost_center_site_id,
    cost_center_site_wid,
    cost_center_site_name,
    cost_center_site_hierarchy_level1_name as top_level,
    cost_center_site_hierarchy_level1_name as cost_center_site_hierarchy_level,
    1 as hier_level,
    create_by,
    upd_by
from
    cost_center_site_hier_details
--    
union all
--
select
    cost_center_site_id,
    cost_center_site_wid,
    cost_center_site_name,
    cost_center_site_hierarchy_level1_name as top_level,
    cost_center_site_hierarchy_level2_name as cost_center_site_hierarchy_level,
    2 as hier_level,
    create_by,
    upd_by
from
    cost_center_site_hier_details
where
    cost_center_site_hierarchy_level2_name is not null
),
cost_center_site_hier
as (
select
    {{
        dbt_utils.surrogate_key([
            'cost_center_site_wid',
            'top_level',
            'hier_level'
        ])
    }} as cost_center_site_hier_key, 
    cost_center_site_id,
    cost_center_site_wid,
    cost_center_site_name,
    top_level,
    cost_center_site_hierarchy_level,
    hier_level as hier_level_num,
    {{
        dbt_utils.surrogate_key([
            'cost_center_site_id',
            'cost_center_site_wid',
            'cost_center_site_name',
            'top_level',
            'cost_center_site_hierarchy_level',
            'hier_level'
        ])
    }} as hash_value,
    create_by || '~' || cost_center_site_id  || '~' || top_level || '~' || hier_level as integration_id,
    current_timestamp as create_date,
    create_by,
    current_timestamp as update_date,
    upd_by as update_by
from
    cost_center_site_hier_long
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
    cost_center_site_hier_key, 
    cost_center_site_id,
    cost_center_site_wid,
    cost_center_site_name,
    top_level,
    cost_center_site_hierarchy_level,
    hier_level_num,
    hash_value,
    integration_id,
    create_date,
    create_by,
    update_date,
    update_by
from
    cost_center_site_hier
where 1 = 1
