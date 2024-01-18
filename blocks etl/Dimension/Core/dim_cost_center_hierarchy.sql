{{
config(
    meta = {
    'critical': true
    }
)
}}
--
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'cost_center_hierarchy_levels'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
{% set column_names_additional = ['cost_cntr_nm', 'cost_cntr_id'] %}
{% set column_names_all = [] %}
{% for m in column_names_additional|list + column_names|list %}
{{ column_names_all.append(m) or ""  }}
{% endfor %}
--
with cost_center_cost_center_hier
as (
select 
    cost_center_cost_center_hierarchy.cost_center_hierarchy_id as cost_center_cost_center_hierarchy_id,
    cost_center_cost_center_hierarchy.cost_center_id as cost_center_cost_center_id
from
{{source('workday_ods', 'cost_center_cost_center_hierarchy')}} as cost_center_cost_center_hierarchy
), 
cost_center_hier
as (
select
    {{
        dbt_utils.surrogate_key([
            'workday_cost_center.cost_cntr_wid'
        ])
    }} as cost_center_hier_key,
    workday_cost_center.cost_cntr_wid,
    workday_cost_center.cost_cntr_id,
    workday_cost_center.cost_cntr_nm,
    cost_center_hierarchy_levels.cost_center_hierarchy_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_name,
    cost_center_hierarchy_levels.cost_center_parent_hierarchy_wid,
    cost_center_hierarchy_levels.cost_center_parent_hierarchy_id,
    cost_center_hierarchy_levels.cost_center_toplevel_hierarchy_wid,
    cost_center_hierarchy_levels.cost_center_toplevel_hierarchy_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level1_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level1_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level1_name,
    cost_center_hierarchy_levels.cost_center_hierarchy_level2_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level2_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level2_name,
    cost_center_hierarchy_levels.cost_center_hierarchy_level3_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level3_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level3_name,
    cost_center_hierarchy_levels.cost_center_hierarchy_level4_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level4_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level4_name,
    cost_center_hierarchy_levels.cost_center_hierarchy_level5_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level5_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level5_name,
    cost_center_hierarchy_levels.cost_center_hierarchy_level6_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level6_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level6_name,
    cost_center_hierarchy_levels.cost_center_hierarchy_level2_alternate_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level2_alternate_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level2_alternate_name,
    cost_center_hierarchy_levels.cost_center_hierarchy_level3_alternate_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level3_alternate_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level3_alternate_name,
    cost_center_hierarchy_levels.cost_center_hierarchy_level4_alternate_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level4_alternate_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level4_alternate_name,
    cost_center_hierarchy_levels.cost_center_hierarchy_level5_alternate_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level5_alternate_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level5_alternate_name,
    cost_center_hierarchy_levels.cost_center_hierarchy_level6_alternate_wid,
    cost_center_hierarchy_levels.cost_center_hierarchy_level6_alternate_id,
    cost_center_hierarchy_levels.cost_center_hierarchy_level6_alternate_name,
    {{
        dbt_utils.surrogate_key(column_names_all or [] )
    }} as hash_value,
    cost_center_hierarchy_levels.create_by
    || '~'
    || workday_cost_center.cost_cntr_id
    || '~'
    || cost_center_hierarchy_levels.cost_center_hierarchy_level1_name as integration_id,
    current_timestamp as create_date,
    cost_center_hierarchy_levels.create_by,
    current_timestamp as update_date,
    cost_center_hierarchy_levels.upd_by as update_by
from
    cost_center_cost_center_hier,
    {{source('workday_ods', 'cost_center_hierarchy_levels')}} as cost_center_hierarchy_levels,
    {{source('workday', 'workday_cost_center')}} as workday_cost_center
where
    cost_center_cost_center_hier.cost_center_cost_center_hierarchy_id
    = cost_center_hierarchy_levels.cost_center_hierarchy_id
    and workday_cost_center.cost_cntr_id = cost_center_cost_center_hier.cost_center_cost_center_id 
    and cost_center_hierarchy_level1_id = 'CCH_Alll_Cost_Centers'
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
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    0,
    'NA',
    CURRENT_TIMESTAMP,
    'DEFAULT',
    CURRENT_TIMESTAMP, 
    'DEFAULT'    
)
select
    cost_center_hier.cost_center_hier_key,
    cost_center_hier.cost_cntr_wid,
    cost_center_hier.cost_cntr_id,
    cost_center_hier.cost_cntr_nm,
    cost_center_hier.cost_center_hierarchy_wid,
    cost_center_hier.cost_center_hierarchy_id,
    cost_center_hier.cost_center_hierarchy_name,
    cost_center_hier.cost_center_parent_hierarchy_wid,
    cost_center_hier.cost_center_parent_hierarchy_id,
    cost_center_hier.cost_center_toplevel_hierarchy_wid,
    cost_center_hier.cost_center_toplevel_hierarchy_id,
    cost_center_hier.cost_center_hierarchy_level1_wid,
    cost_center_hier.cost_center_hierarchy_level1_id,
    cost_center_hier.cost_center_hierarchy_level1_name,
    cost_center_hier.cost_center_hierarchy_level2_wid,
    cost_center_hier.cost_center_hierarchy_level2_id,
    cost_center_hier.cost_center_hierarchy_level2_name,
    cost_center_hier.cost_center_hierarchy_level3_wid,
    cost_center_hier.cost_center_hierarchy_level3_id,
    cost_center_hier.cost_center_hierarchy_level3_name,
    cost_center_hier.cost_center_hierarchy_level4_wid,
    cost_center_hier.cost_center_hierarchy_level4_id,
    cost_center_hier.cost_center_hierarchy_level4_name,
    cost_center_hier.cost_center_hierarchy_level5_wid,
    cost_center_hier.cost_center_hierarchy_level5_id,
    cost_center_hier.cost_center_hierarchy_level5_name,
    cost_center_hier.cost_center_hierarchy_level6_wid,
    cost_center_hier.cost_center_hierarchy_level6_id,
    cost_center_hier.cost_center_hierarchy_level6_name,
    cost_center_hier.cost_center_hierarchy_level2_alternate_wid,
    cost_center_hier.cost_center_hierarchy_level2_alternate_id,
    cost_center_hier.cost_center_hierarchy_level2_alternate_name,
    cost_center_hier.cost_center_hierarchy_level3_alternate_wid,
    cost_center_hier.cost_center_hierarchy_level3_alternate_id,
    cost_center_hier.cost_center_hierarchy_level3_alternate_name,
    cost_center_hier.cost_center_hierarchy_level4_alternate_wid,
    cost_center_hier.cost_center_hierarchy_level4_alternate_id,
    cost_center_hier.cost_center_hierarchy_level4_alternate_name,
    cost_center_hier.cost_center_hierarchy_level5_alternate_wid,
    cost_center_hier.cost_center_hierarchy_level5_alternate_id,
    cost_center_hier.cost_center_hierarchy_level5_alternate_name,
    cost_center_hier.cost_center_hierarchy_level6_alternate_wid,
    cost_center_hier.cost_center_hierarchy_level6_alternate_id,
    cost_center_hier.cost_center_hierarchy_level6_alternate_name,
    cost_center_hier.hash_value,
    cost_center_hier.integration_id,
    cost_center_hier.create_date,
    cost_center_hier.create_by,
    cost_center_hier.update_date,
    cost_center_hier.update_by
from
    cost_center_hier
where 1 = 1

