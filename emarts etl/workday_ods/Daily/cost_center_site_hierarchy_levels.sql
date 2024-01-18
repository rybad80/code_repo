with level_1_hierarchy as (
    select 
        cost_center_site_hierarchy.cost_center_site_hierarchy_wid as cost_center_site_hierarchy_wid,
        cost_center_site_hierarchy.cost_center_site_hierarchy_id as cost_center_site_hierarchy_id,
        cost_center_site_hierarchy.cost_center_site_hierarchy_name as cost_center_site_hierarchy_name,
        cost_center_site_hierarchy.organization_type_id,
        cost_center_site_hierarchy.organization_subtype_id,
        cost_center_site_hierarchy.cost_center_site_parent_hierarchy_wid,
        cost_center_site_hierarchy.cost_center_site_parent_hierarchy_id,
        cost_center_site_hierarchy.cost_center_site_toplevel_hierarchy_wid,
        cost_center_site_hierarchy.cost_center_site_toplevel_hierarchy_id,
        cost_center_site_hierarchy.cost_center_site_hierarchy_wid as cost_center_site_hierarchy_level1_wid,
        cost_center_site_hierarchy.cost_center_site_hierarchy_id as cost_center_site_hierarchy_level1_id,
        cost_center_site_hierarchy.cost_center_site_hierarchy_name as cost_center_site_hierarchy_level1_name,
        null as cost_center_site_hierarchy_level2_wid,
        null as cost_center_site_hierarchy_level2_id,
        null as cost_center_site_hierarchy_level2_name,
        null as cost_center_site_hierarchy_level3_wid,
        null as cost_center_site_hierarchy_level3_id,
        null as cost_center_site_hierarchy_level3_name,
        null as cost_center_site_hierarchy_level4_wid,
        null as cost_center_site_hierarchy_level4_id,
        null as cost_center_site_hierarchy_level4_name,
        null as cost_center_site_hierarchy_level5_wid,
        null as cost_center_site_hierarchy_level5_id,
        null as cost_center_site_hierarchy_level5_name,
        null as cost_center_site_hierarchy_level6_wid,
        null as cost_center_site_hierarchy_level6_id,
        null as cost_center_site_hierarchy_level6_name
    from
        {{ref('cost_center_site_hierarchy')}} as cost_center_site_hierarchy
    where
        (cost_center_site_hierarchy.organization_type_id = 'Cost_Center_Site_Hierarchy' and cost_center_site_hierarchy.organization_subtype_id = 'Top_Level')
        or (cost_center_site_hierarchy.organization_type_id = 'Cost_Center_Site_Hierarchy'
            and cost_center_site_hierarchy.cost_center_site_hierarchy_wid = cost_center_site_hierarchy.cost_center_site_toplevel_hierarchy_wid
        )
),
level_2_hierarchy as (
    select
        cost_center_site_hierarchy.cost_center_site_hierarchy_wid as cost_center_site_hierarchy_wid,
        cost_center_site_hierarchy.cost_center_site_hierarchy_id as cost_center_site_hierarchy_id,
        cost_center_site_hierarchy.cost_center_site_hierarchy_name as cost_center_site_hierarchy_name,
        cost_center_site_hierarchy.cost_center_site_parent_hierarchy_id,
        cost_center_site_hierarchy.cost_center_site_parent_hierarchy_wid,
        cost_center_site_hierarchy.cost_center_site_toplevel_hierarchy_id,
        cost_center_site_hierarchy.cost_center_site_toplevel_hierarchy_wid,
        level_1_hierarchy.cost_center_site_hierarchy_level1_wid,
        level_1_hierarchy.cost_center_site_hierarchy_level1_id,
        level_1_hierarchy.cost_center_site_hierarchy_level1_name,
        cost_center_site_hierarchy.cost_center_site_hierarchy_wid as cost_center_site_hierarchy_level2_wid,
        cost_center_site_hierarchy.cost_center_site_hierarchy_id as cost_center_site_hierarchy_level2_id,
        cost_center_site_hierarchy.cost_center_site_hierarchy_name as cost_center_site_hierarchy_level2_name,
        null as cost_center_site_hierarchy_level3_wid,
        null as cost_center_site_hierarchy_level3_id,
        null as cost_center_site_hierarchy_level3_name,
        null as cost_center_site_hierarchy_level4_wid,
        null as cost_center_site_hierarchy_level4_id,
        null as cost_center_site_hierarchy_level4_name,
        null as cost_center_site_hierarchy_level5_wid,
        null as cost_center_site_hierarchy_level5_id,
        null as cost_center_site_hierarchy_level5_name,
        null as cost_center_site_hierarchy_level6_wid,
        null as cost_center_site_hierarchy_level6_id,
        null as cost_center_site_hierarchy_level6_name
    from
        {{ref('cost_center_site_hierarchy')}} as cost_center_site_hierarchy
    inner join
        level_1_hierarchy
            on cost_center_site_hierarchy.cost_center_site_parent_hierarchy_wid = level_1_hierarchy.cost_center_site_hierarchy_level1_wid
),
level_3_hierarchy as (
    select
        cost_center_site_hierarchy.cost_center_site_hierarchy_wid as cost_center_site_hierarchy_wid,
        cost_center_site_hierarchy.cost_center_site_hierarchy_id as cost_center_site_hierarchy_id,
        cost_center_site_hierarchy.cost_center_site_hierarchy_name as cost_center_site_hierarchy_name,
        cost_center_site_hierarchy.cost_center_site_parent_hierarchy_id,
        cost_center_site_hierarchy.cost_center_site_parent_hierarchy_wid,
        cost_center_site_hierarchy.cost_center_site_toplevel_hierarchy_id,
        cost_center_site_hierarchy.cost_center_site_toplevel_hierarchy_wid,
        level_2_hierarchy.cost_center_site_hierarchy_level1_wid,
        level_2_hierarchy.cost_center_site_hierarchy_level1_id,
        level_2_hierarchy.cost_center_site_hierarchy_level1_name,
        level_2_hierarchy.cost_center_site_hierarchy_level2_wid,
        level_2_hierarchy.cost_center_site_hierarchy_level2_id,
        level_2_hierarchy.cost_center_site_hierarchy_level2_name,
        cost_center_site_hierarchy.cost_center_site_hierarchy_wid as cost_center_site_hierarchy_level3_wid,
        cost_center_site_hierarchy.cost_center_site_hierarchy_id as cost_center_site_hierarchy_level3_id,
        cost_center_site_hierarchy.cost_center_site_hierarchy_name as cost_center_site_hierarchy_level3_name,
        null as cost_center_site_hierarchy_level4_wid,
        null as cost_center_site_hierarchy_level4_id,
        null as cost_center_site_hierarchy_level4_name,
        null as cost_center_site_hierarchy_level5_wid,
        null as cost_center_site_hierarchy_level5_id,
        null as cost_center_site_hierarchy_level5_name,
        null as cost_center_site_hierarchy_level6_wid,
        null as cost_center_site_hierarchy_level6_id,
        null as cost_center_site_hierarchy_level6_name
    from
        {{ref('cost_center_site_hierarchy')}} as cost_center_site_hierarchy
    inner join
        level_2_hierarchy
            on cost_center_site_hierarchy.cost_center_site_parent_hierarchy_wid = level_2_hierarchy.cost_center_site_hierarchy_level2_wid
),
finaloutput as (
    select
        lvl1.cost_center_site_hierarchy_wid,
        lvl1.cost_center_site_hierarchy_id,
        lvl1.cost_center_site_hierarchy_name,
        lvl1.cost_center_site_parent_hierarchy_wid,
        lvl1.cost_center_site_parent_hierarchy_id,
        lvl1.cost_center_site_toplevel_hierarchy_wid,
        lvl1.cost_center_site_toplevel_hierarchy_id,
        lvl1.cost_center_site_hierarchy_level1_wid,
        lvl1.cost_center_site_hierarchy_level1_id,
        lvl1.cost_center_site_hierarchy_level1_name,
        lvl1.cost_center_site_hierarchy_level2_wid,
        lvl1.cost_center_site_hierarchy_level2_id,
        lvl1.cost_center_site_hierarchy_level2_name
    from
        level_1_hierarchy lvl1
    union
    select
        lvl2.cost_center_site_hierarchy_wid,
        lvl2.cost_center_site_hierarchy_id,
        lvl2.cost_center_site_hierarchy_name,
        lvl2.cost_center_site_parent_hierarchy_wid,
        lvl2.cost_center_site_parent_hierarchy_id,
        lvl2.cost_center_site_toplevel_hierarchy_wid,
        lvl2.cost_center_site_toplevel_hierarchy_id,
        lvl2.cost_center_site_hierarchy_level1_wid,
        lvl2.cost_center_site_hierarchy_level1_id,
        lvl2.cost_center_site_hierarchy_level1_name,
        lvl2.cost_center_site_hierarchy_level2_wid,
        lvl2.cost_center_site_hierarchy_level2_id,
        lvl2.cost_center_site_hierarchy_level2_name
    from level_2_hierarchy lvl2
)
select distinct
    finaloutput.cost_center_site_hierarchy_wid,
    finaloutput.cost_center_site_hierarchy_id,
    finaloutput.cost_center_site_hierarchy_name,
    finaloutput.cost_center_site_parent_hierarchy_wid,
    finaloutput.cost_center_site_parent_hierarchy_id,
    finaloutput.cost_center_site_toplevel_hierarchy_wid,
    finaloutput.cost_center_site_toplevel_hierarchy_id,
    finaloutput.cost_center_site_hierarchy_level1_wid,
    finaloutput.cost_center_site_hierarchy_level1_id,
    finaloutput.cost_center_site_hierarchy_level1_name,
    finaloutput.cost_center_site_hierarchy_level2_wid,
    finaloutput.cost_center_site_hierarchy_level2_id,
    finaloutput.cost_center_site_hierarchy_level2_name,
    cast({{
        dbt_utils.surrogate_key([
            'cost_center_site_hierarchy_wid',
            'cost_center_site_hierarchy_id',
            'cost_center_site_hierarchy_name',
            'cost_center_site_parent_hierarchy_wid',
            'cost_center_site_parent_hierarchy_id',
            'cost_center_site_toplevel_hierarchy_wid',
            'cost_center_site_toplevel_hierarchy_id',
            'cost_center_site_hierarchy_level1_wid',
            'cost_center_site_hierarchy_level1_id',
            'cost_center_site_hierarchy_level1_name',
            'cost_center_site_hierarchy_level2_wid',
            'cost_center_site_hierarchy_level2_id',
            'cost_center_site_hierarchy_level2_name'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    finaloutput
where
    1 = 1
