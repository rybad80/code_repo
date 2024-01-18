with level_1_hierarchy as (
select distinct
    cost_center_hierarchy.cost_center_hierarchy_wid as cost_center_hierarchy_wid,
    cost_center_hierarchy.cost_center_hierarchy_id as cost_center_hierarchy_id,
    cost_center_hierarchy.cost_center_hierarchy_name as cost_center_hierarchy_name,
    cost_center_hierarchy.organization_type_id,
    cost_center_hierarchy.organization_subtype_id,
    cost_center_hierarchy.cost_center_parent_hierarchy_wid,
    cost_center_hierarchy.cost_center_parent_hierarchy_id,
    cost_center_hierarchy.cost_center_toplevel_hierarchy_wid,
    cost_center_hierarchy.cost_center_toplevel_hierarchy_id,
    cost_center_hierarchy.cost_center_hierarchy_wid as cost_center_hierarchy_level1_wid,
    cost_center_hierarchy.cost_center_hierarchy_id as cost_center_hierarchy_level1_id,
    cost_center_hierarchy.cost_center_hierarchy_name as cost_center_hierarchy_level1_name,
    null as cost_center_hierarchy_level2_wid,
    null as cost_center_hierarchy_level2_id,
    null as cost_center_hierarchy_level2_name,
    null as cost_center_hierarchy_level3_wid,
    null as cost_center_hierarchy_level3_id,
    null as cost_center_hierarchy_level3_name,
    null as cost_center_hierarchy_level4_wid,
    null as cost_center_hierarchy_level4_id,
    null as cost_center_hierarchy_level4_name,
    null as cost_center_hierarchy_level5_wid,
    null as cost_center_hierarchy_level5_id,
    null as cost_center_hierarchy_level5_name,
    null as cost_center_hierarchy_level6_wid,
    null as cost_center_hierarchy_level6_id,
    null as cost_center_hierarchy_level6_name
from
    {{ref('cost_center_hierarchy')}} as cost_center_hierarchy
where 
    (cost_center_hierarchy.organization_type_id = 'Cost_Center_Hierarchy' and cost_center_hierarchy.organization_subtype_id = 'Top_Level')
    or (cost_center_hierarchy.organization_type_id = 'Cost_Center_Hierarchy' and 
        cost_center_hierarchy.cost_center_hierarchy_wid = cost_center_hierarchy.cost_center_toplevel_hierarchy_wid
    )
),
level_2_hierarchy as (
    select distinct
        cost_center_hierarchy.cost_center_hierarchy_wid as cost_center_hierarchy_wid,
        cost_center_hierarchy.cost_center_hierarchy_id as cost_center_hierarchy_id,
        cost_center_hierarchy.cost_center_hierarchy_name as cost_center_hierarchy_name,
        cost_center_hierarchy.cost_center_parent_hierarchy_id,
        cost_center_hierarchy.cost_center_parent_hierarchy_wid,
        cost_center_hierarchy.cost_center_toplevel_hierarchy_id,
        cost_center_hierarchy.cost_center_toplevel_hierarchy_wid,
        level_1_hierarchy.cost_center_hierarchy_level1_wid,
        level_1_hierarchy.cost_center_hierarchy_level1_id,
        level_1_hierarchy.cost_center_hierarchy_level1_name,
        cost_center_hierarchy.cost_center_hierarchy_wid as cost_center_hierarchy_level2_wid,
        cost_center_hierarchy.cost_center_hierarchy_id as cost_center_hierarchy_level2_id,
        cost_center_hierarchy.cost_center_hierarchy_name as cost_center_hierarchy_level2_name,
        null as cost_center_hierarchy_level3_wid,
        null as cost_center_hierarchy_level3_id,
        null as cost_center_hierarchy_level3_name,
        null as cost_center_hierarchy_level4_wid,
        null as cost_center_hierarchy_level4_id,
        null as cost_center_hierarchy_level4_name,
        null as cost_center_hierarchy_level5_wid,
        null as cost_center_hierarchy_level5_id,
        null as cost_center_hierarchy_level5_name,
        null as cost_center_hierarchy_level6_wid,
        null as cost_center_hierarchy_level6_id,
        null as cost_center_hierarchy_level6_name
    from
        {{ref('cost_center_hierarchy')}} as cost_center_hierarchy
    inner join
        level_1_hierarchy
            on cost_center_hierarchy.cost_center_parent_hierarchy_wid = level_1_hierarchy.cost_center_hierarchy_level1_wid
),
level_3_hierarchy as (
    select distinct
        cost_center_hierarchy.cost_center_hierarchy_wid as cost_center_hierarchy_wid,
        cost_center_hierarchy.cost_center_hierarchy_id as cost_center_hierarchy_id,
        cost_center_hierarchy.cost_center_hierarchy_name as cost_center_hierarchy_name,
        cost_center_hierarchy.cost_center_parent_hierarchy_id,
        cost_center_hierarchy.cost_center_parent_hierarchy_wid,
        cost_center_hierarchy.cost_center_toplevel_hierarchy_id,
        cost_center_hierarchy.cost_center_toplevel_hierarchy_wid,
        level_2_hierarchy.cost_center_hierarchy_level1_wid,
        level_2_hierarchy.cost_center_hierarchy_level1_id,
        level_2_hierarchy.cost_center_hierarchy_level1_name,
        level_2_hierarchy.cost_center_hierarchy_level2_wid,
        level_2_hierarchy.cost_center_hierarchy_level2_id,
        level_2_hierarchy.cost_center_hierarchy_level2_name,
        cost_center_hierarchy.cost_center_hierarchy_wid as cost_center_hierarchy_level3_wid,
        cost_center_hierarchy.cost_center_hierarchy_id as cost_center_hierarchy_level3_id,
        cost_center_hierarchy.cost_center_hierarchy_name as cost_center_hierarchy_level3_name,
        null as cost_center_hierarchy_level4_wid,
        null as cost_center_hierarchy_level4_id,
        null as cost_center_hierarchy_level4_name,
        null as cost_center_hierarchy_level5_wid,
        null as cost_center_hierarchy_level5_id,
        null as cost_center_hierarchy_level5_name,
        null as cost_center_hierarchy_level6_wid,
        null as cost_center_hierarchy_level6_id,
        null as cost_center_hierarchy_level6_name
    from
        {{ref('cost_center_hierarchy')}} as cost_center_hierarchy
    inner join
        level_2_hierarchy
            on cost_center_hierarchy.cost_center_parent_hierarchy_wid = level_2_hierarchy.cost_center_hierarchy_level2_wid
),
level_4_hierarchy as (
    select distinct
        cost_center_hierarchy.cost_center_hierarchy_wid as cost_center_hierarchy_wid,
        cost_center_hierarchy.cost_center_hierarchy_id as cost_center_hierarchy_id,
        cost_center_hierarchy.cost_center_hierarchy_name as cost_center_hierarchy_name,
        cost_center_hierarchy.cost_center_parent_hierarchy_id,
        cost_center_hierarchy.cost_center_parent_hierarchy_wid,
        cost_center_hierarchy.cost_center_toplevel_hierarchy_id,
        cost_center_hierarchy.cost_center_toplevel_hierarchy_wid,
        level_3_hierarchy.cost_center_hierarchy_level1_wid,
        level_3_hierarchy.cost_center_hierarchy_level1_id,
        level_3_hierarchy.cost_center_hierarchy_level1_name,
        level_3_hierarchy.cost_center_hierarchy_level2_wid,
        level_3_hierarchy.cost_center_hierarchy_level2_id,
        level_3_hierarchy.cost_center_hierarchy_level2_name,
        level_3_hierarchy.cost_center_hierarchy_level3_wid,
        level_3_hierarchy.cost_center_hierarchy_level3_id,
        level_3_hierarchy.cost_center_hierarchy_level3_name,
        cost_center_hierarchy.cost_center_hierarchy_wid as cost_center_hierarchy_level4_wid,
        cost_center_hierarchy.cost_center_hierarchy_id as cost_center_hierarchy_level4_id,
        cost_center_hierarchy.cost_center_hierarchy_name as cost_center_hierarchy_level4_name,
        null as cost_center_hierarchy_level5_wid,
        null as cost_center_hierarchy_level5_id,
        null as cost_center_hierarchy_level5_name,
        null as cost_center_hierarchy_level6_wid,
        null as cost_center_hierarchy_level6_id,
        null as cost_center_hierarchy_level6_name
    from
        {{ref('cost_center_hierarchy')}} as cost_center_hierarchy
    inner join
        level_3_hierarchy
            on cost_center_hierarchy.cost_center_parent_hierarchy_wid = level_3_hierarchy.cost_center_hierarchy_level3_wid
)
,
level_5_hierarchy as (
    select distinct
        cost_center_hierarchy.cost_center_hierarchy_wid as cost_center_hierarchy_wid,
        cost_center_hierarchy.cost_center_hierarchy_id as cost_center_hierarchy_id,
        cost_center_hierarchy.cost_center_hierarchy_name as cost_center_hierarchy_name,
        cost_center_hierarchy.cost_center_parent_hierarchy_id,
        cost_center_hierarchy.cost_center_parent_hierarchy_wid,
        cost_center_hierarchy.cost_center_toplevel_hierarchy_id,
        cost_center_hierarchy.cost_center_toplevel_hierarchy_wid,
        level_4_hierarchy.cost_center_hierarchy_level1_wid,
        level_4_hierarchy.cost_center_hierarchy_level1_id,
        level_4_hierarchy.cost_center_hierarchy_level1_name,
        level_4_hierarchy.cost_center_hierarchy_level2_wid,
        level_4_hierarchy.cost_center_hierarchy_level2_id,
        level_4_hierarchy.cost_center_hierarchy_level2_name,
        level_4_hierarchy.cost_center_hierarchy_level3_wid,
        level_4_hierarchy.cost_center_hierarchy_level3_id,
        level_4_hierarchy.cost_center_hierarchy_level3_name,
        level_4_hierarchy.cost_center_hierarchy_level4_wid,
        level_4_hierarchy.cost_center_hierarchy_level4_id,
        level_4_hierarchy.cost_center_hierarchy_level4_name,
        cost_center_hierarchy.cost_center_hierarchy_wid as cost_center_hierarchy_level5_wid,
        cost_center_hierarchy.cost_center_hierarchy_id as cost_center_hierarchy_level5_id,
        cost_center_hierarchy.cost_center_hierarchy_name as cost_center_hierarchy_level5_name,
        null as cost_center_hierarchy_level6_wid,
        null as cost_center_hierarchy_level6_id,
        null as cost_center_hierarchy_level6_name
    from
        {{ref('cost_center_hierarchy')}} as cost_center_hierarchy
    inner join
        level_4_hierarchy
            on cost_center_hierarchy.cost_center_parent_hierarchy_wid = level_4_hierarchy.cost_center_hierarchy_level4_wid
),
level_6_hierarchy as (
    select distinct
        cost_center_hierarchy.cost_center_hierarchy_wid as cost_center_hierarchy_wid,
        cost_center_hierarchy.cost_center_hierarchy_id as cost_center_hierarchy_id,
        cost_center_hierarchy.cost_center_hierarchy_name as cost_center_hierarchy_name,
        cost_center_hierarchy.cost_center_parent_hierarchy_id,
        cost_center_hierarchy.cost_center_parent_hierarchy_wid,
        cost_center_hierarchy.cost_center_toplevel_hierarchy_id,
        cost_center_hierarchy.cost_center_toplevel_hierarchy_wid,
        level_5_hierarchy.cost_center_hierarchy_level1_wid,
        level_5_hierarchy.cost_center_hierarchy_level1_id,
        level_5_hierarchy.cost_center_hierarchy_level1_name,
        level_5_hierarchy.cost_center_hierarchy_level2_wid,
        level_5_hierarchy.cost_center_hierarchy_level2_id,
        level_5_hierarchy.cost_center_hierarchy_level2_name,
        level_5_hierarchy.cost_center_hierarchy_level3_wid,
        level_5_hierarchy.cost_center_hierarchy_level3_id,
        level_5_hierarchy.cost_center_hierarchy_level3_name,
        level_5_hierarchy.cost_center_hierarchy_level4_wid,
        level_5_hierarchy.cost_center_hierarchy_level4_id,
        level_5_hierarchy.cost_center_hierarchy_level4_name,
        level_5_hierarchy.cost_center_hierarchy_level5_wid,
        level_5_hierarchy.cost_center_hierarchy_level5_id,
        level_5_hierarchy.cost_center_hierarchy_level5_name,
        cost_center_hierarchy.cost_center_hierarchy_wid as cost_center_hierarchy_level6_wid,
        cost_center_hierarchy.cost_center_hierarchy_id as cost_center_hierarchy_level6_id,
        cost_center_hierarchy.cost_center_hierarchy_name as cost_center_hierarchy_level6_name
    from
        {{ref('cost_center_hierarchy')}} as cost_center_hierarchy
    inner join
        level_5_hierarchy on cost_center_hierarchy.cost_center_parent_hierarchy_wid = level_5_hierarchy.cost_center_hierarchy_level5_wid
),
finaloutput as (
select distinct
lvl1.cost_center_hierarchy_wid,
lvl1.cost_center_hierarchy_id,
lvl1.cost_center_hierarchy_name,
lvl1.cost_center_parent_hierarchy_wid,
lvl1.cost_center_parent_hierarchy_id,
lvl1.cost_center_toplevel_hierarchy_wid,
lvl1.cost_center_toplevel_hierarchy_id,
lvl1.cost_center_hierarchy_level1_wid,
lvl1.cost_center_hierarchy_level1_id,
lvl1.cost_center_hierarchy_level1_name,
lvl1.cost_center_hierarchy_level2_wid,
lvl1.cost_center_hierarchy_level2_id,
lvl1.cost_center_hierarchy_level2_name,
lvl1.cost_center_hierarchy_level3_wid,
lvl1.cost_center_hierarchy_level3_id,
lvl1.cost_center_hierarchy_level3_name,
lvl1.cost_center_hierarchy_level4_wid,
lvl1.cost_center_hierarchy_level4_id,
lvl1.cost_center_hierarchy_level4_name,
lvl1.cost_center_hierarchy_level5_wid,
lvl1.cost_center_hierarchy_level5_id,
lvl1.cost_center_hierarchy_level5_name,
lvl1.cost_center_hierarchy_level6_wid,
lvl1.cost_center_hierarchy_level6_id,
lvl1.cost_center_hierarchy_level6_name
from level_1_hierarchy lvl1
union
select lvl2.cost_center_hierarchy_wid,
lvl2.cost_center_hierarchy_id,
lvl2.cost_center_hierarchy_name,
lvl2.cost_center_parent_hierarchy_wid,
lvl2.cost_center_parent_hierarchy_id,
lvl2.cost_center_toplevel_hierarchy_wid,
lvl2.cost_center_toplevel_hierarchy_id,
lvl2.cost_center_hierarchy_level1_wid,
lvl2.cost_center_hierarchy_level1_id,
lvl2.cost_center_hierarchy_level1_name,
lvl2.cost_center_hierarchy_level2_wid,
lvl2.cost_center_hierarchy_level2_id,
lvl2.cost_center_hierarchy_level2_name,
lvl2.cost_center_hierarchy_level3_wid,
lvl2.cost_center_hierarchy_level3_id,
lvl2.cost_center_hierarchy_level3_name,
lvl2.cost_center_hierarchy_level4_wid,
lvl2.cost_center_hierarchy_level4_id,
lvl2.cost_center_hierarchy_level4_name,
lvl2.cost_center_hierarchy_level5_wid,
lvl2.cost_center_hierarchy_level5_id,
lvl2.cost_center_hierarchy_level5_name,
lvl2.cost_center_hierarchy_level6_wid,
lvl2.cost_center_hierarchy_level6_id,
lvl2.cost_center_hierarchy_level6_name
from level_2_hierarchy lvl2
union
select lvl3.cost_center_hierarchy_wid,
lvl3.cost_center_hierarchy_id,
lvl3.cost_center_hierarchy_name,
lvl3.cost_center_parent_hierarchy_wid,
lvl3.cost_center_parent_hierarchy_id,
lvl3.cost_center_toplevel_hierarchy_wid,
lvl3.cost_center_toplevel_hierarchy_id,
lvl3.cost_center_hierarchy_level1_wid,
lvl3.cost_center_hierarchy_level1_id,
lvl3.cost_center_hierarchy_level1_name,
lvl3.cost_center_hierarchy_level2_wid,
lvl3.cost_center_hierarchy_level2_id,
lvl3.cost_center_hierarchy_level2_name,
lvl3.cost_center_hierarchy_level3_wid,
lvl3.cost_center_hierarchy_level3_id,
lvl3.cost_center_hierarchy_level3_name,
lvl3.cost_center_hierarchy_level4_wid,
lvl3.cost_center_hierarchy_level4_id,
lvl3.cost_center_hierarchy_level4_name,
lvl3.cost_center_hierarchy_level5_wid,
lvl3.cost_center_hierarchy_level5_id,
lvl3.cost_center_hierarchy_level5_name,
lvl3.cost_center_hierarchy_level6_wid,
lvl3.cost_center_hierarchy_level6_id,
lvl3.cost_center_hierarchy_level6_name
from level_3_hierarchy lvl3
union
select lvl4.cost_center_hierarchy_wid,
lvl4.cost_center_hierarchy_id,
lvl4.cost_center_hierarchy_name,
lvl4.cost_center_parent_hierarchy_wid,
lvl4.cost_center_parent_hierarchy_id,
lvl4.cost_center_toplevel_hierarchy_wid,
lvl4.cost_center_toplevel_hierarchy_id,
lvl4.cost_center_hierarchy_level1_wid,
lvl4.cost_center_hierarchy_level1_id,
lvl4.cost_center_hierarchy_level1_name,
lvl4.cost_center_hierarchy_level2_wid,
lvl4.cost_center_hierarchy_level2_id,
lvl4.cost_center_hierarchy_level2_name,
lvl4.cost_center_hierarchy_level3_wid,
lvl4.cost_center_hierarchy_level3_id,
lvl4.cost_center_hierarchy_level3_name,
lvl4.cost_center_hierarchy_level4_wid,
lvl4.cost_center_hierarchy_level4_id,
lvl4.cost_center_hierarchy_level4_name,
lvl4.cost_center_hierarchy_level5_wid,
lvl4.cost_center_hierarchy_level5_id,
lvl4.cost_center_hierarchy_level5_name,
lvl4.cost_center_hierarchy_level6_wid,
lvl4.cost_center_hierarchy_level6_id,
lvl4.cost_center_hierarchy_level6_name
from level_4_hierarchy lvl4
union
select lvl5.cost_center_hierarchy_wid,
lvl5.cost_center_hierarchy_id,
lvl5.cost_center_hierarchy_name,
lvl5.cost_center_parent_hierarchy_wid,
lvl5.cost_center_parent_hierarchy_id,
lvl5.cost_center_toplevel_hierarchy_wid,
lvl5.cost_center_toplevel_hierarchy_id,
lvl5.cost_center_hierarchy_level1_wid,
lvl5.cost_center_hierarchy_level1_id,
lvl5.cost_center_hierarchy_level1_name,
lvl5.cost_center_hierarchy_level2_wid,
lvl5.cost_center_hierarchy_level2_id,
lvl5.cost_center_hierarchy_level2_name,
lvl5.cost_center_hierarchy_level3_wid,
lvl5.cost_center_hierarchy_level3_id,
lvl5.cost_center_hierarchy_level3_name,
lvl5.cost_center_hierarchy_level4_wid,
lvl5.cost_center_hierarchy_level4_id,
lvl5.cost_center_hierarchy_level4_name,
lvl5.cost_center_hierarchy_level5_wid,
lvl5.cost_center_hierarchy_level5_id,
lvl5.cost_center_hierarchy_level5_name,
lvl5.cost_center_hierarchy_level6_wid,
lvl5.cost_center_hierarchy_level6_id,
lvl5.cost_center_hierarchy_level6_name
from level_5_hierarchy lvl5
union
select lvl6.cost_center_hierarchy_wid,
lvl6.cost_center_hierarchy_id,
lvl6.cost_center_hierarchy_name,
lvl6.cost_center_parent_hierarchy_wid,
lvl6.cost_center_parent_hierarchy_id,
lvl6.cost_center_toplevel_hierarchy_wid,
lvl6.cost_center_toplevel_hierarchy_id,
lvl6.cost_center_hierarchy_level1_wid,
lvl6.cost_center_hierarchy_level1_id,
lvl6.cost_center_hierarchy_level1_name,
lvl6.cost_center_hierarchy_level2_wid,
lvl6.cost_center_hierarchy_level2_id,
lvl6.cost_center_hierarchy_level2_name,
lvl6.cost_center_hierarchy_level3_wid,
lvl6.cost_center_hierarchy_level3_id,
lvl6.cost_center_hierarchy_level3_name,
lvl6.cost_center_hierarchy_level4_wid,
lvl6.cost_center_hierarchy_level4_id,
lvl6.cost_center_hierarchy_level4_name,
lvl6.cost_center_hierarchy_level5_wid,
lvl6.cost_center_hierarchy_level5_id,
lvl6.cost_center_hierarchy_level5_name,
lvl6.cost_center_hierarchy_level6_wid,
lvl6.cost_center_hierarchy_level6_id,
lvl6.cost_center_hierarchy_level6_name
from level_6_hierarchy lvl6
)
select distinct
    finaloutput.cost_center_hierarchy_wid,
    finaloutput.cost_center_hierarchy_id,
    finaloutput.cost_center_hierarchy_name,
    finaloutput.cost_center_parent_hierarchy_wid,
    finaloutput.cost_center_parent_hierarchy_id,
    finaloutput.cost_center_toplevel_hierarchy_wid,
    finaloutput.cost_center_toplevel_hierarchy_id,
    finaloutput.cost_center_hierarchy_level1_wid,
    finaloutput.cost_center_hierarchy_level1_id,
    finaloutput.cost_center_hierarchy_level1_name,
    finaloutput.cost_center_hierarchy_level2_wid,
    finaloutput.cost_center_hierarchy_level2_id,
    finaloutput.cost_center_hierarchy_level2_name,
    finaloutput.cost_center_hierarchy_level3_wid,
    finaloutput.cost_center_hierarchy_level3_id,
    finaloutput.cost_center_hierarchy_level3_name,
    finaloutput.cost_center_hierarchy_level4_wid,
    finaloutput.cost_center_hierarchy_level4_id,
    finaloutput.cost_center_hierarchy_level4_name,
    finaloutput.cost_center_hierarchy_level5_wid,
    finaloutput.cost_center_hierarchy_level5_id,
    finaloutput.cost_center_hierarchy_level5_name,
    finaloutput.cost_center_hierarchy_level6_wid,
    finaloutput.cost_center_hierarchy_level6_id,
    finaloutput.cost_center_hierarchy_level6_name,
    coalesce(finaloutput.cost_center_hierarchy_level2_wid,finaloutput.cost_center_hierarchy_level1_wid) as cost_center_hierarchy_level2_alternate_wid,
    coalesce(finaloutput.cost_center_hierarchy_level2_id,finaloutput.cost_center_hierarchy_level1_id) as cost_center_hierarchy_level2_alternate_id,
    coalesce(finaloutput.cost_center_hierarchy_level2_name,finaloutput.cost_center_hierarchy_level1_name) as cost_center_hierarchy_level2_alternate_name,
    coalesce(finaloutput.cost_center_hierarchy_level3_wid,finaloutput.cost_center_hierarchy_level2_wid,finaloutput.cost_center_hierarchy_level1_wid) as cost_center_hierarchy_level3_alternate_wid,
    coalesce(finaloutput.cost_center_hierarchy_level3_id,finaloutput.cost_center_hierarchy_level2_id,finaloutput.cost_center_hierarchy_level1_id) as cost_center_hierarchy_level3_alternate_id,
    coalesce(finaloutput.cost_center_hierarchy_level3_name,finaloutput.cost_center_hierarchy_level2_name,finaloutput.cost_center_hierarchy_level1_name) as cost_center_hierarchy_level3_alternate_name,
    coalesce(finaloutput.cost_center_hierarchy_level4_wid,finaloutput.cost_center_hierarchy_level3_wid,finaloutput.cost_center_hierarchy_level2_wid,finaloutput.cost_center_hierarchy_level1_wid) as cost_center_hierarchy_level4_alternate_wid,
    coalesce(finaloutput.cost_center_hierarchy_level4_id,finaloutput.cost_center_hierarchy_level3_id,finaloutput.cost_center_hierarchy_level2_id,finaloutput.cost_center_hierarchy_level1_id) as cost_center_hierarchy_level4_alternate_id,
    coalesce(finaloutput.cost_center_hierarchy_level4_name,finaloutput.cost_center_hierarchy_level3_name,finaloutput.cost_center_hierarchy_level2_name,finaloutput.cost_center_hierarchy_level1_name) as cost_center_hierarchy_level4_alternate_name,
    coalesce(finaloutput.cost_center_hierarchy_level5_wid,finaloutput.cost_center_hierarchy_level4_wid,finaloutput.cost_center_hierarchy_level3_wid,finaloutput.cost_center_hierarchy_level2_wid,finaloutput.cost_center_hierarchy_level1_wid) as cost_center_hierarchy_level5_alternate_wid,
    coalesce(finaloutput.cost_center_hierarchy_level5_id,finaloutput.cost_center_hierarchy_level4_id,finaloutput.cost_center_hierarchy_level3_id,finaloutput.cost_center_hierarchy_level2_id,finaloutput.cost_center_hierarchy_level1_id) as cost_center_hierarchy_level5_alternate_id,
    coalesce(finaloutput.cost_center_hierarchy_level5_name,finaloutput.cost_center_hierarchy_level4_name,finaloutput.cost_center_hierarchy_level3_name,finaloutput.cost_center_hierarchy_level2_name,finaloutput.cost_center_hierarchy_level1_name) as cost_center_hierarchy_level5_alternate_name,
    coalesce(finaloutput.cost_center_hierarchy_level6_wid,finaloutput.cost_center_hierarchy_level5_wid,finaloutput.cost_center_hierarchy_level4_wid,finaloutput.cost_center_hierarchy_level3_wid,finaloutput.cost_center_hierarchy_level2_wid,finaloutput.cost_center_hierarchy_level1_wid) as cost_center_hierarchy_level6_alternate_wid,
    coalesce(finaloutput.cost_center_hierarchy_level6_id,finaloutput.cost_center_hierarchy_level5_id,finaloutput.cost_center_hierarchy_level4_id,finaloutput.cost_center_hierarchy_level3_id,finaloutput.cost_center_hierarchy_level2_id,finaloutput.cost_center_hierarchy_level1_id) as cost_center_hierarchy_level6_alternate_id,
    coalesce(finaloutput.cost_center_hierarchy_level6_name,finaloutput.cost_center_hierarchy_level5_name,finaloutput.cost_center_hierarchy_level4_name,finaloutput.cost_center_hierarchy_level3_name,finaloutput.cost_center_hierarchy_level2_name,finaloutput.cost_center_hierarchy_level1_name) as cost_center_hierarchy_level6_alternate_name,
    cast({{
        dbt_utils.surrogate_key([
            'cost_center_hierarchy_wid',
            'cost_center_hierarchy_id',
            'cost_center_hierarchy_name',
            'cost_center_parent_hierarchy_wid',
            'cost_center_parent_hierarchy_id',
            'cost_center_toplevel_hierarchy_wid',
            'cost_center_toplevel_hierarchy_id',
            'cost_center_hierarchy_level1_wid',
            'cost_center_hierarchy_level1_id',
            'cost_center_hierarchy_level1_name',
            'cost_center_hierarchy_level2_wid',
            'cost_center_hierarchy_level2_id',
            'cost_center_hierarchy_level2_name',
            'cost_center_hierarchy_level3_wid',
            'cost_center_hierarchy_level3_id',
            'cost_center_hierarchy_level3_name',
            'cost_center_hierarchy_level4_wid',
            'cost_center_hierarchy_level4_id',
            'cost_center_hierarchy_level4_name',
            'cost_center_hierarchy_level5_wid',
            'cost_center_hierarchy_level5_id',
            'cost_center_hierarchy_level5_name',
            'cost_center_hierarchy_level6_wid',
            'cost_center_hierarchy_level6_id',
            'cost_center_hierarchy_level6_name',
            'cost_center_hierarchy_level2_alternate_wid',
            'cost_center_hierarchy_level2_alternate_id',
            'cost_center_hierarchy_level2_alternate_name',
            'cost_center_hierarchy_level3_alternate_wid',
            'cost_center_hierarchy_level3_alternate_id',
            'cost_center_hierarchy_level3_alternate_name',
            'cost_center_hierarchy_level4_alternate_wid',
            'cost_center_hierarchy_level4_alternate_id',
            'cost_center_hierarchy_level4_alternate_name',
            'cost_center_hierarchy_level5_alternate_wid',
            'cost_center_hierarchy_level5_alternate_id',
            'cost_center_hierarchy_level5_alternate_name',
            'cost_center_hierarchy_level6_alternate_wid',
            'cost_center_hierarchy_level6_alternate_id',
            'cost_center_hierarchy_level6_alternate_name'
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
