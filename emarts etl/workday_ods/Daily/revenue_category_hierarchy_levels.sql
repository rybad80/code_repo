with level_1_hierarchy as (
    select distinct
        revenue_category_hierarchy.revenue_category_hierarchy_wid,
        revenue_category_hierarchy.revenue_category_hierarchy_id,
        revenue_category_hierarchy.revenue_category_hierarchy_name,
        revenue_category_hierarchy.parent_revenue_category_hierarchy_wid,
        revenue_category_hierarchy.parent_revenue_category_hierarchy_id,
        revenue_category_hierarchy.revenue_category_hierarchy_wid as revenue_category_hierarchy_level1_wid,
        revenue_category_hierarchy.revenue_category_hierarchy_id as revenue_category_hierarchy_level1_id,
        revenue_category_hierarchy.revenue_category_hierarchy_name as revenue_category_hierarchy_level1_name,
        null as revenue_category_hierarchy_level2_wid,
        null as revenue_category_hierarchy_level2_id,
        null as revenue_category_hierarchy_level2_name,
        null as revenue_category_hierarchy_level3_wid,
        null as revenue_category_hierarchy_level3_id,
        null as revenue_category_hierarchy_level3_name
    from
        {{ref('revenue_category_hierarchy')}} as revenue_category_hierarchy
    where
        revenue_category_hierarchy.parent_revenue_category_hierarchy_wid is null
),
level_2_hierarchy as (
    select distinct
        revenue_category_hierarchy.revenue_category_hierarchy_wid,
        revenue_category_hierarchy.revenue_category_hierarchy_id,
        revenue_category_hierarchy.revenue_category_hierarchy_name,
        revenue_category_hierarchy.parent_revenue_category_hierarchy_wid,
        revenue_category_hierarchy.parent_revenue_category_hierarchy_id,
        level_1_hierarchy.revenue_category_hierarchy_level1_wid,
        level_1_hierarchy.revenue_category_hierarchy_level1_id,
        level_1_hierarchy.revenue_category_hierarchy_level1_name,
        revenue_category_hierarchy.revenue_category_hierarchy_wid as revenue_category_hierarchy_level2_wid,
        revenue_category_hierarchy.revenue_category_hierarchy_id as revenue_category_hierarchy_level2_id,
        revenue_category_hierarchy.revenue_category_hierarchy_name as revenue_category_hierarchy_level2_name,
         null as revenue_category_hierarchy_level3_wid,
         null as revenue_category_hierarchy_level3_id,
         null as revenue_category_hierarchy_level3_name
    from
        {{ref('revenue_category_hierarchy')}} as revenue_category_hierarchy
    inner join
        level_1_hierarchy
            on revenue_category_hierarchy.parent_revenue_category_hierarchy_wid = level_1_hierarchy.revenue_category_hierarchy_wid
),
level_3_hierarchy as (
    select distinct
        revenue_category_hierarchy.revenue_category_hierarchy_wid,
        revenue_category_hierarchy.revenue_category_hierarchy_id,
        revenue_category_hierarchy.revenue_category_hierarchy_name,
        revenue_category_hierarchy.parent_revenue_category_hierarchy_wid,
        revenue_category_hierarchy.parent_revenue_category_hierarchy_id,
        level_2_hierarchy.revenue_category_hierarchy_level1_wid,
        level_2_hierarchy.revenue_category_hierarchy_level1_id,
        level_2_hierarchy.revenue_category_hierarchy_level1_name,
        level_2_hierarchy.revenue_category_hierarchy_level2_wid,
        level_2_hierarchy.revenue_category_hierarchy_level2_id,
        level_2_hierarchy.revenue_category_hierarchy_level2_name,
        revenue_category_hierarchy.revenue_category_hierarchy_wid as revenue_category_hierarchy_level3_wid,
        revenue_category_hierarchy.revenue_category_hierarchy_id as revenue_category_hierarchy_level3_id,
        revenue_category_hierarchy.revenue_category_hierarchy_name as revenue_category_hierarchy_level3_name
    from
        {{ref('revenue_category_hierarchy')}} as revenue_category_hierarchy
    inner join
        level_2_hierarchy on revenue_category_hierarchy.parent_revenue_category_hierarchy_wid = level_2_hierarchy.revenue_category_hierarchy_wid
),
finaloutput as (
    select 
    lvl1.revenue_category_hierarchy_wid, lvl1.revenue_category_hierarchy_id, lvl1.revenue_category_hierarchy_name
    ,lvl1.parent_revenue_category_hierarchy_wid, lvl1.parent_revenue_category_hierarchy_id
    ,lvl1.revenue_category_hierarchy_level1_wid, lvl1.revenue_category_hierarchy_level1_id, lvl1.revenue_category_hierarchy_level1_name
    ,lvl1.revenue_category_hierarchy_level2_wid, lvl1.revenue_category_hierarchy_level2_id, lvl1.revenue_category_hierarchy_level2_name
    ,lvl1.revenue_category_hierarchy_level3_wid, lvl1.revenue_category_hierarchy_level3_id, lvl1.revenue_category_hierarchy_level3_name
    from level_1_hierarchy lvl1
    union
    select 
    lvl2.revenue_category_hierarchy_wid, lvl2.revenue_category_hierarchy_id, lvl2.revenue_category_hierarchy_name
    ,lvl2.parent_revenue_category_hierarchy_wid, lvl2.parent_revenue_category_hierarchy_id
    ,lvl2.revenue_category_hierarchy_level1_wid, lvl2.revenue_category_hierarchy_level1_id, lvl2.revenue_category_hierarchy_level1_name
    ,lvl2.revenue_category_hierarchy_level2_wid, lvl2.revenue_category_hierarchy_level2_id, lvl2.revenue_category_hierarchy_level2_name
    ,lvl2.revenue_category_hierarchy_level3_wid, lvl2.revenue_category_hierarchy_level3_id, lvl2.revenue_category_hierarchy_level3_name
    from level_2_hierarchy lvl2
    union
    select 
    lvl3.revenue_category_hierarchy_wid, lvl3.revenue_category_hierarchy_id, lvl3.revenue_category_hierarchy_name
    ,lvl3.parent_revenue_category_hierarchy_wid, lvl3.parent_revenue_category_hierarchy_id
    ,lvl3.revenue_category_hierarchy_level1_wid, lvl3.revenue_category_hierarchy_level1_id, lvl3.revenue_category_hierarchy_level1_name
    ,lvl3.revenue_category_hierarchy_level2_wid, lvl3.revenue_category_hierarchy_level2_id, lvl3.revenue_category_hierarchy_level2_name
    ,lvl3.revenue_category_hierarchy_level3_wid, lvl3.revenue_category_hierarchy_level3_id, lvl3.revenue_category_hierarchy_level3_name
    from level_3_hierarchy lvl3
)
select distinct
    revenue_category_hierarchy_wid,
    revenue_category_hierarchy_id,
    revenue_category_hierarchy_name,
    parent_revenue_category_hierarchy_wid,
    parent_revenue_category_hierarchy_id,
    revenue_category_hierarchy_level1_wid,
    revenue_category_hierarchy_level1_id,
    revenue_category_hierarchy_level1_name,
    revenue_category_hierarchy_level2_wid,
    revenue_category_hierarchy_level2_id,
    revenue_category_hierarchy_level2_name,
    revenue_category_hierarchy_level3_wid,
    revenue_category_hierarchy_level3_id,
    revenue_category_hierarchy_level3_name,
    cast({{
        dbt_utils.surrogate_key([
            'revenue_category_hierarchy_wid',
            'revenue_category_hierarchy_id',
            'revenue_category_hierarchy_name',
            'parent_revenue_category_hierarchy_wid',
            'parent_revenue_category_hierarchy_id',
            'revenue_category_hierarchy_level1_wid',
            'revenue_category_hierarchy_level1_id',
            'revenue_category_hierarchy_level1_name',
            'revenue_category_hierarchy_level2_wid',
            'revenue_category_hierarchy_level2_id',
            'revenue_category_hierarchy_level2_name',
            'revenue_category_hierarchy_level3_wid',
            'revenue_category_hierarchy_level3_id',
            'revenue_category_hierarchy_level3_name'
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
