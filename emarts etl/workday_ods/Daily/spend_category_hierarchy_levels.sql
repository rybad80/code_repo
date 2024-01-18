with level_1_hierarchy as (
    select distinct
        sch.spend_category_hierarchy_wid,
        sch.spend_category_hierarchy_id,
        sch.spend_category_hierarchy_name,
        sch.spend_category_hierarchy_level_id,
        sch.parent_spend_category_hierarchy_wid,
        sch.parent_spend_category_hierarchy_id,
        sch.spend_category_hierarchy_wid as spend_category_hierarchy_level1_wid,
        sch.spend_category_hierarchy_id as spend_category_hierarchy_level1_id,
        sch.spend_category_hierarchy_name as spend_category_hierarchy_level1_name,
        null as spend_category_hierarchy_level2_wid,
        null as spend_category_hierarchy_level2_id,
        null as spend_category_hierarchy_level2_name,
        null as spend_category_hierarchy_level3_wid,
        null as spend_category_hierarchy_level3_id,
        null as spend_category_hierarchy_level3_name,
        null as spend_category_hierarchy_level4_wid,
        null as spend_category_hierarchy_level4_id,
        null as spend_category_hierarchy_level4_name
    from
        {{ref('spend_category_hierarchy')}} as sch
    where 
        sch.spend_category_hierarchy_level_id = 'SCH_Top_Level' or sch.parent_spend_category_hierarchy_wid is null
),
Level_2_Hierarchy as (
    select distinct
        sch.spend_category_hierarchy_wid,
        sch.spend_category_hierarchy_id,
        sch.spend_category_hierarchy_name,
        sch.spend_category_hierarchy_level_id,
        sch.parent_spend_category_hierarchy_wid,
        sch.parent_spend_category_hierarchy_id,
        level_1_hierarchy.spend_category_hierarchy_level1_wid,
        level_1_hierarchy.spend_category_hierarchy_level1_id,
        level_1_hierarchy.spend_category_hierarchy_level1_name,
        sch.spend_category_hierarchy_wid as spend_category_hierarchy_level2_wid,
        sch.spend_category_hierarchy_id as spend_category_hierarchy_level2_id,
        sch.spend_category_hierarchy_name as spend_category_hierarchy_level2_name,
        null as spend_category_hierarchy_level3_wid,
        null as spend_category_hierarchy_level3_id,
        null as spend_category_hierarchy_level3_name,
        null as spend_category_hierarchy_level4_wid,
        null as spend_category_hierarchy_level4_id,
        null as spend_category_hierarchy_level4_name
    from
        {{ref('spend_category_hierarchy')}} as sch
    inner join
        level_1_hierarchy
            on sch.parent_spend_category_hierarchy_wid = level_1_hierarchy.spend_category_hierarchy_wid
),
Level_3_Hierarchy as (
    select distinct
        sch.spend_category_hierarchy_wid,
        sch.spend_category_hierarchy_id,
        sch.spend_category_hierarchy_name,
        sch.spend_category_hierarchy_level_id,
        sch.parent_spend_category_hierarchy_wid,
        sch.parent_spend_category_hierarchy_id,
        level_2_hierarchy.spend_category_hierarchy_level1_wid,
        level_2_hierarchy.spend_category_hierarchy_level1_id,
        level_2_hierarchy.spend_category_hierarchy_level1_name,
        level_2_hierarchy.spend_category_hierarchy_level2_wid,
        level_2_hierarchy.spend_category_hierarchy_level2_id,
        level_2_hierarchy.spend_category_hierarchy_level2_name,
        sch.spend_category_hierarchy_wid as spend_category_hierarchy_level3_wid,
        sch.spend_category_hierarchy_id as spend_category_hierarchy_level3_id,
        sch.spend_category_hierarchy_name as spend_category_hierarchy_level3_name,
        null as spend_category_hierarchy_level4_wid,
        null as spend_category_hierarchy_level4_id,
        null as spend_category_hierarchy_level4_name
    from
        {{ref('spend_category_hierarchy')}} as sch
    inner join
        level_2_hierarchy
            on sch.parent_spend_category_hierarchy_wid = level_2_hierarchy.spend_category_hierarchy_wid
),
Level_4_Hierarchy as (
    select distinct
        sch.spend_category_hierarchy_wid,
        sch.spend_category_hierarchy_id,
        sch.spend_category_hierarchy_name,
        sch.spend_category_hierarchy_level_id,
        sch.parent_spend_category_hierarchy_wid,
        sch.parent_spend_category_hierarchy_id,
        level_3_hierarchy.spend_category_hierarchy_level1_wid,
        level_3_hierarchy.spend_category_hierarchy_level1_id,
        level_3_hierarchy.spend_category_hierarchy_level1_name,
        level_3_hierarchy.spend_category_hierarchy_level2_wid,
        level_3_hierarchy.spend_category_hierarchy_level2_id,
        level_3_hierarchy.spend_category_hierarchy_level2_name,
        level_3_hierarchy.spend_category_hierarchy_level3_wid,
        level_3_hierarchy.spend_category_hierarchy_level3_id,
        level_3_hierarchy.spend_category_hierarchy_level3_name,
        sch.spend_category_hierarchy_wid as spend_category_hierarchy_level4_wid,
        sch.spend_category_hierarchy_id as spend_category_hierarchy_level4_id,
        sch.spend_category_hierarchy_name as spend_category_hierarchy_level4_name
    from
        {{ref('spend_category_hierarchy')}} as sch
    inner join
        level_3_hierarchy
            on sch.parent_spend_category_hierarchy_wid = level_3_hierarchy.spend_category_hierarchy_wid
),
finaloutput as (
    select distinct
        lvl1.spend_category_hierarchy_wid,
        lvl1.spend_category_hierarchy_id,
        lvl1.spend_category_hierarchy_name,
        lvl1.spend_category_hierarchy_level_id,
        lvl1.parent_spend_category_hierarchy_wid,
        lvl1.parent_spend_category_hierarchy_id,
        lvl1.spend_category_hierarchy_level1_wid,
        lvl1.spend_category_hierarchy_level1_id,
        lvl1.spend_category_hierarchy_level1_name,
        lvl1.spend_category_hierarchy_level2_wid,
        lvl1.spend_category_hierarchy_level2_id,
        lvl1.spend_category_hierarchy_level2_name,
        lvl1.spend_category_hierarchy_level3_wid,
        lvl1.spend_category_hierarchy_level3_id,
        lvl1.spend_category_hierarchy_level3_name,
        lvl1.spend_category_hierarchy_level4_wid,
        lvl1.spend_category_hierarchy_level4_id,
        lvl1.spend_category_hierarchy_level4_name
    from
        level_1_hierarchy lvl1
    union
    select
        lvl2.spend_category_hierarchy_wid,
        lvl2.spend_category_hierarchy_id,
        lvl2.spend_category_hierarchy_name,
        lvl2.spend_category_hierarchy_level_id,
        lvl2.parent_spend_category_hierarchy_wid,
        lvl2.parent_spend_category_hierarchy_id,
        lvl2.spend_category_hierarchy_level1_wid,
        lvl2.spend_category_hierarchy_level1_id,
        lvl2.spend_category_hierarchy_level1_name,
        lvl2.spend_category_hierarchy_level2_wid,
        lvl2.spend_category_hierarchy_level2_id,
        lvl2.spend_category_hierarchy_level2_name,
        lvl2.spend_category_hierarchy_level3_wid,
        lvl2.spend_category_hierarchy_level3_id,
        lvl2.spend_category_hierarchy_level3_name,
        lvl2.spend_category_hierarchy_level4_wid,
        lvl2.spend_category_hierarchy_level4_id,
        lvl2.spend_category_hierarchy_level4_name
    from
        level_2_hierarchy lvl2
    union
    select
        lvl3.spend_category_hierarchy_wid,
        lvl3.spend_category_hierarchy_id,
        lvl3.spend_category_hierarchy_name,
        lvl3.spend_category_hierarchy_level_id,
        lvl3.parent_spend_category_hierarchy_wid,
        lvl3.parent_spend_category_hierarchy_id,
        lvl3.spend_category_hierarchy_level1_wid,
        lvl3.spend_category_hierarchy_level1_id,
        lvl3.spend_category_hierarchy_level1_name,
        lvl3.spend_category_hierarchy_level2_wid,
        lvl3.spend_category_hierarchy_level2_id,
        lvl3.spend_category_hierarchy_level2_name,
        lvl3.spend_category_hierarchy_level3_wid,
        lvl3.spend_category_hierarchy_level3_id,
        lvl3.spend_category_hierarchy_level3_name,
        lvl3.spend_category_hierarchy_level4_wid,
        lvl3.spend_category_hierarchy_level4_id,
        lvl3.spend_category_hierarchy_level4_name
    from
        level_3_hierarchy lvl3
    union
    select
        lvl4.spend_category_hierarchy_wid,
        lvl4.spend_category_hierarchy_id,
        lvl4.spend_category_hierarchy_name,
        lvl4.spend_category_hierarchy_level_id,
        lvl4.parent_spend_category_hierarchy_wid,
        lvl4.parent_spend_category_hierarchy_id,
        lvl4.spend_category_hierarchy_level1_wid,
        lvl4.spend_category_hierarchy_level1_id,
        lvl4.spend_category_hierarchy_level1_name,
        lvl4.spend_category_hierarchy_level2_wid,
        lvl4.spend_category_hierarchy_level2_id,
        lvl4.spend_category_hierarchy_level2_name,
        lvl4.spend_category_hierarchy_level3_wid,
        lvl4.spend_category_hierarchy_level3_id,
        lvl4.spend_category_hierarchy_level3_name,
        lvl4.spend_category_hierarchy_level4_wid,
        lvl4.spend_category_hierarchy_level4_id,
        lvl4.spend_category_hierarchy_level4_name
    from level_4_hierarchy lvl4
)
select distinct
    spend_category_hierarchy_wid,
    spend_category_hierarchy_id,
    spend_category_hierarchy_name,
    spend_category_hierarchy_level_id,
    parent_spend_category_hierarchy_wid,
    parent_spend_category_hierarchy_id,
    spend_category_hierarchy_level1_wid,
    spend_category_hierarchy_level1_id,
    spend_category_hierarchy_level1_name,
    spend_category_hierarchy_level2_wid,
    spend_category_hierarchy_level2_id,
    spend_category_hierarchy_level2_name,
    spend_category_hierarchy_level3_wid,
    spend_category_hierarchy_level3_id,
    spend_category_hierarchy_level3_name,
    spend_category_hierarchy_level4_wid,
    spend_category_hierarchy_level4_id,
    spend_category_hierarchy_level4_name,
    cast({{
        dbt_utils.surrogate_key([
            'spend_category_hierarchy_wid',
            'spend_category_hierarchy_id',
            'spend_category_hierarchy_name',
            'spend_category_hierarchy_level_id',
            'parent_spend_category_hierarchy_wid',
            'parent_spend_category_hierarchy_id',
            'spend_category_hierarchy_level1_wid',
            'spend_category_hierarchy_level1_id',
            'spend_category_hierarchy_level1_name',
            'spend_category_hierarchy_level2_wid',
            'spend_category_hierarchy_level2_id',
            'spend_category_hierarchy_level2_name',
            'spend_category_hierarchy_level3_wid',
            'spend_category_hierarchy_level3_id',
            'spend_category_hierarchy_level3_name',
            'spend_category_hierarchy_level4_wid',
            'spend_category_hierarchy_level4_id',
            'spend_category_hierarchy_level4_name'
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
