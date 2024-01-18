 with level_1_hierarchy as (
    select distinct
    company_hierarchy.company_hierarchy_wid as company_hierarchy_wid,
    company_hierarchy.company_hierarchy_id as company_hierarchy_id,
    company_hierarchy.company_hierarchy_name as company_hierarchy_name,
    company_hierarchy.organization_type_id,
    company_hierarchy.organization_subtype_id,
    company_hierarchy.company_parent_hierarchy_wid,
    company_hierarchy.company_parent_hierarchy_id,
    company_hierarchy.company_toplevel_hierarchy_wid,
    company_hierarchy.company_toplevel_hierarchy_id,
    company_hierarchy.company_hierarchy_wid as company_hierarchy_level1_wid,
    company_hierarchy.company_hierarchy_id as company_hierarchy_level1_id,
    company_hierarchy.company_hierarchy_name as company_hierarchy_level1_name,
    null as company_hierarchy_level2_wid,
    null as company_hierarchy_level2_id,
    null as company_hierarchy_level2_name,
    null as company_hierarchy_level3_wid,
    null as company_hierarchy_level3_id,
    null as company_hierarchy_level3_name
    from
        {{ref('company_hierarchy')}} as company_hierarchy
    where
        (company_hierarchy.organization_type_id = 'Company_Hierarchy' and company_hierarchy.organization_subtype_id = 'Top_Level')
        or
        (company_hierarchy.organization_type_id = 'Company_Hierarchy'
            and company_hierarchy.company_hierarchy_wid = company_hierarchy.company_toplevel_hierarchy_wid
        )
),
level_2_hierarchy as (
    select distinct
        company_hierarchy.company_hierarchy_wid as company_hierarchy_wid,
        company_hierarchy.company_hierarchy_id as company_hierarchy_id,
        company_hierarchy.company_hierarchy_name as company_hierarchy_name,
        company_hierarchy.company_parent_hierarchy_id,
        company_hierarchy.company_parent_hierarchy_wid,
        company_hierarchy.company_toplevel_hierarchy_id,
        company_hierarchy.company_toplevel_hierarchy_wid,
        level_1_hierarchy.company_hierarchy_level1_wid,
        level_1_hierarchy.company_hierarchy_level1_id,
        level_1_hierarchy.company_hierarchy_level1_name,
        company_hierarchy.company_hierarchy_wid as company_hierarchy_level2_wid,
        company_hierarchy.company_hierarchy_id as company_hierarchy_level2_id,
        company_hierarchy.company_hierarchy_name as company_hierarchy_level2_name,
        null as company_hierarchy_level3_wid,
        null as company_hierarchy_level3_id,
        null as company_hierarchy_level3_name
    from
        {{ref('company_hierarchy')}} as company_hierarchy
    inner join
        level_1_hierarchy
            on company_hierarchy.company_parent_hierarchy_wid = level_1_hierarchy.company_hierarchy_level1_wid
),
level_3_hierarchy as (
    select distinct
        company_hierarchy.company_hierarchy_wid as company_hierarchy_wid,
        company_hierarchy.company_hierarchy_id as company_hierarchy_id,
        company_hierarchy.company_hierarchy_name as company_hierarchy_name,
        company_hierarchy.company_parent_hierarchy_id,
        company_hierarchy.company_parent_hierarchy_wid,
        company_hierarchy.company_toplevel_hierarchy_id,
        company_hierarchy.company_toplevel_hierarchy_wid,
        level_2_hierarchy.company_hierarchy_level1_wid,
        level_2_hierarchy.company_hierarchy_level1_id,
        level_2_hierarchy.company_hierarchy_level1_name,
        level_2_hierarchy.company_hierarchy_level2_wid,
        level_2_hierarchy.company_hierarchy_level2_id,
        level_2_hierarchy.company_hierarchy_level2_name,
        company_hierarchy.company_hierarchy_wid as company_hierarchy_level3_wid,
        company_hierarchy.company_hierarchy_id as company_hierarchy_level3_id,
        company_hierarchy.company_hierarchy_name as company_hierarchy_level3_name
    from
        {{ref('company_hierarchy')}} as company_hierarchy
    inner join
        level_2_hierarchy on company_hierarchy.company_parent_hierarchy_wid = level_2_hierarchy.company_hierarchy_level2_wid
),
finaloutput as (
    select
        lvl1.company_hierarchy_wid, lvl1.company_hierarchy_id, lvl1.company_hierarchy_name
        , lvl1.company_parent_hierarchy_wid, lvl1.company_parent_hierarchy_id
        , lvl1.company_toplevel_hierarchy_wid, lvl1.company_toplevel_hierarchy_id
        , lvl1.company_hierarchy_level1_wid, lvl1.company_hierarchy_level1_id, lvl1.company_hierarchy_level1_name
        , lvl1.company_hierarchy_level2_wid, lvl1.company_hierarchy_level2_id, lvl1.company_hierarchy_level2_name
        , lvl1.company_hierarchy_level3_wid, lvl1.company_hierarchy_level3_id, lvl1.company_hierarchy_level3_name
    from level_1_hierarchy lvl1
    union
    select lvl2.company_hierarchy_wid, lvl2.company_hierarchy_id, lvl2.company_hierarchy_name
        , lvl2.company_parent_hierarchy_wid, lvl2.company_parent_hierarchy_id
        , lvl2.company_toplevel_hierarchy_wid, lvl2.company_toplevel_hierarchy_id
        , lvl2.company_hierarchy_level1_wid, lvl2.company_hierarchy_level1_id, lvl2.company_hierarchy_level1_name
        , lvl2.company_hierarchy_level2_wid, lvl2.company_hierarchy_level2_id, lvl2.company_hierarchy_level2_name
        , lvl2.company_hierarchy_level3_wid, lvl2.company_hierarchy_level3_id, lvl2.company_hierarchy_level3_name
    from level_2_hierarchy lvl2
    union
    select lvl3.company_hierarchy_wid, lvl3.company_hierarchy_id, lvl3.company_hierarchy_name
        , lvl3.company_parent_hierarchy_wid, lvl3.company_parent_hierarchy_id
        , lvl3.company_toplevel_hierarchy_wid, lvl3.company_toplevel_hierarchy_id
        , lvl3.company_hierarchy_level1_wid, lvl3.company_hierarchy_level1_id, lvl3.company_hierarchy_level1_name
        , lvl3.company_hierarchy_level2_wid, lvl3.company_hierarchy_level2_id, lvl3.company_hierarchy_level2_name
        , lvl3.company_hierarchy_level3_wid, lvl3.company_hierarchy_level3_id, lvl3.company_hierarchy_level3_name
    from level_3_hierarchy lvl3
)
select distinct
    finaloutput.company_hierarchy_wid,
    finaloutput.company_hierarchy_id,
    finaloutput.company_hierarchy_name,
    finaloutput.company_parent_hierarchy_wid,
    finaloutput.company_parent_hierarchy_id,
    finaloutput.company_toplevel_hierarchy_wid,
    finaloutput.company_toplevel_hierarchy_id,
    finaloutput.company_hierarchy_level1_wid, 
    finaloutput.company_hierarchy_level1_id,
    finaloutput.company_hierarchy_level1_name,
    finaloutput.company_hierarchy_level2_wid,
    finaloutput.company_hierarchy_level2_id,
    finaloutput.company_hierarchy_level2_name,
    finaloutput.company_hierarchy_level3_wid,
    finaloutput.company_hierarchy_level3_id,
    finaloutput.company_hierarchy_level3_name,
    cast({{
        dbt_utils.surrogate_key([
            'company_hierarchy_wid',
            'company_hierarchy_id', 
            'company_hierarchy_name',
            'company_parent_hierarchy_wid', 
            'company_parent_hierarchy_id',
            'company_toplevel_hierarchy_wid',
            'company_toplevel_hierarchy_id',
            'company_hierarchy_level1_wid', 
            'company_hierarchy_level1_id', 
            'company_hierarchy_level1_name', 
            'company_hierarchy_level2_wid', 
            'company_hierarchy_level2_id', 
            'company_hierarchy_level2_name', 
            'company_hierarchy_level3_wid',
            'company_hierarchy_level3_id', 
            'company_hierarchy_level3_name'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from finaloutput
where
    1 = 1
