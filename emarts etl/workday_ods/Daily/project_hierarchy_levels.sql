 with level_1_hierarchy as (
    select distinct
        project_hierarchy_reference_wid as project_hierarchy_wid,
        project_hierarchy_reference_project_hierarchy_id as project_hierarchy_id,
        project_hierarchy_data_project_hierarchy_name as project_hierarchy_name,
        null as project_hierarchy_parent_hierarchy_wid,
        null as project_hierarchy_parent_hierarchy_id,
        project_hierarchy_reference_wid as project_hierarchy_toplevel_hierarchy_wid,
        project_hierarchy_reference_project_hierarchy_id as project_hierarchy_toplevel_hierarchy_id,
        project_hierarchy_reference_wid as project_hierarchy_level1_wid,
        project_hierarchy_reference_project_hierarchy_id as project_hierarchy_level1_id,
        project_hierarchy_data_project_hierarchy_name as project_hierarchy_level1_name,
        null as project_hierarchy_level2_wid,
        null as project_hierarchy_level2_id, 
        null as project_hierarchy_level2_name,
        null as project_hierarchy_level3_wid, 
        null as project_hierarchy_level3_id,
        null as project_hierarchy_level3_name
    from
        {{source('workday_ods', 'get_project_hierarchies')}} as get_project_hierarchies
    where parent_reference_wid is null
        
),
level_2_hierarchy as (
    select distinct
        project_hierarchy_reference_wid as project_hierarchy_wid,
        project_hierarchy_reference_project_hierarchy_id as project_hierarchy_id,
        project_hierarchy_data_project_hierarchy_name as project_hierarchy_name,
        level_1_hierarchy.project_hierarchy_level1_wid as project_hierarchy_parent_hierarchy_wid,
        level_1_hierarchy.project_hierarchy_level1_id as project_hierarchy_parent_hierarchy_id,
        level_1_hierarchy.project_hierarchy_level1_wid as project_hierarchy_toplevel_hierarchy_wid,
        level_1_hierarchy.project_hierarchy_level1_id as project_hierarchy_toplevel_hierarchy_id,
        level_1_hierarchy.project_hierarchy_level1_wid,
        level_1_hierarchy.project_hierarchy_level1_id,
        level_1_hierarchy.project_hierarchy_level1_name,
        project_hierarchy_reference_wid as project_hierarchy_level2_wid,
        project_hierarchy_reference_project_hierarchy_id as project_hierarchy_level2_id,
        project_hierarchy_data_project_hierarchy_name as project_hierarchy_level2_name,
        null as project_hierarchy_level3_wid,
        null as project_hierarchy_level3_id,
        null as project_hierarchy_level3_name
    from {{source('workday_ods', 'get_project_hierarchies')}} as get_project_hierarchies
        inner join level_1_hierarchy
            on level_1_hierarchy.project_hierarchy_level1_wid = get_project_hierarchies.parent_reference_wid
),
level_3_hierarchy as (
    select distinct
        project_hierarchy_reference_wid as project_hierarchy_wid,
        project_hierarchy_reference_project_hierarchy_id as project_hierarchy_id,
        project_hierarchy_data_project_hierarchy_name as project_hierarchy_name,
        level_2_hierarchy.project_hierarchy_level2_wid as project_hierarchy_parent_hierarchy_wid,
        level_2_hierarchy.project_hierarchy_level2_id as project_hierarchy_parent_hierarchy_id,
        level_2_hierarchy.project_hierarchy_level1_wid as project_hierarchy_toplevel_hierarchy_wid,
        level_2_hierarchy.project_hierarchy_level1_id as project_hierarchy_toplevel_hierarchy_id,
        level_2_hierarchy.project_hierarchy_level1_wid,
        level_2_hierarchy.project_hierarchy_level1_id,
        level_2_hierarchy.project_hierarchy_level1_name,
        level_2_hierarchy.project_hierarchy_level2_wid,
        level_2_hierarchy.project_hierarchy_level2_id,
        level_2_hierarchy.project_hierarchy_level2_name,
        project_hierarchy_reference_wid as project_hierarchy_level3_wid,
        project_hierarchy_reference_project_hierarchy_id as project_hierarchy_level3_id,
        project_hierarchy_data_project_hierarchy_name as project_hierarchy_level3_name
    from
        {{source('workday_ods', 'get_project_hierarchies')}} as get_project_hierarchies
        inner join level_2_hierarchy
            on level_2_hierarchy.project_hierarchy_level2_wid = get_project_hierarchies.parent_reference_wid
    ),
finaloutput as (
    select
    project_hierarchy_wid,project_hierarchy_id,project_hierarchy_name,project_hierarchy_parent_hierarchy_wid,project_hierarchy_parent_hierarchy_id,project_hierarchy_toplevel_hierarchy_wid,project_hierarchy_toplevel_hierarchy_id
    ,project_hierarchy_level1_wid, project_hierarchy_level1_id, project_hierarchy_level1_name 
    ,project_hierarchy_level2_wid, project_hierarchy_level2_id, project_hierarchy_level2_name
    ,project_hierarchy_level3_wid, project_hierarchy_level3_id, project_hierarchy_level3_name
    ,null as project_hierarchy_level4_wid, null as project_hierarchy_level4_id, null as project_hierarchy_level4_name
    ,null as project_hierarchy_level5_wid, null as project_hierarchy_level5_id, null as project_hierarchy_level5_name
    ,null as project_hierarchy_level6_wid, null as project_hierarchy_level6_id, null as project_hierarchy_level6_name
    from level_1_hierarchy
    union 
    select 
    project_hierarchy_wid,project_hierarchy_id,project_hierarchy_name,project_hierarchy_parent_hierarchy_wid,project_hierarchy_parent_hierarchy_id,project_hierarchy_toplevel_hierarchy_wid,project_hierarchy_toplevel_hierarchy_id
    ,project_hierarchy_level1_wid, project_hierarchy_level1_id, project_hierarchy_level1_name, 
    project_hierarchy_level2_wid, project_hierarchy_level2_id, project_hierarchy_level2_name, 
    project_hierarchy_level3_wid, project_hierarchy_level3_id, project_hierarchy_level3_name
    ,null as project_hierarchy_level4_wid, null as project_hierarchy_level4_id, null as project_hierarchy_level4_name
    ,null as project_hierarchy_level5_wid, null as project_hierarchy_level5_id, null as project_hierarchy_level5_name
    ,null as project_hierarchy_level6_wid, null as project_hierarchy_level6_id, null as project_hierarchy_level6_name
    from level_2_hierarchy
    union 
    select 
    project_hierarchy_wid,project_hierarchy_id,project_hierarchy_name,project_hierarchy_parent_hierarchy_wid,project_hierarchy_parent_hierarchy_id,project_hierarchy_toplevel_hierarchy_wid,project_hierarchy_toplevel_hierarchy_id
    ,project_hierarchy_level1_wid, project_hierarchy_level1_id, project_hierarchy_level1_name, 
    project_hierarchy_level2_wid, project_hierarchy_level2_id, project_hierarchy_level2_name, 
    project_hierarchy_level3_wid, project_hierarchy_level3_id, project_hierarchy_level3_name
    ,null as project_hierarchy_level4_wid, null as project_hierarchy_level4_id, null as project_hierarchy_level4_name
    ,null as project_hierarchy_level5_wid, null as project_hierarchy_level5_id, null as project_hierarchy_level5_name
    ,null as project_hierarchy_level6_wid, null as project_hierarchy_level6_id, null as project_hierarchy_level6_name
    from level_3_hierarchy
)
select
    finaloutput.*,
    cast({{
        dbt_utils.surrogate_key([
            'project_hierarchy_wid',
            'project_hierarchy_id',
            'project_hierarchy_name',
            'project_hierarchy_parent_hierarchy_wid',
            'project_hierarchy_parent_hierarchy_id',
            'project_hierarchy_toplevel_hierarchy_wid',
            'project_hierarchy_toplevel_hierarchy_id',
            'project_hierarchy_level1_wid',
            'project_hierarchy_level1_id',
            'project_hierarchy_level1_name',
            'project_hierarchy_level2_wid',
            'project_hierarchy_level2_id',
            'project_hierarchy_level2_name',
            'project_hierarchy_level3_wid',
            'project_hierarchy_level3_id',
            'project_hierarchy_level3_name',
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from finaloutput 
where
    1 = 1
