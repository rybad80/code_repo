with ods_project as (
    select distinct
        project_wid, 
        project_id 
    from 
        {{ ref('project') }} as project
),
stg_project_hier as (
    select distinct
        project_hierarchy_reference_wid as project_hierarchy_wid,
        project_hierarchy_data_project_hierarchy_id as project_hierarchy_id,
        included_projects_reference_project_id as project_id
    from
        {{source('workday_ods', 'get_project_hierarchies')}} as get_project_hierarchies
)
select distinct
    ods_project.project_wid,
    stg_project_hier.project_id,
    stg_project_hier.project_hierarchy_wid,
    stg_project_hier.project_hierarchy_id,
    cast({{
        dbt_utils.surrogate_key([
            'ods_project.project_wid',
            'stg_project_hier.project_hierarchy_wid',
            'stg_project_hier.project_hierarchy_id',
            'stg_project_hier.project_id'
            ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    stg_project_hier
inner join
    ods_project on 
        stg_project_hier.project_id = ods_project.project_id
where
    1 = 1
