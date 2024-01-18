{{ config(
    materialized = 'incremental',
    unique_key = 'project_hierarchy_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['project_hierarchy_wid','project_hierarchy_id' ,'project_hierarchy_name','include_project_hierarchy_id_in_name_ind','enable_as_optional_hierarchy_ind','inactive_ind','description','document_status_id','md5', 'upd_dt', 'upd_by']
) }}
select distinct
    project_hierarchy_reference_wid as project_hierarchy_wid,
    project_hierarchy_reference_project_hierarchy_id as project_hierarchy_id,
    project_hierarchy_data_project_hierarchy_name as project_hierarchy_name,
    coalesce (cast(project_hierarchy_data_include_project_hierarchy_id_in_name  as int),-2) as include_project_hierarchy_id_in_name_ind,
    coalesce (cast(project_hierarchy_data_enable_as_optional_hierarchy as int), -2) as enable_as_optional_hierarchy_ind,
    coalesce (cast(project_hierarchy_data_inactive as int ), -2) as inactive_ind,
    project_hierarchy_data_description as description,
    project_hierarchy_status_reference_document_status_id as document_status_id,
    cast({{
        dbt_utils.surrogate_key([
            'project_hierarchy_wid',
            'project_hierarchy_id',
            'project_hierarchy_name',
            'include_project_hierarchy_id_in_name_ind',
            'enable_as_optional_hierarchy_ind',
            'inactive_ind',
            'description',
            'document_status_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WOKRDAY' as upd_by
from
    {{source('workday_ods', 'get_project_hierarchies')}} as get_project_hierarchies
where
    1 = 1
    {%- if is_incremental() %}
    and md5 not in (
        select md5
        from
            {{ this }}
        where
            project_hierarchy_wid = get_project_hierarchies.project_hierarchy_reference_wid
        ) 
    {%- endif %}
