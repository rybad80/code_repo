{{ config(
    materialized = 'incremental',
    unique_key = 'program_hierarchy_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['program_hierarchy_wid','program_hierarchy_id','program_hierarchy_name','include_program_hierarchy_id_in_name_ind', 'program_hierarchy_is_inactive_ind', 'organization_subtype_id', 'parent_program_hierarchy_id', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
    program_hierarchy_reference_wid as program_hierarchy_wid,
    program_hierarchy_reference_program_hierarchy_id as program_hierarchy_id,
    program_hierarchy_data_program_hierarchy_name as program_hierarchy_name,
    coalesce(cast(program_hierarchy_data_include_program_hierarchy_id_in_name as int), -2) as include_program_hierarchy_id_in_name_ind,
    coalesce(cast(program_hierarchy_data_program_hierarchy_is_inactive as int), -2) as program_hierarchy_is_inactive_ind,
    null as organization_subtype_id,
    null as parent_program_hierarchy_id,
    cast({{
        dbt_utils.surrogate_key([
            'program_hierarchy_wid',
            'program_hierarchy_id',
            'program_hierarchy_name',
            'include_program_hierarchy_id_in_name_ind',
            'program_hierarchy_is_inactive_ind',
            'organization_subtype_id',
            'parent_program_hierarchy_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_program_hierarchies')}} as get_program_hierarchies
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                program_hierarchy_wid = get_program_hierarchies.program_hierarchy_reference_wid
        )
    {%- endif %}
