{{ config(
    materialized = 'incremental',
    unique_key = 'grant_hierarchy_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['grant_hierarchy_wid','grant_hierarchy_id','grant_hierarchy_name','include_grant_hierarchy_id_in_name_ind','grant_hierarchy_is_inactive_ind','parent_grant_hierarchy_wid','parent_grant_hierarchy_id', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
    grant_hierarchy_reference_wid as grant_hierarchy_wid,
    grant_hierarchy_reference_grant_hierarchy_id as grant_hierarchy_id,
    grant_hierarchy_data_grant_hierarchy_name as grant_hierarchy_name,
    coalesce(cast(grant_hierarchy_data_include_grant_hierarchy_id_in_name as int), -2) as include_grant_hierarchy_id_in_name_ind,
    coalesce(cast(grant_hierarchy_data_grant_hierarchy_is_inactive as int), -2) as grant_hierarchy_is_inactive_ind,
    superior_grant_hierarchy_reference_wid as parent_grant_hierarchy_wid,
    superior_grant_hierarchy_reference_grant_hierarchy_id as parent_grant_hierarchy_id,
    cast({{
        dbt_utils.surrogate_key([
            'grant_hierarchy_wid',
            'grant_hierarchy_id',
            'grant_hierarchy_name',
            'include_grant_hierarchy_id_in_name_ind',
            'grant_hierarchy_is_inactive_ind',
            'parent_grant_hierarchy_wid',
            'parent_grant_hierarchy_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_grant_hierarchies')}} as get_grant_hierarchies
where
    1 = 1
    and grant_hierarchy_reference_grant_hierarchy_id is not null
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                grant_hierarchy_wid = get_grant_hierarchies.grant_hierarchy_reference_wid
        )
    {%- endif %}
