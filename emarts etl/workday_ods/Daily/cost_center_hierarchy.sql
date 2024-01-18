{{ 
    config(
        materialized = 'incremental',
        unique_key = 'cost_center_hierarchy_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['cost_center_hierarchy_wid','cost_center_hierarchy_id','cost_center_hierarchy_name','cost_center_hierarchy_code','include_code_in_name_ind','availibility_date', 'last_updated_date','inactive_ind','inactive_date','organization_type_wid', 'organization_type_id', 'organization_subtype_wid','organization_subtype_id', 'cost_center_parent_hierarchy_wid','cost_center_parent_hierarchy_id','cost_center_toplevel_hierarchy_wid', 'cost_center_toplevel_hierarchy_id','md5', 'upd_dt', 'upd_by']
) }}
select distinct 
    organization_organization_reference_wid as cost_center_hierarchy_wid,
    organization_organization_reference_organization_reference_id as cost_center_hierarchy_id,
    organization_organization_data_name as cost_center_hierarchy_name,
    organization_organization_data_organization_code as cost_center_hierarchy_code,
    organization_organization_data_include_organization_code_in_name as include_code_in_name_ind,
    to_timestamp(replace(substr(organization_organization_data_availibility_date,1,23),'T',' '),'yyyy-mm-dd hh24:mi:ss.US') - cast(strright(organization_organization_data_availibility_date,4) as time) as availibility_date,
    to_timestamp(replace(substr(organization_organization_data_last_updated_datetime,1,23),'T',' '),'yyyy-mm-dd hh24:mi:ss.US') - cast(strright(organization_organization_data_last_updated_datetime,4) as time) as last_updated_date, 
    coalesce(cast(organization_organization_data_inactive as int),-2) as inactive_ind,
    to_timestamp(organization_organization_data_inactive_date,'yyyy-mm-dd') as inactive_date,
    organization_data_organization_type_reference_wid as organization_type_wid,
    organization_data_organization_type_reference_organization_type_id as organization_type_id,
    organization_data_organization_subtype_reference_wid as organization_subtype_wid,
    organization_data_organization_subtype_reference_organization_subtype_id as organization_subtype_id,
    hierarchy_data_superior_organization_reference_wid as cost_center_parent_hierarchy_wid,
    hierarchy_data_superior_organization_reference_organization_reference_id as cost_center_parent_hierarchy_id,
    hierarchy_data_top_level_organization_reference_wid as cost_center_toplevel_hierarchy_wid,
    hierarchy_data_top_level_organization_reference_organization_reference_id as cost_center_toplevel_hierarchy_id,
    cast({{
        dbt_utils.surrogate_key([
            'cost_center_hierarchy_wid',
            'cost_center_hierarchy_id',
            'cost_center_hierarchy_name',
            'cost_center_hierarchy_code',
            'include_code_in_name_ind',
            'availibility_date', 
            'last_updated_date',
            'inactive_ind',
            'inactive_date',
            'organization_type_wid', 
            'organization_type_id',
            'organization_subtype_wid',
            'organization_subtype_id', 
            'cost_center_parent_hierarchy_wid',
            'cost_center_parent_hierarchy_id',
            'cost_center_toplevel_hierarchy_wid',
            'cost_center_toplevel_hierarchy_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods','get_organizations')}} as get_organizations
where
    1 = 1
    and organization_data_organization_type_reference_organization_type_id = 'Cost_Center_Hierarchy'
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
               {{ this }}
            where
               cost_center_hierarchy_wid = get_organizations.organization_organization_reference_wid
        ) 
    {%- endif %}
