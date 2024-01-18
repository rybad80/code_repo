{{
    config(
        materialized = 'incremental',
        unique_key = 'supervisory_department_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['supervisory_department_wid','supervisory_department_id','supervisory_department_name','supervisory_department_code','availibility_date','last_updated_date','inactive_ind','inactive_date','organization_type_wid','organization_type_id','organization_subtype_wid','organization_subtype_id','manager_worker_wid','manager_employee_id','manager_contingent_worker_id','leadership_worker_wid','leadership_employee_id','leadership_contingent_worker_id','location_wid','location_id','supervisory_department_parent_wid','supervisory_department_parent_id','supervisory_department_toplevel_wid','supervisory_department_toplevel_id', 'md5', 'upd_dt', 'upd_by']
    )
}}
select distinct
    organization_organization_reference_wid as supervisory_department_wid,
    organization_organization_reference_organization_reference_id as supervisory_department_id,
    organization_organization_data_name as supervisory_department_name,
    organization_organization_data_organization_code as supervisory_department_code,
    to_timestamp(replace(substr(organization_organization_data_availibility_date,1,23),'T',' '),'yyyy-mm-dd hh24:mi:ss.us') - cast(strright(organization_organization_data_availibility_date,4) as time) as availibility_date,
    to_timestamp(replace(substr(organization_organization_data_last_updated_datetime,1,23),'T',' '),'yyyy-mm-dd hh24:mi:ss.us') - cast(strright(organization_organization_data_last_updated_datetime,4) as time) as last_updated_date,
    organization_organization_data_inactive as inactive_ind,
    to_timestamp(organization_organization_data_inactive_date,'yyyy-mm-dd') as inactive_date,
    organization_data_organization_type_reference_wid as organization_type_wid,
    organization_data_organization_type_reference_organization_type_id as organization_type_id,
    organization_data_organization_subtype_reference_wid as organization_subtype_wid,
    organization_data_organization_subtype_reference_organization_subtype_id as organization_subtype_id,
    organization_data_manager_reference_wid as manager_worker_wid,
    organization_data_manager_reference_employee_id as manager_employee_id,
    organization_data_manager_reference_contingent_worker_id as manager_contingent_worker_id,
    organization_data_leadership_reference_wid as leadership_worker_wid,
    organization_data_leadership_reference_employee_id as leadership_employee_id,
    organization_data_leadership_reference_contingent_worker_id as leadership_contingent_worker_id,
    supervisory_data_location_reference_wid as location_wid,
    supervisory_data_location_reference_location_id as location_id,
    hierarchy_data_superior_organization_reference_wid as supervisory_department_parent_wid,
    hierarchy_data_superior_organization_reference_organization_reference_id as supervisory_department_parent_id,
    hierarchy_data_top_level_organization_reference_wid as supervisory_department_toplevel_wid,
    hierarchy_data_top_level_organization_reference_organization_reference_id as supervisory_department_toplevel_id,
    cast({{
        dbt_utils.surrogate_key([
            'supervisory_department_wid',
            'supervisory_department_id',
            'supervisory_department_name',
            'supervisory_department_code',
            'availibility_date',
            'last_updated_date',
            'inactive_ind',
            'inactive_date',
            'organization_type_wid',
            'organization_type_id',
            'organization_subtype_wid',
            'organization_subtype_id',
            'manager_worker_wid',
            'manager_employee_id',
            'manager_contingent_worker_id',
            'leadership_worker_wid',
            'leadership_employee_id',
            'leadership_contingent_worker_id',
            'location_wid',
            'location_id',
            'supervisory_department_parent_wid',
            'supervisory_department_parent_id',
            'supervisory_department_toplevel_wid',
            'supervisory_department_toplevel_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_organizations')}} as get_organizations
where
    organization_data_organization_type_reference_organization_type_id = 'Supervisory'
    and organization_data_organization_subtype_reference_organization_subtype_id in ('Department','Company')
    and 1 = 1     
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                supervisory_department_wid = get_organizations.organization_organization_reference_wid
        )
    {%- endif %}