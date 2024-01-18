{{
    config(
        materialized = 'incremental',
        unique_key = 'provider_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['provider_wid', 'provider_id', 'provider_name', 'provider_code', 'availibility_date', 'last_updated_date', 'inactive_ind', 'inactive_date', 'organization_type_wid', 'organization_type_id', 'organization_subtype_wid', 'organization_subtype_id', 'md5', 'upd_dt', 'upd_by']
    )
}}
select distinct
    organization_organization_reference_wid as provider_wid,
    organization_organization_reference_organization_reference_id as provider_id,
    organization_organization_data_name as provider_name,
    organization_organization_data_organization_code as provider_code,
    to_timestamp(replace(substr(organization_organization_data_availibility_date,1,23),'T',' '),'yyyy-mm-dd hh24:mi:ss.us') - cast(strright(organization_organization_data_availibility_date,4) as time) as availibility_date,
    to_timestamp(replace(substr(organization_organization_data_last_updated_datetime,1,23),'T',' '),'yyyy-mm-dd hh24:mi:ss.us') - cast(strright(organization_organization_data_last_updated_datetime,4) as time) as last_updated_date,
    organization_organization_data_inactive as inactive_ind,
    to_timestamp(organization_organization_data_inactive_date,'yyyy-mm-dd') as inactive_date,
    organization_data_organization_type_reference_wid as organization_type_wid,
    organization_data_organization_type_reference_organization_type_id as organization_type_id,
    organization_data_organization_subtype_reference_wid as organization_subtype_wid,
    organization_data_organization_subtype_reference_organization_subtype_id as organization_subtype_id,
    cast({{
        dbt_utils.surrogate_key([
            'provider_wid',
            'provider_id',
            'provider_name',
            'provider_code',
            'availibility_date',
            'last_updated_date',
            'inactive_ind',
            'inactive_date',
            'organization_type_wid',
            'organization_type_id',
            'organization_subtype_wid',
            'organization_subtype_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_organizations')}} as get_organizations
where
    organization_data_organization_type_reference_organization_type_id = 'ORGANIZATION_TYPE-6-57'
    and organization_data_organization_subtype_reference_organization_subtype_id = 'Provider'
    and 1 = 1     
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                provider_wid = get_organizations.organization_organization_reference_wid
        )
    {%- endif %}