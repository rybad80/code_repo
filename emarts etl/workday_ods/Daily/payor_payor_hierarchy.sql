select distinct
    organization_organization_reference_wid as payor_wid,
    organization_organization_reference_organization_reference_id as payor_id,
    hierarchy_data_included_in_organization_reference_wid as payor_hierarchy_wid,
    payor_hierarchy.payor_hierarchy_id as payor_hierarchy_id,
    cast({{
        dbt_utils.surrogate_key([
            'payor_wid',
            'payor_id',
            'payor_hierarchy_wid',
            'payor_hierarchy_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_organizations')}} as get_organizations
    left join {{ref('payor_hierarchy')}} as payor_hierarchy
        on get_organizations.hierarchy_data_included_in_organization_reference_wid = payor_hierarchy.payor_hierarchy_wid
where
    1 = 1
    and organization_data_organization_type_reference_organization_type_id = 'ORGANIZATION_TYPE-6-42'
    and organization_data_organization_subtype_reference_organization_subtype_id = 'ORGANIZATION_SUBTYPE-6-17'
