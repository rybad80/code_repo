with company_company_hierarchy_base as
(
    select distinct
        organization_organization_reference_wid as company_wid,
        organization_organization_reference_organization_reference_id as company_id,
        hierarchy_data_included_in_organization_reference_wid as company_hierarchy_wid
    from
        {{source('workday_ods', 'get_organizations')}} as get_organizations
    where 
        organization_data_organization_type_reference_organization_type_id = 'Company'
),
final_output as (
select distinct
    company_company_hierarchy_base.company_wid,
    company_company_hierarchy_base.company_id,
    company_company_hierarchy_base.company_hierarchy_wid,
    company_hierarchy.company_hierarchy_id,
    cast({{
        dbt_utils.surrogate_key([
            'company_company_hierarchy_base.company_wid',
            'company_company_hierarchy_base.company_id',
            'company_company_hierarchy_base.company_hierarchy_wid',
            'company_hierarchy.company_hierarchy_id'
            ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    company_company_hierarchy_base
inner join
    {{ref('company_hierarchy')}} as company_hierarchy
        on company_company_hierarchy_base.company_hierarchy_wid = company_hierarchy.company_hierarchy_wid
)
select
    company_wid,
    company_id,
    company_hierarchy_wid,
    company_hierarchy_id,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    final_output
where
    1 = 1

