With costcentersite_costcentersite_hierarchy as (
    select distinct
        organization_organization_reference_wid as cost_center_site_wid,
        organization_organization_reference_organization_reference_id as cost_center_site_id,
        hierarchy_data_included_in_organization_reference_wid as cost_center_site_hierarchy_wid
        from
            {{source('workday_ods', 'get_organizations')}} as get_organizations
        where 
            organization_data_organization_type_reference_organization_type_id = 'Cost_Center_Site'
)
select distinct
    costcentersite_costcentersite_hierarchy.cost_center_site_wid,
    costcentersite_costcentersite_hierarchy.cost_center_site_id,
    costcentersite_costcentersite_hierarchy.cost_center_site_hierarchy_wid,
    cost_center_site_hierarchy.cost_center_site_hierarchy_id,
    cast({{
        dbt_utils.surrogate_key([
            'costcentersite_costcentersite_hierarchy.cost_center_site_wid',
            'costcentersite_costcentersite_hierarchy.cost_center_site_id',
            'costcentersite_costcentersite_hierarchy.cost_center_site_hierarchy_wid',
            'cost_center_site_hierarchy.cost_center_site_hierarchy_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    costcentersite_costcentersite_hierarchy
    inner join {{ref('cost_center_site_hierarchy')}} as cost_center_site_hierarchy
        on costcentersite_costcentersite_hierarchy.cost_center_site_hierarchy_wid = cost_center_site_hierarchy.cost_center_site_hierarchy_wid
where
    1 = 1     
