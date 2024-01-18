with costcenter_costcenter_hierarchy as (
    select distinct
        organization_organization_reference_wid as cost_center_wid,
        organization_organization_reference_organization_reference_id as cost_center_id,
        hierarchy_data_included_in_organization_reference_wid as cost_center_hierarchy_wid
    from
        {{source('workday_ods', 'get_organizations')}} as get_organizations
    where
        organization_data_organization_type_reference_organization_type_id = 'Cost_Center'
)
select distinct
    costcenter_costcenter_hierarchy.cost_center_wid,
    costcenter_costcenter_hierarchy.cost_center_id,
    costcenter_costcenter_hierarchy.cost_center_hierarchy_wid,
    cost_center_hierarchy.cost_center_hierarchy_id,
    cast({{
        dbt_utils.surrogate_key([
            'cost_center_wid',
            'cost_center_id',
            'costcenter_costcenter_hierarchy.cost_center_hierarchy_wid',
            'cost_center_hierarchy_id'
            ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    costcenter_costcenter_hierarchy
inner join
    {{ref('cost_center_hierarchy')}} as cost_center_hierarchy
        on costcenter_costcenter_hierarchy.cost_center_hierarchy_wid = cost_center_hierarchy.cost_center_hierarchy_wid
where
    1 = 1
