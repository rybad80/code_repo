{{
    config(
        materialized = 'table',
        dist = 'cost_center_hierarchy_key',
        meta = {
            'critical': true
        }
    )
}}

with main as (
    select
        cost_center_hierarchy.cost_center_hierarchy_id,
        cost_center_hierarchy.cost_center_hierarchy_wid,
        cost_center_hierarchy.cost_center_hierarchy_code,
        cost_center_hierarchy.cost_center_hierarchy_name,
        cost_center_hierarchy.organization_type_id,
        cost_center_hierarchy.organization_subtype_id,
        cost_center_hierarchy.availibility_date,
        cost_center_hierarchy.last_updated_date,
        cost_center_hierarchy.inactive_date,
        cost_center_hierarchy.include_code_in_name_ind,
        cost_center_hierarchy.inactive_ind,

        cost_center_hierarchy.cost_center_parent_hierarchy_id,
        cost_center_hierarchy.cost_center_toplevel_hierarchy_id
    from
        {{ source('workday_ods', 'cost_center_hierarchy') }} as cost_center_hierarchy
),

audit_fields as (
    select
        *,
        current_timestamp as update_date,
        'CLARITY' as update_source
    from
        main
),

primary_key as (
    select
        {{
            dbt_utils.surrogate_key([
                'cost_center_hierarchy_id'
            ])
        }} as cost_center_hierarchy_key,
        *
    from
        audit_fields
),

other_keys as (
    select
        main_pk.*,
        case
            -- The parent of the toplevel is 'NOT APPLICABLE', hence key = 0
            when main_pk.cost_center_hierarchy_key = top_pk.cost_center_hierarchy_key
                then 0
            else parent_pk.cost_center_hierarchy_key
        end as cost_center_parent_hierarchy_key,
        top_pk.cost_center_hierarchy_key as cost_center_toplevel_hierarchy_key
    from
        primary_key as main_pk
    left join primary_key as parent_pk
        on main_pk.cost_center_parent_hierarchy_id = parent_pk.cost_center_hierarchy_id
    left join primary_key as top_pk
        on main_pk.cost_center_toplevel_hierarchy_id = top_pk.cost_center_hierarchy_id
)

select
    cost_center_hierarchy_key,
    cost_center_parent_hierarchy_key,
    cost_center_toplevel_hierarchy_key,
    cost_center_hierarchy_id,
    cost_center_hierarchy_wid,
    cost_center_hierarchy_code,
    cost_center_hierarchy_name,
    organization_type_id,
    organization_subtype_id,
    availibility_date,
    last_updated_date,
    inactive_date,
    include_code_in_name_ind,
    inactive_ind,
    update_date,
    update_source
from
    other_keys

union all
select -2, -2, -2, '-2', '-2', '-2', 'INVALID', null, null, null, null, null, -2, -2,
    CURRENT_TIMESTAMP, 'DEFAULT'
union all
select -1, -1, -1, '-1', '-1', '-1', 'NOT APPLICABLE', null, null, null, null, null, -2, -2,
    CURRENT_TIMESTAMP, 'DEFAULT'
