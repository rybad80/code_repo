{{ config(
    materialized = 'incremental',
    unique_key = 'grant_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['grant_wid','grant_id','grant_name','include_id_in_name_ind','inactive_ind','fund_id','cost_center_id','cost_center_site_id','program_id','gift_id','provider_id', 'md5', 'upd_dt', 'upd_by']
) }}
with grant_wktag_costcenter as (
    select distinct
        grant_reference_wid,
        default_worktag_reference_cost_center_reference_id as cost_center_id
    from
        {{source('workday_ods', 'get_grants')}} as get_grants
    where
        default_worktag_reference_cost_center_reference_id is not null
),
grant_wktag_fund as (
    select distinct
        grant_reference_wid,
        default_worktag_reference_fund_id as fund_id
    from
        {{source('workday_ods', 'get_grants')}} as get_grants
    where
        default_worktag_reference_fund_id is not null
),
grant_wktag_program as (
    select distinct
        grant_reference_wid,
        default_worktag_reference_program_id as program_id
    from 
        {{source('workday_ods', 'get_grants')}} as get_grants
    where
        default_worktag_reference_program_id is not null
),
grant_wktag_provider as ( 
    select distinct
        grant_reference_wid,
        default_worktag_reference_organization_reference_id as provider_id
    from
        {{source('workday_ods', 'get_grants')}} as get_grants
    where
        worktag_type_reference_worktag_type_id = 'CUSTOM_ORGANIZATION_04'
),
grant_wktag_cc_site as (
    select distinct
        grant_reference_wid,
        default_worktag_reference_organization_reference_id as cost_center_site_id
    from
        {{source('workday_ods', 'get_grants')}} as get_grants
    where
        worktag_type_reference_worktag_type_id = 'CUSTOM_ORGANIZATION_03'
)
select distinct
    get_grants.grant_reference_wid as grant_wid,
    substr(get_grants.grant_reference_grant_id, 1, 50) as grant_id,
    substr(get_grants.grant_data_grant_name, 1, 200) as grant_name,
    coalesce(cast(get_grants.grant_data_include_grant_id_in_name as int), -2) as include_id_in_name_ind,
    coalesce(cast(get_grants.grant_data_grant_is_inactive as int), -2) as inactive_ind,
    grant_wktag_fund.fund_id,
    grant_wktag_costcenter.cost_center_id,
    grant_wktag_cc_site.cost_center_site_id,
    grant_wktag_program.program_id,
    null as gift_id,
    grant_wktag_provider.provider_id,
    cast({{
        dbt_utils.surrogate_key([
            'grant_wid',
            'grant_id',
            'grant_name',
            'include_id_in_name_ind',
            'inactive_ind',
            'fund_id',
            'cost_center_id',
            'cost_center_site_id',
            'program_id',
            'gift_id',
            'provider_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_grants')}} as get_grants
left join
    grant_wktag_costcenter
        on get_grants.grant_reference_wid = grant_wktag_costcenter.grant_reference_wid
left join
    grant_wktag_fund
        on get_grants.grant_reference_wid = grant_wktag_fund.grant_reference_wid
left join
    grant_wktag_program
        on get_grants.grant_reference_wid = grant_wktag_program.grant_reference_wid
left join
    grant_wktag_provider
        on get_grants.grant_reference_wid = grant_wktag_provider.grant_reference_wid
left join
    grant_wktag_cc_site
        on get_grants.grant_reference_wid = grant_wktag_cc_site.grant_reference_wid
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                grant_wid = get_grants.grant_reference_wid
        )
    {%- endif %}