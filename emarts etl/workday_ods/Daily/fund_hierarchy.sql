{{ config(
    materialized = 'incremental',
    unique_key = 'fund_hierarchy_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['fund_hierarchy_wid', 'fund_hierarchy_id', 'fund_hierarchy_name', 'include_fund_hierarchy_id_in_name_ind', 'fund_hierarchy_is_inactive_ind', 'parent_fund_hierarchy_id', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
    fund_hierarchy_reference_wid as fund_hierarchy_wid,
    fund_hierarchy_reference_fund_hierarchy_id as fund_hierarchy_id,
    fund_hierarchy_data_fund_hierarchy_name as fund_hierarchy_name,
    coalesce(cast(fund_hierarchy_data_include_fund_hierarchy_id_in_name as int), -2) as include_fund_hierarchy_id_in_name_ind,
    coalesce(cast(fund_hierarchy_data_fund_hierarchy_is_inactive as int), -2) as fund_hierarchy_is_inactive_ind,
    parent_fund_hierarchy_reference_fund_hierarchy_id as parent_fund_hierarchy_id,
    cast({{
        dbt_utils.surrogate_key([
            'fund_hierarchy_wid',
            'fund_hierarchy_id',
            'fund_hierarchy_name',
            'include_fund_hierarchy_id_in_name_ind',
            'fund_hierarchy_is_inactive_ind',
            'parent_fund_hierarchy_id'
            ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_fund_hierarchies')}} as get_fund_hierarchies
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                fund_hierarchy_wid = get_fund_hierarchies.fund_hierarchy_reference_wid
        )
    {%- endif %}
