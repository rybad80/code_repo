{{ config(
    materialized = 'incremental',
    unique_key = 'fund_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['fund_wid', 'fund_id', 'fund_name', 'include_fund_id_in_name_ind', 'fund_is_inactive_ind', 'fund_type_id', 'fund_hierarchy_id', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
    get_funds.fund_reference_wid as fund_wid,
    get_funds.fund_reference_fund_id as fund_id,
    get_funds.fund_data_fund_name as fund_name,
    coalesce(cast(get_funds.fund_data_include_fund_id_in_name as int), -2) as include_fund_id_in_name_ind,
    coalesce(cast(get_funds.fund_data_fund_is_inactive as int), -2) as fund_is_inactive_ind,
    get_funds.fund_type_reference_fund_type_id as fund_type_id,
    get_fund_hierarchies.fund_hierarchy_data_fund_hierarchy_id as fund_hierarchy_id,
    cast({{
        dbt_utils.surrogate_key([
            'fund_wid',
            'fund_id',
            'fund_name',
            'include_fund_id_in_name_ind',
            'fund_is_inactive_ind',
            'fund_type_id',
            'fund_hierarchy_id'
            ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_funds')}} as get_funds
left join
    {{source('workday_ods', 'get_fund_hierarchies')}} as get_fund_hierarchies
        on get_funds.fund_reference_wid = get_fund_hierarchies.contains_funds_reference_wid
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                fund_wid = get_funds.fund_reference_wid
        )
    {%- endif %}
