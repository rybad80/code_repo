{{ config(
    materialized = 'incremental',
    unique_key = 'fund_type_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['fund_type_wid', 'fund_type_id', 'fund_type_name', 'include_fund_type_id_in_name_ind', 'inactive_ind', 'fund_restriction_id', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
fund_type_reference_wid as fund_type_wid,
fund_type_reference_fund_type_id as fund_type_id,
fund_type_data_fund_type_name as fund_type_name,
coalesce(cast(fund_type_data_include_fund_type_id_in_name as int), -2) as include_fund_type_id_in_name_ind,
coalesce(cast(fund_type_data_inactive as int), -2) as inactive_ind,
fund_restriction_reference_fund_restriction_id as fund_restriction_id,
cast({{
        dbt_utils.surrogate_key([
            'fund_type_wid',
            'fund_type_id',
            'fund_type_name',
            'include_fund_type_id_in_name_ind',
            'inactive_ind',
            'fund_restriction_id'
            ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_fund_types')}} as get_fund_types
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                fund_type_wid = get_fund_types.fund_type_reference_wid
        )
    {%- endif %}