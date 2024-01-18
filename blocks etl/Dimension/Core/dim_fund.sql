{{
  config(
    materialized = 'incremental',
    unique_key = 'fund_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['fund_wid', 'fund_id', 'fund_name', 'include_fund_id_in_name_ind', 'fund_is_inactive_ind', 'fund_type_id', 'fund_hierarchy_id', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'fund'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with fund
as (
select
    {{
        dbt_utils.surrogate_key([
            'fund.fund_wid'
        ])
    }} as fund_key,
    fund.fund_wid,
    fund.fund_id,
    fund.fund_name,
    fund.include_fund_id_in_name_ind,
    fund.fund_is_inactive_ind,
    fund.fund_type_id,
    fund.fund_hierarchy_id,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    fund.create_by || '~' || fund.fund_id as integration_id,
    current_timestamp as create_date,
    fund.create_by,
    current_timestamp as update_date,
    fund.upd_by as update_by      
from
    {{source('workday_ods', 'fund')}} as fund
--
union all
--
select
    0,
    'NA',
    'NA',
    'NA',
    0,
    0,
    'NA',
    'NA',
    0,
    'NA',
    CURRENT_TIMESTAMP,
    'DEFAULT',
    CURRENT_TIMESTAMP, 
    'DEFAULT'
)
select
    fund.fund_key,
    fund.fund_wid,
    fund.fund_id,
    fund.fund_name,
    fund.include_fund_id_in_name_ind,
    fund.fund_is_inactive_ind,
    fund.fund_type_id,
    fund.fund_hierarchy_id,
    fund.hash_value,
    fund.integration_id,
    fund.create_date,
    fund.create_by,
    fund.update_date,
    fund.update_by      
from
    fund
where
    1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where fund_wid = fund.fund_wid)
{%- endif %}
