{{
  config(
    materialized = 'incremental',
    unique_key = 'pay_group_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['pay_group_id', 'pay_group_name', 'pay_group_code', 'availibility_date', 'last_updated_date', 'inactive_ind', 'inactive_date', 'organization_type_wid','organization_type_id', 'organization_subtype_wid', 'organization_subtype_id', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'pay_group'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with pay_group
as (
select
    {{
        dbt_utils.surrogate_key([
            'pay_group_wid'
        ])
    }} as pay_group_key,
    pay_group.pay_group_wid,
    pay_group.pay_group_id,
    pay_group.pay_group_name,
    pay_group.pay_group_code,
    pay_group.availibility_date,
    pay_group.last_updated_date,
    pay_group.inactive_ind,
    pay_group.inactive_date,
    pay_group.organization_type_wid,
    pay_group.organization_type_id,
    pay_group.organization_subtype_wid,
    pay_group.organization_subtype_id,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    pay_group.create_by || '~' || pay_group.pay_group_id as integration_id,
    current_timestamp as create_date,
    pay_group.create_by,
    current_timestamp as update_date,
    pay_group.upd_by as update_by
from
    {{source('workday_ods', 'pay_group')}} as pay_group
--
union all
--
select
    0,
    'NA',
    'NA',
    'NA',
    'NA',
    null,
    null,
    0,
    null,
    'NA',
    'NA',
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
    pay_group.pay_group_key,
    pay_group.pay_group_wid,
    pay_group.pay_group_id,
    pay_group.pay_group_name,
    pay_group.pay_group_code,
    pay_group.availibility_date,
    pay_group.last_updated_date,
    pay_group.inactive_ind,
    pay_group.inactive_date,
    pay_group.organization_type_wid,
    pay_group.organization_type_id,
    pay_group.organization_subtype_wid,
    pay_group.organization_subtype_id,
    pay_group.hash_value,
    pay_group.integration_id,
    pay_group.create_date,
    pay_group.create_by,
    pay_group.update_date,
    pay_group.update_by
from
    pay_group
where 1 = 1     
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where pay_group_wid = pay_group.pay_group_wid)
{%- endif %}    
