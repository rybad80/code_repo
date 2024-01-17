{{
  config(
    materialized = 'incremental',
    unique_key = 'payor_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['payor_id', 'payor_name', 'payor_code', 'availibility_date', 'last_updated_date', 'inactive_ind', 'inactive_date', 'organization_type_wid','organization_type_id', 'organization_subtype_wid', 'organization_subtype_id', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'payor'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with payor
as (
select
    {{
        dbt_utils.surrogate_key([
            'payor_wid'
        ])
    }} as payor_key,
    payor.payor_wid,
    payor.payor_id,
    payor.payor_name,
    payor.payor_code,
    payor.availibility_date,
    payor.last_updated_date,
    payor.inactive_ind,
    payor.inactive_date,
    payor.organization_type_wid,
    payor.organization_type_id,
    payor.organization_subtype_wid,
    payor.organization_subtype_id,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    payor.create_by || '~' || payor.payor_id as integration_id,
    current_timestamp as create_date,
    payor.create_by,
    current_timestamp as update_date,
    payor.upd_by as update_by
from
    {{source('workday_ods', 'payor')}} as payor
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
    payor.payor_key,
    payor.payor_wid,
    payor.payor_id,
    payor.payor_name,
    payor.payor_code,
    payor.availibility_date,
    payor.last_updated_date,
    payor.inactive_ind,
    payor.inactive_date,
    payor.organization_type_wid,
    payor.organization_type_id,
    payor.organization_subtype_wid,
    payor.organization_subtype_id,
    payor.hash_value,
    payor.integration_id,
    payor.create_date,
    payor.create_by,
    payor.update_date,
    payor.update_by
from
    payor
where 1 = 1     
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where payor_wid = payor.payor_wid)
{%- endif %}    
