{{
  config(
    materialized = 'incremental',
    unique_key = 'grant_costshare_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['grant_costshare_ref_id', 'grant_costshare_inactive', 'grant_costshare_code', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'workday_grant_costshare'), except=['upd_by']) %}
with grant_costshare
as (
select
    {{
        dbt_utils.surrogate_key([
            'grant_costshare.grant_costshare_wid'
        ])
    }} as grant_costshare_key, 
    grant_costshare.grant_costshare_wid,
    grant_costshare.grant_costshare_ref_id,
    grant_costshare.grant_costshare_code,
    regexp_replace(grant_costshare_code, '-+[^-]*$', '') as grant_id,
    grant_costshare.grant_costshare_inactive,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    'WORKDAY' || '~' || grant_costshare.grant_costshare_ref_id as integration_id,
    current_timestamp as create_date,
    'WORKDAY' as create_by,
    current_timestamp as update_date,
    'WORKDAY' as update_by
from
    {{source('workday_ods', 'workday_grant_costshare')}} as grant_costshare
--
union all
--
select
    0,
    'NA',
    'NA',
    'NA',
    'NA',
    0,
    0,
    'NA',
    CURRENT_TIMESTAMP,
    'DEFAULT',
    CURRENT_TIMESTAMP,
    'DEFAULT'
)
select
    grant_costshare.grant_costshare_key,      
    grant_costshare.grant_costshare_wid,
    grant_costshare.grant_costshare_ref_id,
    grant_costshare.grant_costshare_code,
    grant_costshare.grant_id,
    grant_costshare.grant_costshare_inactive,
    grant_costshare.hash_value,
    grant_costshare.integration_id,
    grant_costshare.create_date,
    grant_costshare.create_by,
    grant_costshare.update_date,
    grant_costshare.update_by
from
    grant_costshare
where 1 = 1     
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where grant_costshare_wid = grant_costshare.grant_costshare_wid)
{%- endif %}
