{{
  config(
    materialized = 'incremental',
    unique_key = 'cost_center_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['cost_center_id', 'cost_center_name', 'cost_center_code', 'availibility_date', 'last_updated_date', 'inactive_ind', 'inactive_date', 'organization_type_wid','organization_type_id', 'organization_subtype_wid', 'organization_subtype_id', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'cost_center'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with cost_center
as (
select
    {{
        dbt_utils.surrogate_key([
            'cost_center.cost_center_wid'
        ])
    }} as cost_center_key,
    cost_center.cost_center_wid,
    cost_center.cost_center_id,
    cost_center.cost_center_name,
    cost_center.cost_center_code,
    cost_center.availibility_date,
    cost_center.last_updated_date,
    cost_center.inactive_ind,
    cost_center.inactive_date,
    cost_center.organization_type_wid,
    cost_center.organization_type_id,
    cost_center.organization_subtype_wid,
    cost_center.organization_subtype_id,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    cost_center.create_by || '~' || cost_center.cost_center_id as integration_id,
    current_timestamp as create_date,
    cost_center.create_by,
    current_timestamp as update_date,
    cost_center.upd_by as update_by
from
    {{source('workday_ods', 'cost_center')}} as cost_center
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
    CURRENT_TIMESTAMP,
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
    cost_center.cost_center_key,
    cost_center.cost_center_wid,
    cost_center.cost_center_id,
    cost_center.cost_center_name,
    cost_center.cost_center_code,
    cost_center.availibility_date,
    cost_center.last_updated_date,
    cost_center.inactive_ind,
    cost_center.inactive_date,
    cost_center.organization_type_wid,
    cost_center.organization_type_id,
    cost_center.organization_subtype_wid,
    cost_center.organization_subtype_id,
    cost_center.hash_value,
    cost_center.integration_id,
    cost_center.create_date,
    cost_center.create_by,
    cost_center.update_date,
    cost_center.update_by
from
    cost_center
where 1 = 1     
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where cost_center_wid = cost_center.cost_center_wid)
{%- endif %}
