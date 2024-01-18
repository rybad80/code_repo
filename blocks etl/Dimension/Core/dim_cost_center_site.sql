{{
  config(
    materialized = 'incremental',
    unique_key = 'cost_center_site_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['cost_center_site_id', 'cost_center_site_name', 'cost_center_site_code', 'availibility_date', 'last_updated_date', 'inactive_ind', 'inactive_date', 'organization_type_wid','organization_type_id', 'organization_subtype_wid', 'organization_subtype_id', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'cost_center_site'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with cost_center_site
as (
select
    {{
        dbt_utils.surrogate_key([
            'cost_center_site.cost_center_site_wid'
        ])
    }} as cost_center_site_key,
    cost_center_site.cost_center_site_wid,
    cost_center_site.cost_center_site_id,
    cost_center_site.cost_center_site_name,
    cost_center_site.cost_center_site_code,
    cost_center_site.availibility_date,
    cost_center_site.last_updated_date,
    cost_center_site.inactive_ind,
    cost_center_site.inactive_date,
    cost_center_site.organization_type_wid,
    cost_center_site.organization_type_id,
    cost_center_site.organization_subtype_wid,
    cost_center_site.organization_subtype_id,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    cost_center_site.create_by || '~' || cost_center_site.cost_center_site_id as integration_id,
    current_timestamp as create_date,
    cost_center_site.create_by,
    current_timestamp as update_date,
    cost_center_site.upd_by as update_by
from
    {{source('workday_ods', 'cost_center_site')}} as cost_center_site
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
    cost_center_site.cost_center_site_key,
    cost_center_site.cost_center_site_wid,
    cost_center_site.cost_center_site_id,
    cost_center_site.cost_center_site_name,
    cost_center_site.cost_center_site_code,
    cost_center_site.availibility_date,
    cost_center_site.last_updated_date,
    cost_center_site.inactive_ind,
    cost_center_site.inactive_date,
    cost_center_site.organization_type_wid,
    cost_center_site.organization_type_id,
    cost_center_site.organization_subtype_wid,
    cost_center_site.organization_subtype_id,
    cost_center_site.hash_value,
    cost_center_site.integration_id,
    cost_center_site.create_date,
    cost_center_site.create_by,
    cost_center_site.update_date,
    cost_center_site.update_by
from
    cost_center_site
where 1 = 1     
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where cost_center_site_wid = cost_center_site.cost_center_site_wid)
{%- endif %}
