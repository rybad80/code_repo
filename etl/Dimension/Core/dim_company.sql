{{
  config(
    materialized = 'incremental',
    unique_key = 'company_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['company_id', 'company_name', 'company_code', 'availibility_date', 'last_updated_date', 'inactive_ind', 'inactive_date', 'organization_type_wid','organization_type_id', 'organization_subtype_wid', 'organization_subtype_id', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'company'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with company
as (
select
    {{
        dbt_utils.surrogate_key([
            'company.company_wid'
        ])
    }} as company_key, 
    company.company_wid,
    company.company_id,
    company.company_name,
    company.company_code,
    company.availibility_date,
    company.last_updated_date,
    company.inactive_ind,
    company.inactive_date,
    company.organization_type_wid,
    company.organization_type_id,
    company.organization_subtype_wid,
    company.organization_subtype_id,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    company.create_by || '~' || company.company_id as integration_id,
    current_timestamp as create_date,
    company.create_by,
    current_timestamp as update_date,
    company.upd_by as update_by
from
    {{source('workday_ods', 'company')}} as company
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
    company.company_key, 
    company.company_wid,
    company.company_id,
    company.company_name,
    company.company_code,
    company.availibility_date,
    company.last_updated_date,
    company.inactive_ind,
    company.inactive_date,
    company.organization_type_wid,
    company.organization_type_id,
    company.organization_subtype_wid,
    company.organization_subtype_id,
    company.hash_value,
    company.integration_id,
    company.create_date,
    company.create_by,
    company.update_date,
    company.update_by
from
    company
where 1 = 1     
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where company_wid = company.company_wid)
{%- endif %}
