{{
  config(
    materialized = 'incremental',
    unique_key = 'revenue_category_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['revenue_category_wid', 'revenue_category_id', 'revenue_category_name', 'revenue_category_inactive_ind', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'revenue_category'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with revenue_category
as (
select
    {{
        dbt_utils.surrogate_key([
            'revenue_category_wid'
        ])
    }} as revenue_category_key,
    revenue_category.revenue_category_wid,
    revenue_category.revenue_category_id,
    revenue_category.revenue_category_name,
    revenue_category.revenue_category_inactive_ind,
    {{
        dbt_utils.surrogate_key(['revenue_category.revenue_category_wid', 'revenue_category.revenue_category_id', 'revenue_category.revenue_category_name', 'revenue_category.revenue_category_inactive_ind']
        )
    }} as hash_value,
    revenue_category.create_by || '~' || revenue_category.revenue_category_id as integration_id,
    current_timestamp as create_date,
    revenue_category.create_by,
    current_timestamp as update_date,
    revenue_category.upd_by as update_by
from
    {{source('workday_ods', 'revenue_category')}} as revenue_category    
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
    CURRENT_TIMESTAMP,
    'DEFAULT',
    CURRENT_TIMESTAMP, 
    'DEFAULT'
)    
select
    revenue_category.revenue_category_key,
    revenue_category.revenue_category_wid,
    revenue_category.revenue_category_id,
    revenue_category.revenue_category_name,
    revenue_category.revenue_category_inactive_ind,
    revenue_category.hash_value,
    revenue_category.integration_id,
    revenue_category.create_date,
    revenue_category.create_by,
    revenue_category.update_date,
    revenue_category.update_by
from
    revenue_category
where 1 = 1     
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where revenue_category_wid = revenue_category.revenue_category_wid)
{%- endif %}
