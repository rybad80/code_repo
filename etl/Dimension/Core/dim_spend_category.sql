{{
  config(
    materialized = 'incremental',
    unique_key = 'spend_category_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['spend_category_wid', 'spend_category_id', 'spend_category_name', 'procurement_usage_ind', 'expense_usage_ind', 'allocate_freight_ind', 'allocate_other_charges_ind', 'track_items_ind', 'stock_items_ind', 'intangible_reference_ind', 'lease_ind', 'inactive_ind', 'description', 'commodity_code', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'spend_category'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with spend_category
as (
select
    {{
        dbt_utils.surrogate_key([
            'spend_category.spend_category_wid'
        ])
    }} as spend_category_key,
    spend_category.spend_category_wid,
    spend_category.spend_category_id,
    spend_category.spend_category_name,
    spend_category.procurement_usage_ind,
    spend_category.expense_usage_ind,
    spend_category.allocate_freight_ind,
    spend_category.allocate_other_charges_ind,
    spend_category.track_items_ind,
    spend_category.stock_items_ind,
    spend_category.intangible_reference_ind,
    spend_category.lease_ind,
    spend_category.inactive_ind,
    spend_category.description,
    spend_category.commodity_code,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    spend_category.create_by || '~' || spend_category.spend_category_id as integration_id,
    current_timestamp as create_date,
    spend_category.create_by,
    current_timestamp as update_date,
    spend_category.upd_by as update_by
from
    {{source('workday_ods', 'spend_category')}} as spend_category
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
    0,
    0,
    0,
    0,
    0,
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
    spend_category.spend_category_key,
    spend_category.spend_category_wid,
    spend_category.spend_category_id,
    spend_category.spend_category_name,
    spend_category.procurement_usage_ind,
    spend_category.expense_usage_ind,
    spend_category.allocate_freight_ind,
    spend_category.allocate_other_charges_ind,
    spend_category.track_items_ind,
    spend_category.stock_items_ind,
    spend_category.intangible_reference_ind,
    spend_category.lease_ind,
    spend_category.inactive_ind,
    spend_category.description,
    spend_category.commodity_code,
    spend_category.hash_value,
    spend_category.integration_id,
    spend_category.create_date,
    spend_category.create_by,
    spend_category.update_date,
    spend_category.update_by
from
    spend_category
where 1 = 1     
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where spend_category_wid = spend_category.spend_category_wid)
{%- endif %}
