{{
  config(
    materialized = 'incremental',
    unique_key = 'supplier_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['supplier_id', 'supplier_name', 'supplier_reference_id', 'worktag_only_ind', 'customer_account_number', 'duns_number', 'submit_ind', 'business_entity_name', 'external_entity_id', 'business_entity_tax_id', 'supplier_category_id', 'supplier_category_wid', 'custom_supplier_classification_id', 'custom_supplier_classification_wid', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'supplier'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with supplier
as (
select
    {{
        dbt_utils.surrogate_key([
            'supplier_wid'
        ])
    }} as supplier_key,
    supplier_wid,
    supplier_id,
    supplier_name,
    supplier_reference_id,
    worktag_only_ind,
    customer_account_number,
    duns_number,
    submit_ind,
    business_entity_name,
    external_entity_id,
    business_entity_tax_id,
    supplier_category_id,
    supplier_category_wid,
    custom_supplier_classification_id,
    custom_supplier_classification_wid,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    create_by || '~' || supplier_id as integration_id,
    current_timestamp as create_date,
    create_by,
    current_timestamp as update_date,
    upd_by as update_by
from
    {{source('workday_ods', 'supplier')}}
--
union all
--
select
    0,
	'N/A',
    'N/A',
    'N/A',
    'N/A',
    null,
    'N/A',
    'N/A',
    null,
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    0,
    'NA',
    CURRENT_TIMESTAMP,
    'DEFAULT',
    CURRENT_TIMESTAMP,
    'DEFAULT'
)
select
    supplier_key,
    supplier_wid,
    supplier_id,
    supplier_name,
    supplier_reference_id,
    worktag_only_ind,
    customer_account_number,
    duns_number,
    submit_ind,
    business_entity_name,
    external_entity_id,
    business_entity_tax_id,
    supplier_category_id,
    supplier_category_wid,
    custom_supplier_classification_id,
    custom_supplier_classification_wid,
    hash_value,
    integration_id,
    create_date,
    create_by,
    update_date,
    update_by
from
    supplier
where 1 = 1     
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where supplier_wid = supplier.supplier_wid)
{%- endif %}
