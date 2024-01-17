{{
  config(
    materialized = 'incremental',
    unique_key = 'purchase_item_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['spend_category_key', 'spend_category_id', 'purchase_item_wid', 'purchase_item_id', 'item_name', 'item_desc', 'item_identifier', 'packaging_string', 'conversion_factor', 'item_unit_price', 'default_as_service_request_ind', 'item_unit_of_measure', 'base_uom_allows_decimals_ind', 'lot_control_ind', 'manuf_required_ind', 'use_item_manuf_only_ind', 'expiration_date_required_ind', 'inbound_alert_period_in_days', 'outbound_alert_period_in_days', 'inactive_ind', 'accounting_treatment', 'un_cefact_common_code', 'uom_edi_code', 'purchase_item_group', 'hash_value', 'integration_id', 'update_date'],
    meta = {
        'critical': true
    }
  )
}}
--
{% set column_names = [
    'purchase_item.purchase_item_id',
    'purchase_item.item_name',
    'purchase_item.item_description',
    'purchase_item.item_identifier',
    'purchase_item.packaging_string',
    'purchase_item.conversion_factor',
    'purchase_item.item_unit_price',
    'purchase_item.default_as_service_request_ind',
    'purchase_item.item_units_of_measure',
    'purchase_item.base_uom_allows_decimals_ind',
    'purchase_item.lot_control_ind',
    'purchase_item.is_manufacturer_required_ind',
    'purchase_item.use_item_manufacturers_only_ind',
    'purchase_item.is_expiration_date_required_ind',
    'purchase_item.inbound_alert_period_in_days',
    'purchase_item.outbound_alert_period_in_days',
    'purchase_item.inactive_ind',
    'purchase_item.accounting_treatment_id',
    'purchase_item.un_cefact_common_code_id',
    'purchase_item.uom_edi_code_id',
    'purchase_item.purchase_item_group_id'
] %}
with purchase_item
as (
select
    {{
        dbt_utils.surrogate_key([
            'purchase_item.purchase_item_wid'
        ])
    }} as purchase_item_key,
    nvl(dim_spend_category.spend_category_key, -1) as spend_category_key,
    purchase_item.spend_category_id,
    purchase_item.purchase_item_wid,
    purchase_item.purchase_item_id,
    purchase_item.item_name,
    purchase_item.item_description as item_desc,
    purchase_item.item_identifier,
    purchase_item.packaging_string,
    purchase_item.conversion_factor,
    purchase_item.item_unit_price,
    purchase_item.default_as_service_request_ind,
    purchase_item.item_units_of_measure as item_unit_of_measure,
    purchase_item.base_uom_allows_decimals_ind,
    purchase_item.lot_control_ind,
    purchase_item.is_manufacturer_required_ind as manuf_required_ind,
    purchase_item.use_item_manufacturers_only_ind as use_item_manuf_only_ind,
    purchase_item.is_expiration_date_required_ind as expiration_date_required_ind,
    purchase_item.inbound_alert_period_in_days,
    purchase_item.outbound_alert_period_in_days,
    purchase_item.inactive_ind,
    purchase_item.accounting_treatment_id as accounting_treatment,
    purchase_item.un_cefact_common_code_id as un_cefact_common_code,
    purchase_item.uom_edi_code_id as uom_edi_code,
    purchase_item.purchase_item_group_id as purchase_item_group,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    purchase_item.create_by || '~' || purchase_item.purchase_item_id as integration_id,
    current_timestamp as create_date,
    purchase_item.create_by,
    current_timestamp as update_date,
    purchase_item.upd_by as update_by
from {{source('workday_ods', 'purchase_item')}} as purchase_item
    left join {{ref('dim_spend_category')}} as dim_spend_category 
    on purchase_item.spend_category_id = dim_spend_category.spend_category_id
--
union all
--
select
    -1,
    -1,
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    0,
    0,
    0,
    'N/A',
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    -1,
    'NA',
    CURRENT_TIMESTAMP,
    'UNSPECIFIED',
    CURRENT_TIMESTAMP, 
    'UNSPECIFIED'
--
union all
--
select
    -2,
    -2,
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    0,
    0,
    0,
    'N/A',
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    -2,
    'NA',
    CURRENT_TIMESTAMP,
    'NOT APPLICABLE',
    CURRENT_TIMESTAMP, 
    'NOT APPLICABLE'
)
select
    purchase_item.purchase_item_key,
    purchase_item.spend_category_key,
    purchase_item.spend_category_id,
    purchase_item.purchase_item_wid,
    purchase_item.purchase_item_id,
    purchase_item.item_name,
    purchase_item.item_desc,
    purchase_item.item_identifier,
    purchase_item.packaging_string,
    purchase_item.conversion_factor,
    purchase_item.item_unit_price,
    purchase_item.default_as_service_request_ind,
    purchase_item.item_unit_of_measure,
    purchase_item.base_uom_allows_decimals_ind,
    purchase_item.lot_control_ind,
    purchase_item.manuf_required_ind,
    purchase_item.use_item_manuf_only_ind,
    purchase_item.expiration_date_required_ind,
    purchase_item.inbound_alert_period_in_days,
    purchase_item.outbound_alert_period_in_days,
    purchase_item.inactive_ind,
    purchase_item.accounting_treatment,
    purchase_item.un_cefact_common_code,
    purchase_item.uom_edi_code,
    purchase_item.purchase_item_group,
    purchase_item.hash_value,
    purchase_item.integration_id,
    purchase_item.create_date,
    purchase_item.create_by,
    purchase_item.update_date,
    purchase_item.update_by
from
    purchase_item
where
    1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where purchase_item_wid = purchase_item.purchase_item_wid)
{%- endif %}


