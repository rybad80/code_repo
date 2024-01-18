{{
    config(
        materialized = 'incremental',
        unique_key = 'purchase_item_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['purchase_item_wid', 'purchase_item_id', 'item_name', 'item_description', 'item_identifier', 'packaging_string', 'conversion_factor', 'item_unit_price', 'default_as_service_request_ind', 'item_units_of_measure', 'base_uom_allows_decimals_ind', 'lot_control_ind', 'is_manufacturer_required_ind', 'use_item_manufacturers_only_ind', 'is_expiration_date_required_ind', 'inbound_alert_period_in_days', 'outbound_alert_period_in_days', 'inactive_ind', 'accounting_treatment_id', 'un_cefact_common_code_id', 'uom_edi_code_id', 'spend_category_id', 'purchase_item_group_id', 'manufacturer_wid', 'manufacturer_id', 'manufacturer_name', 'mfg_part_num', 'mfg', 'gtin', 'hazard_code', 'supply_type', 'supplier_wid', 'supplier_id', 'product_class', 'product_sub_class', 'md5', 'upd_dt', 'upd_by']
    )
}}
with base_data as (
    select
        purchase_item_purchase_item_reference_wid as purchase_item_wid,
        case when purchase_item_data_purchase_item_group_reference_purchase_item_group_id = 'SLCMEZZ_ITEM_GROUP' then 4
            when purchase_item_data_purchase_item_group_reference_purchase_item_group_id = 'SLCBULK_ITEM_GROUP' then 3
            when purchase_item_data_purchase_item_group_reference_purchase_item_group_id = 'CAROUSEL_ITEM_GROUP' then 2
            when purchase_item_data_purchase_item_group_reference_purchase_item_group_id = 'BULK_ITEM_GROUP' then 1
            when purchase_item_data_purchase_item_group_reference_purchase_item_group_id is null then 0
        end as group_value,
        purchase_item_data_purchase_item_group_reference_purchase_item_group_id,
        rank() over(partition by purchase_item_purchase_item_reference_wid order by group_value desc) as group_rank
     from
        {{source('workday_ods', 'get_purchase_items')}} as get_purchase_items
     group by purchase_item_purchase_item_reference_wid, purchase_item_data_purchase_item_group_reference_purchase_item_group_id
),
manufacturer_info as (
    select distinct
        purchase_item_purchase_item_reference_wid as purchase_item_wid,
        alternate_item_identifiers_data_manufacturer_reference_wid as manufacturer_wid,
        alternate_item_identifiers_data_manufacturer_reference_manufacturer_reference_id as manufacturer_id,
        alternate_item_identifiers_data_manufacturer_reference_manufacturer_name as manufacturer_name,
        purchase_item_data_alternate_item_identifiers_data_alternate_item_identifier_value as mfg_part_num
    from
        {{source('workday_ods', 'get_purchase_items')}} as get_purchase_items
    where
        alternate_item_identifiers_data_item_identifier_type_reference_item_identifier_type_id = 'MFG_PART_NUMBER'
),
mfg_info as (
    select distinct
        purchase_item_purchase_item_reference_wid as purchase_item_wid,
        purchase_item_data_alternate_item_identifiers_data_alternate_item_identifier_value as mfg
    from
        {{source('workday_ods', 'get_purchase_items')}} as get_purchase_items
    where
        alternate_item_identifiers_data_item_identifier_type_reference_item_identifier_type_id = 'MFG'
),
gtin_info as (
    select distinct
    purchase_item_purchase_item_reference_wid as purchase_item_wid,
    purchase_item_data_alternate_item_identifiers_data_alternate_item_identifier_value as gtin
    from
        {{source('workday_ods', 'get_purchase_items')}} as get_purchase_items
    where
        alternate_item_identifiers_data_item_identifier_type_reference_item_identifier_type_id = 'GTIN'
),
hazardcode_info as (
    select distinct
    purchase_item_purchase_item_reference_wid as purchase_item_wid,
    purchase_item_data_alternate_item_identifiers_data_alternate_item_identifier_value as hazard_code
    from
        {{source('workday_ods', 'get_purchase_items')}} as get_purchase_items
    where
        alternate_item_identifiers_data_item_identifier_type_reference_item_identifier_type_id = 'HAZARD_CODE'
),
supplytype_info as (
    select distinct
    purchase_item_purchase_item_reference_wid as purchase_item_wid,
    purchase_item_data_alternate_item_identifiers_data_alternate_item_identifier_value as supply_type
    from
        {{source('workday_ods', 'get_purchase_items')}} as get_purchase_items
    where
        alternate_item_identifiers_data_item_identifier_type_reference_item_identifier_type_id = 'SUPPLY_TYPE'
),
product_class_info as (
    select distinct
    purchase_item_purchase_item_reference_wid as purchase_item_wid,
    purchase_item_data_alternate_item_identifiers_data_alternate_item_identifier_value as product_class
    from
        {{source('workday_ods', 'get_purchase_items')}} as get_purchase_items
    where
        alternate_item_identifiers_data_item_identifier_type_reference_item_identifier_type_id = 'PRODUCT_CLASS'
),
product_subclass_info as (
    select distinct
    purchase_item_purchase_item_reference_wid as purchase_item_wid,
    purchase_item_data_alternate_item_identifiers_data_alternate_item_identifier_value as product_sub_class
    from
        {{source('workday_ods', 'get_purchase_items')}} as get_purchase_items
    where
        alternate_item_identifiers_data_item_identifier_type_reference_item_identifier_type_id = 'PRODUCT_SUB_CLASS'
),
purch_item as (
    select distinct
        get_purchase_items.purchase_item_purchase_item_reference_wid as purchase_item_wid,
        get_purchase_items.purchase_item_purchase_item_reference_purchase_item_id as purchase_item_id,
        get_purchase_items.purchase_item_purchase_item_data_item_name as item_name,
        get_purchase_items.purchase_item_purchase_item_data_item_description as item_description,
        get_purchase_items.purchase_item_purchase_item_data_item_identifier as item_identifier,
        get_purchase_items.purchase_item_purchase_item_data_packaging_string as packaging_string,
        0 as conversion_factor,
        get_purchase_items.purchase_item_purchase_item_data_item_unit_price as item_unit_price,
        coalesce(cast(get_purchase_items.purchase_item_purchase_item_data_default_as_service_request as int), -2) as default_as_service_request_ind,
        null as item_units_of_measure,
        -2 as base_uom_allows_decimals_ind,
        coalesce(cast(get_purchase_items.purchase_item_purchase_item_data_lot_control as int), -2) as lot_control_ind,
        coalesce(cast(get_purchase_items.purchase_item_purchase_item_data_is_manufacturer_required as int), -2) as is_manufacturer_required_ind,
        coalesce(cast(get_purchase_items.purchase_item_purchase_item_data_use_item_manufacturers_only as int), -2) as use_item_manufacturers_only_ind,
        coalesce(cast(get_purchase_items.purchase_item_purchase_item_data_is_expiration_date_required as int), -2) as is_expiration_date_required_ind,
        get_purchase_items.purchase_item_purchase_item_data_inbound_alert_period_in_days as inbound_alert_period_in_days,
        get_purchase_items.purchase_item_purchase_item_data_outbound_alert_period_in_days as outbound_alert_period_in_days,
        coalesce(cast(get_purchase_items.purchase_item_purchase_item_data_inactive  as int), -2) as inactive_ind,
        get_purchase_items.purchase_item_data_accounting_treatment_reference_accounting_treatment_id as accounting_treatment_id,
        get_purchase_items.purchase_item_data_base_unit_of_measure_reference_un_cefact_common_code_id as un_cefact_common_code_id,
        get_purchase_items.purchase_item_data_base_unit_of_measure_reference_uom_edi_code_id as uom_edi_code_id,
        get_purchase_items.purchase_item_data_resource_category_reference_spend_category_id as spend_category_id,
        get_purchase_items.purchase_item_data_purchase_item_group_reference_purchase_item_group_id as purchase_item_group_id,
        manufacturer_info.manufacturer_wid as manufacturer_wid,
        manufacturer_info.manufacturer_id as manufacturer_id,
        manufacturer_info.manufacturer_name as manufacturer_name,
        manufacturer_info.mfg_part_num as mfg_part_num,
        mfg_info.mfg as mfg,
        gtin_info.gtin as gtin,
        hazardcode_info.hazard_code as hazard_code,
        supplytype_info.supply_type as supply_type,
        null as supplier_wid,
        null as supplier_id,
        product_class_info.product_class as product_class,
        product_subclass_info.product_sub_class as product_sub_class,
        cast({{
            dbt_utils.surrogate_key([
                'get_purchase_items.purchase_item_purchase_item_reference_wid',
                'purchase_item_id',
                'item_name',
                'item_description',
                'item_identifier',
                'packaging_string',
                'conversion_factor',
                'item_unit_price',
                'default_as_service_request_ind',
                'item_units_of_measure',
                'base_uom_allows_decimals_ind',
                'lot_control_ind',
                'is_manufacturer_required_ind',
                'use_item_manufacturers_only_ind',
                'is_expiration_date_required_ind',
                'inbound_alert_period_in_days',
                'outbound_alert_period_in_days',
                'inactive_ind',
                'accounting_treatment_id',
                'un_cefact_common_code_id',
                'uom_edi_code_id',
                'spend_category_id',
                'purchase_item_group_id',
                'manufacturer_wid',
                'manufacturer_id',
                'manufacturer_name',
                'mfg_part_num',
                'mfg',
                'gtin',
                'hazard_code',
                'supply_type',
                'supplier_wid',
                'supplier_id',
                'product_class',
                'product_sub_class'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_purchase_items')}} as get_purchase_items
    inner join
        base_data on
            get_purchase_items.purchase_item_purchase_item_reference_wid = base_data.purchase_item_wid
            and coalesce(get_purchase_items.purchase_item_data_purchase_item_group_reference_purchase_item_group_id, '0') = coalesce(base_data.purchase_item_data_purchase_item_group_reference_purchase_item_group_id, '0')
            and base_data.group_rank = 1
    left join manufacturer_info on
        get_purchase_items.purchase_item_purchase_item_reference_wid = manufacturer_info.purchase_item_wid
    left join mfg_info on
        get_purchase_items.purchase_item_purchase_item_reference_wid = mfg_info.purchase_item_wid
    left join gtin_info on
        get_purchase_items.purchase_item_purchase_item_reference_wid = gtin_info.purchase_item_wid
    left join hazardcode_info on
        get_purchase_items.purchase_item_purchase_item_reference_wid = hazardcode_info.purchase_item_wid
    left join supplytype_info on
        get_purchase_items.purchase_item_purchase_item_reference_wid = supplytype_info.purchase_item_wid
    left join product_class_info on
        get_purchase_items.purchase_item_purchase_item_reference_wid = product_class_info.purchase_item_wid
    left join product_subclass_info on
        get_purchase_items.purchase_item_purchase_item_reference_wid = product_subclass_info.purchase_item_wid
)
select
    purchase_item_wid,
    purchase_item_id,
    item_name,
    item_description,
    item_identifier,
    packaging_string,
    conversion_factor,
    item_unit_price,
    default_as_service_request_ind,
    item_units_of_measure,
    base_uom_allows_decimals_ind,
    lot_control_ind,
    is_manufacturer_required_ind,
    use_item_manufacturers_only_ind,
    is_expiration_date_required_ind,
    inbound_alert_period_in_days,
    outbound_alert_period_in_days,
    inactive_ind,
    accounting_treatment_id,
    un_cefact_common_code_id,
    uom_edi_code_id,
    spend_category_id,
    purchase_item_group_id,
    manufacturer_wid,
    manufacturer_id,
    manufacturer_name,
    mfg_part_num,
    mfg,
    gtin,
    hazard_code,
    supply_type,
    supplier_wid,
    supplier_id,
    product_class,
    product_sub_class,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    purch_item
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                purchase_item_wid = purch_item.purchase_item_wid
        )
    {%- endif %}
