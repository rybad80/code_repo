with purch_item_attrib as (
    select distinct
        get_purchase_items.purchase_item_purchase_item_reference_wid as purchase_item_wid,
        get_purchase_items.purchase_item_purchase_item_reference_purchase_item_id as purchase_item_id,
        coalesce(cast(get_purchase_items.purchase_item_data_item_inventory_attributes_data_use_reservations as int), -2) as use_reservations_ind,
        get_purchase_items.item_inventory_attributes_data_fulfillment_attributes_data_fulfillment_lead_time as fulfillment_lead_time,
        get_purchase_items.item_inventory_attributes_data_replenishment_policy_data_average_daily_usage as average_daily_usage,
        get_purchase_items.item_inventory_attributes_data_replenishment_policy_data_preferred_supplier_lead_time as preferred_supplier_lead_time,
        get_purchase_items.item_inventory_attributes_data_replenishment_policy_data_reorder_point_quantity as reorder_point_quantity,
        get_purchase_items.item_inventory_attributes_data_replenishment_policy_data_maximum_quantity as maximum_quantity,
        get_purchase_items.item_inventory_attributes_data_replenishment_policy_data_economic_order_quantity as economic_order_quantity,
        coalesce(cast(get_purchase_items.item_inventory_attributes_data_counting_attributes_data_include_in_count as int), -2) as include_in_count_ind,
        coalesce(get_purchase_items.item_inventory_attributes_data_inventory_site_reference_wid, 'N/A') as inventory_site_location_wid,
        get_purchase_items.item_inventory_attributes_data_inventory_site_reference_location_id as inventory_site_location_id,
        get_purchase_items.item_inventory_attributes_data_preferred_picking_location_reference_wid as preferred_picking_location_wid,
        get_purchase_items.item_inventory_attributes_data_preferred_picking_location_reference_location_id as preferred_picking_location_id,
        get_purchase_items.replenishment_policy_data_replenishment_type_reference_replenishment_type_id as replenishment_type_id,
        get_purchase_items.replenishment_policy_data_replenishment_unit_of_measure_reference_uom_edi_code_id as uom_edi_code_id,
        get_purchase_items.replenishment_policy_data_replenishment_unit_of_measure_reference_un_cefact_common_code_id as un_cefact_common_code_id,
        get_purchase_items.counting_attributes_data_inventory_abc_classification_reference_inventory_abc_classification_id as inventory_abc_classification_id,
        get_purchase_items.item_inventory_attributes_data_replenishment_option_reference_replenishment_option_id as replenishment_option_id,
        cast({{
            dbt_utils.surrogate_key([
                'purchase_item_wid',
                'purchase_item_id',
                'use_reservations_ind',
                'fulfillment_lead_time',
                'average_daily_usage',
                'preferred_supplier_lead_time',
                'reorder_point_quantity',
                'maximum_quantity',
                'economic_order_quantity',
                'include_in_count_ind',
                'inventory_site_location_wid',
                'inventory_site_location_id',
                'preferred_picking_location_wid',
                'preferred_picking_location_id',
                'replenishment_type_id',
                'uom_edi_code_id',
                'un_cefact_common_code_id',
                'inventory_abc_classification_id',
                'replenishment_option_id'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_purchase_items')}} as get_purchase_items
)
select
    purchase_item_wid,
    purchase_item_id,
    use_reservations_ind,
    fulfillment_lead_time,
    average_daily_usage,
    preferred_supplier_lead_time,
    reorder_point_quantity,
    maximum_quantity,
    economic_order_quantity,
    include_in_count_ind,
    inventory_site_location_wid,
    inventory_site_location_id,
    preferred_picking_location_wid,
    preferred_picking_location_id,
    replenishment_type_id,
    uom_edi_code_id,
    un_cefact_common_code_id,
    inventory_abc_classification_id,
    replenishment_option_id,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    purch_item_attrib
where
    1 = 1

