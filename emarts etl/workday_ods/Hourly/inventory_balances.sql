select distinct
    get_inventory_balance_detail_response_element_qoh_with_uom as qoh_with_uom,
    item_in_inventory_location_reference_purchase_item_id as item_inventory_location_purchase_item_id,
    item_in_inventory_location_reference_wid as item_inventory_location_purchase_item_wid,
    get_inventory_balance_detail_site_response_element_average_cost_in_base_currency as average_cost_in_base_currency,
    inventory_balance_detail__inventory_site_reference_location_id as inventory_balance_detail_inventory_site_location_id,
    inventory_balance_detail__inventory_site_reference_wid as inventory_balance_detail_inventory_site_location_wid,
    inventory_balance_detail_site_currency_reference_currency_id as inventory_balance_detail_site_currency_id,
    get_inventory_balance_detail_site_response_element_location_quantity_with_uom as location_quantity_with_uom,
    inventory_balance_sub_inventory_location_reference_location_id as inventory_balance_sub_inventory_location_id,
    inventory_balance_sub_inventory_location_reference_wid as inventory_balance_sub_inventory_location_wid,
    get_inventory_balance_location_quantity_detail_view_element_quantity_on_hand as quantity_on_hand,
    get_inventory_balance_location_quantity_detail_view_element_total_allocated_pick_quantity as total_allocated_pick_quantity,
    get_inventory_balance_location_quantity_detail_view_element_quantity_available as quantity_available,
    get_inventory_balance_location_quantity_detail_view_element_base_quantity_on_hand as base_quantity_on_hand,
    inventory_location_quantity_reference_wid as inventory_location_quantity_reference_wid,
    location_quantity_unit_of_measure_reference_uom_edi_code_id as edi_code_id,
    location_quantity_unit_of_measure_reference_un_cefact_common_code_id as cefact_common_code_id,
    base_unit_of_measure_reference_uom_edi_code_id as base_edi_code_id,
    base_unit_of_measure_reference_un_cefact_common_code_id as base_cefact_common_code_id,
    cast({{ 
        dbt_utils.surrogate_key([ 
            'qoh_with_uom',
            'item_inventory_location_purchase_item_id',
            'item_inventory_location_purchase_item_wid',
            'average_cost_in_base_currency',
            'inventory_balance_detail_inventory_site_location_id', 
            'inventory_balance_detail_inventory_site_location_wid',
            'inventory_balance_detail_site_currency_id',
            'location_quantity_with_uom',
            'inventory_balance_sub_inventory_location_id',
            'inventory_balance_sub_inventory_location_wid',
            'quantity_on_hand', 
            'total_allocated_pick_quantity',
            'quantity_available',
            'base_quantity_on_hand', 
            'inventory_location_quantity_reference_wid',
            'edi_code_id', 
            'cefact_common_code_id', 
            'base_edi_code_id', 
            'base_cefact_common_code_id'
        ]) 
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_inventory_balances')}} as get_inventory_balances
where
    1 = 1
