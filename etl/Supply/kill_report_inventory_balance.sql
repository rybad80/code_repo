-- Getting the Source Inventory Site - Main Hospital, KOPH
with inventory_site as (
select
    source_inventory_site,
    source_inventory_site_wid
from
    {{ source('workday_ods', 'inventory_par_location_items') }}
group by
    source_inventory_site,
    source_inventory_site_wid
),
-- Deriving the Unit of Measure Code EA --> Each, RL --> Roll
uom as (
    select
        unit_of_measure,
        edi_uom_code,
        un_cefact_common_code
    from
        {{ source('workday_ods', 'inventory_par_location_items') }}
    group by
        unit_of_measure,
        edi_uom_code,
        un_cefact_common_code
)
select distinct
    purchase_item.item_identifier || ' - ' || purchase_item.item_name as purchase_item,
    coalesce(inv_s.source_inventory_site,
    case
        when inventory_balances.inventory_balance_detail_inventory_site_location_id = 'INV_KOPH'
            then 'KOPH Storeroom'
        else inventory_balances.inventory_balance_detail_inventory_site_location_id
    end ) as inventory_site,
    scm_inventory_balance.inv_location,
    uom.unit_of_measure as base_uom,
    scm_inventory_balance.base_qty_on_hand,
    scm_inventory_balance.quantity_picked,
    scm_inventory_balance.qty_allocated,
    scm_inventory_balance.base_qty_avail,
    purchase_item.purchase_item_wid,
    inventory_balances.inventory_balance_detail_inventory_site_location_wid as inventory_site_wid,
    abs({{
        dbt_utils.surrogate_key([
            'purchase_item.item_identifier',
            'purchase_item.item_name',
            'scm_inventory_balance.base_qty_avail',
            'scm_inventory_balance.base_qty_on_hand',
            'scm_inventory_balance.inv_location',
            'inventory_balances.inventory_balance_detail_inventory_site_location_id'
        ])
    }}) as primary_key
from
    {{ source('workday_ods', 'inventory_balances') }} as inventory_balances
left join
    {{ source('workday_ods', 'purchase_item') }} as purchase_item
on
    inventory_balances.item_inventory_location_purchase_item_wid = purchase_item.purchase_item_wid
inner join
    {{ source('workday_ods', 'scm_inventory_balance') }} as scm_inventory_balance
on
    purchase_item.purchase_item_wid = scm_inventory_balance.purchase_item_wid
    and inventory_balances.inventory_balance_detail_inventory_site_location_wid
        = scm_inventory_balance.inventory_site_wid
left join
    inventory_site as inv_s
on
    inv_s.source_inventory_site_wid = inventory_balances.inventory_balance_detail_inventory_site_location_wid
left join
    uom
on uom.edi_uom_code = inventory_balances.base_edi_code_id
