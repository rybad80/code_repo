{{ config(meta = {
    'critical': true
}) }}

--get the latest record for any worker wid, removing duplicates
with identifier_price_history as (
    select
        supplier_items_pricing_history.unit_price_as_of_effective_date,
        supplier_items_pricing_history.effective_date,
        supplier_items_pricing_history.unit_of_measure_descriptor,
        purchase_item_supplier_item_data.supplier_id,
        purchase_item_supplier_item_data.supplier_item_identifier,
        purchase_item_supplier_item_data.purchase_item_id,
        case when coalesce(purchase_item_supplier_item_data.sourcing_priority, 0) = 0
        then 99 else purchase_item_supplier_item_data.sourcing_priority end as sourcing_priority,
        row_number() over (partition by supplier_items_pricing_history.supplier_item_id,
        supplier_items_pricing_history.unit_of_measure_descriptor
        order by supplier_items_pricing_history.effective_date desc)
        as identifier_unit_of_measure_latest_price_row_number
    from
        {{source('workday_ods', 'supplier_items_pricing_history')}} as supplier_items_pricing_history
        inner join {{source('workday_ods', 'purchase_item_supplier_item_data')}}
        as purchase_item_supplier_item_data
        on purchase_item_supplier_item_data.supplier_item_id
        = supplier_items_pricing_history.supplier_item_id--no duplicates with inactive indicator 
        inner join {{source('workday_ods', 'purchase_item')}} as purchase_item
        on purchase_item.purchase_item_id = purchase_item_supplier_item_data.purchase_item_id
    where
        purchase_item_supplier_item_data.inactive_ind = 0
        and  purchase_item.inactive_ind = 0
)

select
    row_number() over (partition by identifier_price_history.purchase_item_id,
    identifier_price_history.unit_of_measure_descriptor
    order by identifier_price_history.identifier_unit_of_measure_latest_price_row_number asc,
    identifier_price_history.sourcing_priority asc)
    as item_unit_of_measure_latest_price_with_primary_source_row_number,
    identifier_price_history.unit_price_as_of_effective_date,
    identifier_price_history.unit_of_measure_descriptor,
    identifier_price_history.purchase_item_id,
    identifier_price_history.identifier_unit_of_measure_latest_price_row_number
 from
    identifier_price_history as identifier_price_history
