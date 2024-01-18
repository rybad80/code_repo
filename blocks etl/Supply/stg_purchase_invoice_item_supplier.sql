--get the latest record for any item-supplier combination, removing duplicates
with purchase_item_supplier_item_data_cte as (
    select
        purchase_item_supplier_item_data.*,
        row_number() over (partition by purchase_item_supplier_item_data.supplier_id,
        purchase_item_supplier_item_data.purchase_item_id
        order by purchase_item_supplier_item_data.upd_dt desc) as item_supplier_latest_row_number
    from
        {{source('workday_ods', 'purchase_item_supplier_item_data')}} as purchase_item_supplier_item_data
)

select
    *
from
    purchase_item_supplier_item_data_cte
where
    item_supplier_latest_row_number = 1
