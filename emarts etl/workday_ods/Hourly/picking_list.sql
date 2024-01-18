{{ config(
    materialized = 'incremental',
    unique_key = ['picking_list_wid','picking_list_lines_wid'],
    incremental_strategy = 'merge',
    merge_update_columns = ['picking_list_wid','picking_list_id','inventory_stock_request_wid','inventory_stock_request_id','deliver_to_location_wid', 'deliver_to_location_id','picking_list_lines_wid','picking_list_lines_id','picking_list_purchase_item_wid','picking_list_purchase_item_id','inventory_pick_list_descriptor','inventory_stock_request_descriptor','inventory_zone_descriptor','status_descriptor','assigned_to_worker_descriptor', 'total_line_count', 'pick_list_lines_item_descriptor', 'deliver_to_descriptor', 'item_description', 'quantity','original_quantity_requested','document_date', 'inventory_stock_request_created_date', 'picking_list_lines_last_updated','md5', 'upd_dt', 'upd_by']
) }}
with picking_list_lastest_updated as (
    select
        picking_list_wid as max_picking_list_wid,
        picking_list_lines_wid as max_picking_list_lines_wid,
        max((to_timestamp(substring(picking_list_lines_last_updated,1,10) || ' ' || substring(picking_list_lines_last_updated, 12,8), 'yyyy-mm-dd hh:mi:ss')) + INTERVAL '3 Hours') AS max_picking_list_lines_last_updated
    from
        {{source('workday_ods','workday_picking_list')}} as workday_picking_list
    group by
        picking_list_wid,
        picking_list_lines_wid
),
picking_list_data as (
    select distinct
        picking_list_wid as picking_list_wid, 
        picking_list_id as picking_list_id,
        inventory_stock_request_wid as inventory_stock_request_wid,
        inventory_stock_request_id as inventory_stock_request_id,
        deliver_to_location_wid as deliver_to_location_wid,
        deliver_to_location_id as deliver_to_location_id,
        coalesce(cast(picking_list_lines_wid as varchar(50)),'0') as picking_list_lines_wid,
        picking_list_lines_id as picking_list_lines_id,
        purchase_item_wid as picking_list_purchase_item_wid,
        purchase_item_id as picking_list_purchase_item_id,
        inventory_pick_list as inventory_pick_list_descriptor,
        inventory_stock_request as inventory_stock_request_descriptor,
        inventory_zone as inventory_zone_descriptor,
        status as status_descriptor,
        assigned_to_worker as assigned_to_worker_descriptor,
        total_line_count as total_line_count,
        picking_list_lines_item as pick_list_lines_item_descriptor,
        deliver_to as deliver_to_descriptor,
        item_description as item_description,
        quantity as quantity,
        original_quantity_requested as original_quantity_requested,
        to_timestamp(document_date,'yyyy-mm-dd') as document_date,
        (to_timestamp(substring(inventory_stock_request_created_date,1,10) || ' ' || substring(inventory_stock_request_created_date, 12,8), 'yyyy-mm-dd hh:mi:ss')) + INTERVAL '3 Hours' AS inventory_stock_request_created_date,
        (to_timestamp(substring(picking_list_lines_last_updated,1,10) || ' ' || substring(picking_list_lines_last_updated, 12,8), 'yyyy-mm-dd hh:mi:ss')) + INTERVAL '3 Hours' AS picking_list_lines_last_updated
    from
        {{source('workday_ods','workday_picking_list')}} as workday_picking_list
    inner join
        picking_list_lastest_updated
            on workday_picking_list.picking_list_wid = picking_list_lastest_updated.max_picking_list_wid
            and workday_picking_list.picking_list_lines_wid = picking_list_lastest_updated.max_picking_list_lines_wid
            and (to_timestamp(substring(picking_list_lines_last_updated,1,10) || ' ' || substring(picking_list_lines_last_updated, 12,8), 'yyyy-mm-dd hh:mi:ss')) + INTERVAL '3 Hours' = picking_list_lastest_updated.max_picking_list_lines_last_updated
)
select distinct
    picking_list_wid,
    picking_list_id,
    inventory_stock_request_wid,
    inventory_stock_request_id,
    deliver_to_location_wid,
    deliver_to_location_id,
    picking_list_lines_wid,
    picking_list_lines_id,
    picking_list_purchase_item_wid,
    picking_list_purchase_item_id,
    inventory_pick_list_descriptor,
    inventory_stock_request_descriptor,
    inventory_zone_descriptor,
    status_descriptor,
    assigned_to_worker_descriptor,
    total_line_count,
    pick_list_lines_item_descriptor,
    deliver_to_descriptor,
    item_description,
    quantity,
    original_quantity_requested,
    document_date,
    inventory_stock_request_created_date,
    picking_list_lines_last_updated,
    cast({{ 
        dbt_utils.surrogate_key([ 
            'picking_list_wid',
            'picking_list_id',
            'inventory_stock_request_wid',
            'inventory_stock_request_id',
            'deliver_to_location_wid', 
            'deliver_to_location_id',
            'picking_list_lines_wid',
            'picking_list_lines_id',
            'picking_list_purchase_item_wid',
            'picking_list_purchase_item_id',
            'inventory_pick_list_descriptor',
            'inventory_stock_request_descriptor',
            'inventory_zone_descriptor',
            'status_descriptor',
            'assigned_to_worker_descriptor',
            'total_line_count',
            'pick_list_lines_item_descriptor',
            'deliver_to_descriptor',
            'item_description',
            'quantity',
            'original_quantity_requested',
            'document_date',
            'inventory_stock_request_created_date', 
            'picking_list_lines_last_updated'
        ]) 
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    picking_list_data
where
1 = 1
{%- if is_incremental() %}
    and md5 not in (
        select md5
        from
            {{ this }}
        where
            picking_list_wid = picking_list_data.picking_list_wid
            and picking_list_lines_wid = picking_list_data.picking_list_lines_wid
        ) 
{%- endif %}