{{ config(
    materialized = 'incremental',
    unique_key = 'inventory_stock_request_line_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['inventory_stock_request_line_wid','inventory_stock_request_line_id','inventory_stock_request_line_number','inventory_stock_request_wid','inventory_stock_request_number','inventory_stock_request_id','last_functionally_updated','last_functionally_updated_utc_offset','line_fulfillment_date','quantity','allocated_quantity','distribution_quantity','original_quantity_requested','quantity_shipped','quantity_picked','remaining_quantity_requested','quantity_in_transit','in_transit','is_inventory_backorder','unit_cost','inventory_item_descriptor','purchase_item_id','purchase_item_wid','base_unit_of_measure_descriptor','unit_of_measure_descriptor','md5', 'upd_dt', 'upd_by']
) }}
with latest_inventory_stock_req_lines as (
    select
        inventory_stock_request_line_wid as max_inventory_stock_request_line_wid,
        max(to_timestamp(substr(last_functionally_updated ,1,10) || ' ' || substr(last_functionally_updated ,12,12), 'yyyy-mm-dd hh:mi:ss.ms')) as max_last_functionally_updated
    from
        {{source('workday_ods', 'workday_inventory_stock_request_lines')}} as workday_inventory_stock_request_lines
    group by
        inventory_stock_request_line_wid
)
select distinct
    inventory_stock_request_line_wid,
    inventory_stock_request_line_id,
    stock_request_line_number as inventory_stock_request_line_number,
    inventory_stock_request_wid,
    inventory_stock_request_number,
    inventory_stock_request_id,
    to_timestamp(substr(last_functionally_updated ,1,10) || ' ' || substr(last_functionally_updated ,12,12), 'yyyy-mm-dd hh:mi:ss.ms') as last_functionally_updated,
    substr(last_functionally_updated,25,5) as last_functionally_updated_utc_offset,
    to_date(substr(line_fulfillment_date,1,10),'yyyy-mm-dd') as line_fulfillment_date,
    cast(quantity as numeric(30,2)) as quantity,
    cast(allocated_quantity as numeric(30,2)) as allocated_quantity,
    cast(distribution_quantity as numeric(30,2)) as distribution_quantity,
    cast(original_quantity_requested as numeric(30,2)) as original_quantity_requested,
    cast(quantity_shipped as numeric(30,2)) as quantity_shipped,
    cast(quantity_picked as numeric(30,2)) as quantity_picked,
    cast(remaining_quantity_requested as numeric(30,2)) as remaining_quantity_requested,
    cast(quantity_in_transit as numeric(30,2)) as quantity_in_transit,
    cast(in_transit as integer) as in_transit,
    cast(is_inventory_backorder as integer) as is_inventory_backorder,
    cast(unit_cost as numeric(30,2)) as unit_cost,
    inventory_item as inventory_item_descriptor,
    purchase_item_ref_id as purchase_item_id,
    purchase_item_wid,
    base_unit_of_measure as base_unit_of_measure_descriptor,
    unit_of_measure as unit_of_measure_descriptor,
    cast({{ 
        dbt_utils.surrogate_key([ 
            'inventory_stock_request_line_wid',
            'inventory_stock_request_line_id',
            'inventory_stock_request_line_number',
            'inventory_stock_request_wid',
            'inventory_stock_request_number',
            'inventory_stock_request_id',
            'last_functionally_updated',
            'last_functionally_updated_utc_offset',
            'line_fulfillment_date',
            'quantity',
            'allocated_quantity',
            'distribution_quantity',
            'original_quantity_requested',
            'quantity_shipped',
            'quantity_picked',
            'remaining_quantity_requested',
            'quantity_in_transit',
            'in_transit',
            'is_inventory_backorder',
            'unit_cost',
            'inventory_item_descriptor',
            'purchase_item_id',
            'purchase_item_wid',
            'base_unit_of_measure_descriptor',
            'unit_of_measure_descriptor'
        ]) 
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'workday_inventory_stock_request_lines')}} as workday_inventory_stock_request_lines
inner join
    latest_inventory_stock_req_lines
        on workday_inventory_stock_request_lines.inventory_stock_request_line_wid = latest_inventory_stock_req_lines.max_inventory_stock_request_line_wid
        and to_timestamp(substr(last_functionally_updated ,1,10) || ' ' || substr(last_functionally_updated ,12,12), 'yyyy-mm-dd hh:mi:ss.ms') = latest_inventory_stock_req_lines.max_last_functionally_updated
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                inventory_stock_request_line_wid = workday_inventory_stock_request_lines.inventory_stock_request_line_wid
            ) 
    {%- endif %}