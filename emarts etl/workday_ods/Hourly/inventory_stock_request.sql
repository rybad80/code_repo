{{ config(
    materialized = 'incremental',
    unique_key = 'inventory_stock_request_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['inventory_stock_request_wid','inventory_stock_request_number','inventory_stock_request_id','created_moment','created_moment_utc_offset','last_functionally_updated','last_functionally_updated_utc_offset','cf_sri_quantity_requested','cf_sri_quantity_picked','document_date','target_fulfillment_date','transaction_date','par_location_id','par_location_wid','document_status_id','document_status_wid','stock_request_type_id','stock_request_type_wid','inventory_stock_request_transaction_type_id','inventory_stock_request_transaction_type_wid','inventory_site_location_id','inventory_site_location_wid','md5', 'upd_dt', 'upd_by']
) }}
with inventory_stock_latest as (
    select
        inventory_stock_request_wid as max_inventory_stock_request_wid,
        max(to_timestamp(substr(last_functionally_updated ,1,10) || ' ' || substr(last_functionally_updated ,12,12), 'yyyy-mm-dd hh:mi:ss.ms')) as max_last_functionally_updated
    from
        {{source('workday_ods', 'workday_inventory_stock_request')}} as workday_inventory_stock_request
    group by
        inventory_stock_request_wid
),
stock_requests as (
    select distinct
        inventory_stock_request_wid,
        inventory_stock_request_number,
        inventory_stock_request_id,
        to_timestamp(substr(created_moment ,1,10) || ' ' || substr(created_moment ,12,12), 'yyyy-mm-dd hh:mi:ss.ms') as created_moment,
        substr(created_moment,25,5) as created_moment_utc_offset,
        to_timestamp(substr(last_functionally_updated ,1,10) || ' ' || substr(last_functionally_updated ,12,12), 'yyyy-mm-dd hh:mi:ss.ms') as last_functionally_updated,
        substr(last_functionally_updated,25,5) as last_functionally_updated_utc_offset,
        cast(cf_sri_quantity_requested as numeric(30,2)) as cf_sri_quantity_requested,
        cast(cf_sri_quantity_picked as numeric(30,2)) as cf_sri_quantity_picked,
        to_date(substr(document_date,1,10),'yyyy-mm-dd') as document_date,
        to_date(substr(target_fulfillment_date,1,10),'yyyy-mm-dd') as target_fulfillment_date,
        to_date(substr(transaction_date,1,10),'yyyy-mm-dd') as transaction_date,
        par_location_id,
        par_location_wid,
        document_status_id,
        document_status_wid,
        stock_request_type_id,
        stock_request_type_wid,
        inventory_stock_request_transaction_type_id,
        inventory_stock_request_transaction_type_wid,
        inventory_site_location_id,
        inventory_site_location_wid
    from
        {{source('workday_ods', 'workday_inventory_stock_request')}} as workday_inventory_stock_request
    inner join
        inventory_stock_latest
            on workday_inventory_stock_request.inventory_stock_request_wid = inventory_stock_latest.max_inventory_stock_request_wid
            and to_timestamp(substr(last_functionally_updated ,1,10) || ' ' || substr(last_functionally_updated ,12,12), 'yyyy-mm-dd hh:mi:ss.ms') = inventory_stock_latest.max_last_functionally_updated
)
select
    inventory_stock_request_wid,
    inventory_stock_request_number,
    inventory_stock_request_id,
    created_moment,
    created_moment_utc_offset,
    last_functionally_updated,
    last_functionally_updated_utc_offset,
    cf_sri_quantity_requested,
    cf_sri_quantity_picked,
    document_date,
    target_fulfillment_date,
    transaction_date,
    par_location_id,
    par_location_wid,
    document_status_id,
    document_status_wid,
    stock_request_type_id,
    stock_request_type_wid,
    inventory_stock_request_transaction_type_id,
    inventory_stock_request_transaction_type_wid,
    inventory_site_location_id,
    inventory_site_location_wid,
    cast({{ 
        dbt_utils.surrogate_key([ 
            'inventory_stock_request_wid',
            'inventory_stock_request_number',
            'inventory_stock_request_id',
            'created_moment',
            'created_moment_utc_offset',
            'last_functionally_updated',
            'last_functionally_updated_utc_offset',
            'cf_sri_quantity_requested',
            'cf_sri_quantity_picked',
            'document_date',
            'target_fulfillment_date',
            'transaction_date',
            'par_location_id',
            'par_location_wid',
            'document_status_id',
            'document_status_wid',
            'stock_request_type_id',
            'stock_request_type_wid',
            'inventory_stock_request_transaction_type_id',
            'inventory_stock_request_transaction_type_wid',
            'inventory_site_location_id',
            'inventory_site_location_wid'
        ]) 
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    stock_requests
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                inventory_stock_request_wid = stock_requests.inventory_stock_request_wid
            ) 
    {%- endif %}